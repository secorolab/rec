"""A run and its lifecycle, recorded in a store as the run's provenance bundle."""

from __future__ import annotations

import logging
import platform
import socket
import threading
import traceback
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from rdf_utils.models.prov import create_bundle, get_py_pkg_info, load_execution_prov, load_pkg_prov
from rdf_utils.models.time import add_wall_instant
from rdflib import XSD, Literal, URIRef
from rdflib.namespace import PROV, RDF, TIME

from rec import State, Verdict
from rec.provenance import HOST_TERMS, OSLC_AUTO, PROV_EXT, REC, RUN_BASE, RUN_ID, STATES, VERDICTS

if TYPE_CHECKING:
    # The MariaDB driver is an optional extra.
    from rec.stores.file_store import FileStore
    from rec.stores.mariadb_store import MariaDBStore

logger = logging.getLogger(__name__)


class Run:
    """A run, recorded in its store as a `prov-ext:Execution` and an `oslc_auto:AutomationResult`.

    The run lives in its own `prov:Bundle`, attributed to the rec that first recorded it. Every
    step is one edit of the bundle the store parsed under its lock: the run's state is checked and
    only the step's triples change. Any holder of the id reaches the same run through the store.
    """

    def __init__(self, store: FileStore | MariaDBStore, run_id: str | None = None, base: str = RUN_BASE):
        """
        :param store: Where the run's bundle is kept
        :param run_id: The run's id; a UUID is minted when none is given
        :param base: IRI base of the run's node, ``<base><run_id>``
        :raises ValueError: ``run_id`` is not a run id (``rec.provenance.RUN_ID``)
        """
        self.id = run_id or str(uuid4())
        if not RUN_ID.fullmatch(self.id):
            raise ValueError(f"'{self.id}' is not a run id -- use letters, digits, '-', '.', '_' or '~', not '.' or '..'")
        self.store = store
        self.node = URIRef(base + self.id)
        # Read once: it inspects rec's checkout, and every step holds the store's lock.
        self.recorder = get_py_pkg_info("rec")
        self.beat_interval = 10.0

    @contextmanager
    def _step(self, allowed: tuple[URIRef, ...], refusal: str):
        """Yield the run's bundle and current state under the store's lock; the bundle is created on the first step.

        :param allowed: The states the step may start from
        :param refusal: Why the step is refused from any other state
        :raises RuntimeError: The run is in a state not ``allowed``
        """
        with self.store.edit(self.id) as ds:
            bundle = next((graph for graph in ds.graphs() if (graph.identifier, RDF.type, PROV.Bundle) in graph), None)
            if bundle is None:
                bundle = ds.graph(create_bundle(ds, self.recorder.purl))
                load_pkg_prov(bundle, self.recorder)
                bundle.add((self.node, RDF.type, PROV_EXT.Execution))
                bundle.add((self.node, RDF.type, OSLC_AUTO.AutomationResult))
                bundle.add((self.node, OSLC_AUTO.state, OSLC_AUTO.new))
                bundle.add((self.node, OSLC_AUTO.verdict, OSLC_AUTO.unavailable))
            state = bundle.value(self.node, OSLC_AUTO.state)
            if state not in allowed:
                raise RuntimeError(f"run '{self.id}' is {state.fragment} -- {refusal}")
            yield bundle, state

    def lifecycle(self) -> tuple[State, Verdict]:
        """The run's state and verdict as the store keeps them; a run not stored is new."""
        ds = self.store.load(self.id)
        if ds is None:
            return State.NEW, Verdict.UNAVAILABLE
        ds.default_union = True
        state, verdict = ds.value(self.node, OSLC_AUTO.state), ds.value(self.node, OSLC_AUTO.verdict)
        return (
            next(name for name, iri in STATES.items() if iri == state),
            next(name for name, iri in VERDICTS.items() if iri == verdict),
        )

    def queue(self):
        """Record a new run as queued, before a runner starts it."""
        with self._step((OSLC_AUTO.new,), "only a new run can be queued") as (bundle, _):
            bundle.set((self.node, OSLC_AUTO.state, OSLC_AUTO.queued))
            bundle.add((self.node, REC["queued-time"], Literal(datetime.now(UTC))))
        logger.info("Queued run %s", self.id)

    def start(self):
        """Record a new or queued run as in progress, executed by this rec on this machine."""
        host = {
            "hostname": socket.gethostname(),
            "os": platform.platform(),
            "python": platform.python_version(),
            "cpu": platform.processor() or None,
        }
        with self._step((OSLC_AUTO.new, OSLC_AUTO.queued), "only a run that has not started can start") as (bundle, _):
            # The rec that starts the run executes it; another rec may have queued it.
            if (self.recorder.purl, RDF.type, PROV.Agent) not in bundle:
                load_pkg_prov(bundle, self.recorder)
            started = add_wall_instant(bundle, datetime.now(UTC), instant_id=URIRef(f"{self.node}/beginning"))
            load_execution_prov(bundle, self.node, (), self.recorder.purl, started)
            bundle.set((self.node, OSLC_AUTO.state, OSLC_AUTO.inProgress))
            node = URIRef(f"{self.node}/host")
            bundle.add((node, RDF.type, REC.Host))
            bundle.add((node, RDF.type, PROV.Location))
            for key, predicate in HOST_TERMS.items():
                if host[key]:
                    bundle.add((node, predicate, Literal(host[key])))
            bundle.add((self.node, PROV.atLocation, node))
        logger.info("Started run %s", self.id)

    def beat(self):
        """Record that the started run is still alive."""
        with self._step((OSLC_AUTO.inProgress, OSLC_AUTO.canceling), "only a started run beats") as (bundle, _):
            bundle.set((self.node, REC["heartbeat-time"], Literal(datetime.now(UTC))))

    def end(self, verdict: Verdict, fail_trace: str | None = None):
        """End the started run with its verdict: a canceling run that passed ends canceled, any other ends complete.

        :param verdict: ``passed``, ``failed`` (an exception ended it) or ``error`` (it was interrupted)
        :param fail_trace: The trace of what ended a failed or error run; none for a passed one
        :raises ValueError: The verdict is unavailable, or a passed run has a trace
        """
        if verdict is Verdict.UNAVAILABLE:
            raise ValueError(f"run '{self.id}' cannot end without a verdict -- end it as passed, failed or error")
        if verdict is Verdict.PASSED and fail_trace:
            raise ValueError(f"run '{self.id}' passed with a fail trace -- only a failed or error run has one")
        with self._step((OSLC_AUTO.inProgress, OSLC_AUTO.canceling), "only a started run can end") as (bundle, state):
            ended = add_wall_instant(bundle, datetime.now(UTC), instant_id=URIRef(f"{self.node}/end"))
            bundle.add((self.node, TIME.hasEnd, ended))
            # A run that failed while canceling did not stop as asked: its verdict stands.
            if state == OSLC_AUTO.canceling and verdict is Verdict.PASSED:
                bundle.set((self.node, OSLC_AUTO.state, OSLC_AUTO.canceled))
            else:
                bundle.set((self.node, OSLC_AUTO.state, OSLC_AUTO.complete))
                bundle.set((self.node, OSLC_AUTO.verdict, VERDICTS[verdict]))
            if fail_trace:
                bundle.add((self.node, REC["fail-trace"], Literal(fail_trace, datatype=XSD.string)))
        logger.log(logging.INFO if verdict is Verdict.PASSED else logging.ERROR, "Ended run %s as %s", self.id, verdict)

    def cancel(self):
        """Cancel a queued run, or ask one in progress to stop: it ends canceled once its work returns.

        A queued run that is cancelled never started, so it has neither a beginning nor an end.
        """
        refusal = "only a queued run or one in progress can be cancelled"
        with self._step((OSLC_AUTO.queued, OSLC_AUTO.inProgress), refusal) as (bundle, state):
            bundle.set((self.node, OSLC_AUTO.state, OSLC_AUTO.canceled if state == OSLC_AUTO.queued else OSLC_AUTO.canceling))
        logger.info("Cancelled run %s", self.id)

    def run(self, work: Callable[[Run], object]) -> object:
        """Start the run, do ``work(self)`` while the run beats, and end it by how the work ended.

        ``work`` sees a cancel request as ``self.lifecycle()[0] is State.CANCELING``.

        :returns: What ``work`` returned, None if it raised; rec does not record it
        :raises BaseException: What interrupted ``work``, e.g. ``KeyboardInterrupt``, once the run is ended as error
        """
        self.start()
        stopped = threading.Event()

        def beat_until_stopped():
            while not stopped.wait(self.beat_interval):
                # A store briefly out of reach must not stop the beats that follow.
                try:
                    self.beat()
                except Exception:
                    logger.exception("Run %s could not record its heartbeat", self.id)

        beating = threading.Thread(target=beat_until_stopped, daemon=True)
        error, result = None, None
        try:
            beating.start()
            result = work(self)
        except BaseException as caught:
            error = caught
        finally:
            stopped.set()
            # Bounded: a beat stuck on the store must not keep the run from ending.
            beating.join(timeout=self.beat_interval)
        try:
            if error is None:
                self.end(Verdict.PASSED)
            else:
                self.end(Verdict.FAILED if isinstance(error, Exception) else Verdict.ERROR, "".join(traceback.format_exception(error)))
        finally:
            # An interrupt is the process being asked to stop: passed on even when the end cannot be recorded.
            if error is not None and not isinstance(error, Exception):
                raise error
        return result
