"""Records the events of runs in a store, each run as its provenance bundle."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING

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


class Observer:
    """Records the events of any number of runs in one store; every call names its run.

    A run is a `prov-ext:Execution` and an `oslc_auto:AutomationResult` in a `prov:Bundle` attributed
    to the rec that first recorded it. Each event changes only its own triples, in the bundle the
    store parsed under its lock.
    """

    def __init__(self, store: FileStore | MariaDBStore, base: str = RUN_BASE):
        """
        :param store: Where the runs' bundles are kept
        :param base: IRI base of the run nodes; a run is ``<base><run_id>``
        """
        self.store = store
        self.base = base
        # Read once: it inspects rec's checkout, and every edit of a run holds the store's lock.
        self.recorder = get_py_pkg_info("rec")

    @contextmanager
    def _run(self, run_id: str):
        """Yield the run's bundle and node under the store's lock, the bundle created on the run's first edit.

        :raises ValueError: ``run_id`` is not a run id (``rec.provenance.RUN_ID``)
        """
        if not RUN_ID.fullmatch(run_id):
            raise ValueError(f"'{run_id}' is not a run id -- use letters, digits, '-', '.', '_' or '~', not '.' or '..'")
        run = URIRef(self.base + run_id)
        with self.store.edit(run_id) as ds:
            bundle = next((graph for graph in ds.graphs() if (graph.identifier, RDF.type, PROV.Bundle) in graph), None)
            if bundle is None:
                bundle = ds.graph(create_bundle(ds, self.recorder.purl))
                load_pkg_prov(bundle, self.recorder)
                bundle.add((run, RDF.type, PROV_EXT.Execution))
                bundle.add((run, RDF.type, OSLC_AUTO.AutomationResult))
                bundle.add((run, OSLC_AUTO.state, OSLC_AUTO.new))
                bundle.add((run, OSLC_AUTO.verdict, OSLC_AUTO.unavailable))
            yield bundle, run

    def lifecycle(self, run_id: str) -> tuple[State, Verdict]:
        """The run's state and verdict as the store keeps them; a run not stored is new.

        :raises ValueError: The stored bundle holds no run of this id under the observer's base
        """
        ds = self.store.load(run_id)
        if ds is None:
            return State.NEW, Verdict.UNAVAILABLE
        run = URIRef(self.base + run_id)
        bundle = next((graph for graph in ds.graphs() if (run, OSLC_AUTO.state, None) in graph), None)
        if bundle is None:
            raise ValueError(f"the bundle of run '{run_id}' has no '{run}' -- it was recorded under another base")
        state, verdict = bundle.value(run, OSLC_AUTO.state), bundle.value(run, OSLC_AUTO.verdict)
        return (
            next(name for name, iri in STATES.items() if iri == state),
            next(name for name, iri in VERDICTS.items() if iri == verdict),
        )

    def log_queued_run(self, run_id: str, queued_time: datetime):
        """Record a new run as queued; raises RuntimeError for a run that is not new."""
        if queued_time.utcoffset() is None:
            raise ValueError(f"queued time {queued_time} of run '{run_id}' has no time zone -- a run's times are wall-clock instants")
        with self._run(run_id) as (bundle, run):
            state = bundle.value(run, OSLC_AUTO.state)
            if state != OSLC_AUTO.new:
                raise RuntimeError(f"run '{run_id}' is {state.fragment} -- only a new run can be queued")
            bundle.set((run, OSLC_AUTO.state, OSLC_AUTO.queued))
            bundle.add((run, REC["queued-time"], Literal(queued_time)))

    def log_started_run(self, run_id: str, started_time: datetime):
        """Record a new or queued run as in progress, executed by this observer's rec.

        :raises RuntimeError: The run has already started, or was cancelled
        """
        with self._run(run_id) as (bundle, run):
            state = bundle.value(run, OSLC_AUTO.state)
            if state not in (OSLC_AUTO.new, OSLC_AUTO.queued):
                raise RuntimeError(f"run '{run_id}' is {state.fragment} -- only a run that has not started can start")
            # The rec that starts the run executes it; another rec may have queued it.
            if (self.recorder.purl, RDF.type, PROV.Agent) not in bundle:
                load_pkg_prov(bundle, self.recorder)
            started = URIRef(f"{run}/beginning")
            add_wall_instant(bundle, started, started_time)
            load_execution_prov(bundle, run, (), self.recorder.purl, started)
            bundle.set((run, OSLC_AUTO.state, OSLC_AUTO.inProgress))

    def log_run_heartbeat(self, run_id: str, beat_time: datetime):
        """Record that a started run is still alive; raises RuntimeError for any other run."""
        if beat_time.utcoffset() is None:
            raise ValueError(f"heartbeat time {beat_time} of run '{run_id}' has no time zone -- a run's times are wall-clock instants")
        with self._run(run_id) as (bundle, run):
            state = bundle.value(run, OSLC_AUTO.state)
            if state not in (OSLC_AUTO.inProgress, OSLC_AUTO.canceling):
                raise RuntimeError(f"run '{run_id}' is {state.fragment} -- only a started run beats")
            bundle.set((run, REC["heartbeat-time"], Literal(beat_time)))

    def log_ended_run(self, run_id: str, ended_time: datetime, verdict: Verdict, fail_trace: str | None = None):
        """End a started run with its verdict: a canceling run that passed ends canceled, any other ends complete.

        :param verdict: ``passed``, ``failed`` (its main raised) or ``error`` (it was interrupted)
        :param fail_trace: The trace of what ended a failed or error run
        :raises RuntimeError: The run has not started, or has ended
        """
        with self._run(run_id) as (bundle, run):
            state = bundle.value(run, OSLC_AUTO.state)
            if state not in (OSLC_AUTO.inProgress, OSLC_AUTO.canceling):
                raise RuntimeError(f"run '{run_id}' is {state.fragment} -- only a started run can end")
            ended = URIRef(f"{run}/end")
            add_wall_instant(bundle, ended, ended_time)
            bundle.add((run, TIME.hasEnd, ended))
            # A run that failed while canceling did not stop as asked: its verdict stands.
            if state == OSLC_AUTO.canceling and verdict is Verdict.PASSED:
                bundle.set((run, OSLC_AUTO.state, OSLC_AUTO.canceled))
                return
            bundle.set((run, OSLC_AUTO.state, OSLC_AUTO.complete))
            bundle.set((run, OSLC_AUTO.verdict, VERDICTS[verdict]))
            if fail_trace:
                bundle.add((run, REC["fail-trace"], Literal(fail_trace, datatype=XSD.string)))

    def log_cancelled_run(self, run_id: str):
        """Cancel a queued run, or ask a run in progress to stop; raises RuntimeError for any other run.

        A queued run that is cancelled never started, so it has neither a beginning nor an end.
        """
        with self._run(run_id) as (bundle, run):
            state = bundle.value(run, OSLC_AUTO.state)
            if state == OSLC_AUTO.queued:
                bundle.set((run, OSLC_AUTO.state, OSLC_AUTO.canceled))
            elif state == OSLC_AUTO.inProgress:
                bundle.set((run, OSLC_AUTO.state, OSLC_AUTO.canceling))
            else:
                raise RuntimeError(f"run '{run_id}' is {state.fragment} -- only a queued run or one in progress can be cancelled")

    def log_host_info(self, run_id: str, host_info: dict):
        """Record the machine executing the run from ``hostname``, ``os``, ``python`` and ``cpu``; other keys are not recorded."""
        with self._run(run_id) as (bundle, run):
            host = URIRef(f"{run}/host")
            bundle.add((host, RDF.type, REC.Host))
            bundle.add((host, RDF.type, PROV.Location))
            for key, predicate in HOST_TERMS.items():
                if host_info.get(key):
                    bundle.set((host, predicate, Literal(host_info[key])))
            bundle.set((run, PROV.atLocation, host))
