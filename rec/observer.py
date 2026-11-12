"""Records the events of runs in a store."""

import threading
from contextlib import contextmanager
from datetime import datetime
from uuid import uuid4

from rdf_utils.models.prov import get_py_pkg_info
from rdflib import URIRef

from rec import State, Verdict
from rec.record import Host
from rec.stores import Store


class Observer:
    """Records the events of any number of runs in one store; every call names its run.

    Whoever creates the observer closes it, once every run it records is over.
    """

    def __init__(self, store: Store):
        self.store = store
        # Per thread: a heartbeat must wait for the store, not join another thread's held run.
        self._held = threading.local()

    @contextmanager
    def _hold(self, run_id: str):
        """Keep the run open in the store: the calls made meanwhile are kept together when the block ends, or none is."""
        with self._run(run_id) as run:
            outer = getattr(self._held, "run", None)
            self._held.run = run
            try:
                yield
            finally:
                self._held.run = outer

    @contextmanager
    def _run(self, run_id: str):
        held = getattr(self._held, "run", None)
        if held is not None and held.run_id == run_id:
            yield held
            return
        with self.store.edit(run_id) as run:
            # A run's bundle is named once and attributed to the rec that first recorded it.
            if run.bundle is None:
                run.update(bundle=URIRef(uuid4().urn), recorder=get_py_pkg_info("rec"))
            yield run

    def log_queued_run(self, run_id: str, queued_time: datetime):
        """Record a new run as queued; raises RuntimeError for a run that is not new."""
        with self._run(run_id) as run:
            if run.state is not State.NEW:
                raise RuntimeError(f"run '{run_id}' is {run.state} -- only a new run can be queued")
            run.update(state=State.QUEUED, verdict=Verdict.UNAVAILABLE, queued_time=queued_time)

    def log_started_run(self, run_id: str, started_time: datetime):
        """Record a new or queued run as in progress.

        :raises RuntimeError: The run has already started, or was cancelled
        """
        with self._run(run_id) as run:
            if run.state not in (State.NEW, State.QUEUED):
                raise RuntimeError(f"run '{run_id}' is {run.state} -- only a run that has not started can start")
            run.update(state=State.IN_PROGRESS, verdict=Verdict.UNAVAILABLE, start_time=started_time)

    def log_run_heartbeat(self, run_id: str, beat_time: datetime):
        """Record that a started run is still alive; raises RuntimeError for any other run."""
        with self._run(run_id) as run:
            if run.state not in (State.IN_PROGRESS, State.CANCELING):
                raise RuntimeError(f"run '{run_id}' is {run.state} -- only a started run beats")
            run.update(heartbeat_time=beat_time)

    def log_completed_run(self, run_id: str, completed_time: datetime):
        """End a run in progress as passed, or a canceling one as canceled.

        :raises RuntimeError: The run has not started, or has ended
        """
        self._end(run_id, completed_time, Verdict.PASSED)

    def log_interrupted_run(self, run_id: str, interrupted_time: datetime, fail_trace: str | None = None):
        """End a run in progress with an error verdict and the trace of what interrupted it."""
        self._end(run_id, interrupted_time, Verdict.ERROR, fail_trace=fail_trace)

    def log_failed_run(self, run_id: str, failed_time: datetime, fail_trace: str | None = None):
        """End a run in progress as failed, with the trace of the exception that ended it."""
        self._end(run_id, failed_time, Verdict.FAILED, fail_trace=fail_trace)

    def log_cancelled_run(self, run_id: str, cancelled_time: datetime):
        """Cancel a queued run, or ask a run in progress to stop; raises RuntimeError for any other run."""
        with self._run(run_id) as run:
            if run.state is State.QUEUED:
                run.update(state=State.CANCELED, verdict=Verdict.UNAVAILABLE, end_time=cancelled_time)
            elif run.state is State.IN_PROGRESS:
                run.update(state=State.CANCELING)
            else:
                raise RuntimeError(f"run '{run_id}' is {run.state} -- only a queued run or one in progress can be cancelled")

    def log_host_info(self, run_id: str, host_info: dict):
        """Record the machine executing the run, from ``hostname``, ``os``, ``python`` and ``cpu``."""
        with self._run(run_id) as run:
            run.update(host=Host(host_info.get("hostname"), host_info.get("os"), host_info.get("python"), host_info.get("cpu")))

    def close(self):
        self.store.close()

    def _end(self, run_id, end_time, verdict, fail_trace=None):
        with self._run(run_id) as run:
            if run.state is State.CANCELING:
                # The shapes keep a fail trace for a failed or error verdict only.
                run.update(state=State.CANCELED, verdict=Verdict.UNAVAILABLE, end_time=end_time)
            elif run.state is State.IN_PROGRESS:
                run.update(state=State.COMPLETE, verdict=verdict, end_time=end_time, fail_trace=fail_trace)
            else:
                raise RuntimeError(f"run '{run_id}' is {run.state} -- only a started run can end")
