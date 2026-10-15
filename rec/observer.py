"""Records the events of runs in a store."""

import os
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path, PurePath
from urllib.parse import urlsplit

from rdf_utils.models.prov import get_pkg_info

from rec import State, Verdict
from rec.record import (
    Activity,
    Agent,
    Dependency,
    FileRef,
    Generation,
    Host,
    Repository,
    Software,
    Usage,
)
from rec.stores import Store

# PROV-O's agent classes (PROV-O 4.1).
AGENT_TYPES = ("Agent", "SoftwareAgent", "Person", "Organization")
ACTIVITY_KINDS = ("Specification", "Transformation", "Generalization", "Execution", "Assurance")


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
            yield run

    def log_queued_run(self, run_id: str, queued_time: datetime):
        """Record a new run as queued; raises RuntimeError for a run that is not new."""
        with self._run(run_id) as run:
            if run.state is not State.NEW:
                raise RuntimeError(f"run '{run_id}' is {run.state} -- only a new run can be queued")
            run.update(state=State.QUEUED, verdict=Verdict.UNAVAILABLE, queued_time=queued_time)

    def log_started_run(self, run_id: str, started_time: datetime, trigger_entity=None, trigger_generator=None, program=None):
        """Record a new or queued run as in progress, with what started it and rec as its recorder.

        :param trigger_entity: An entity that triggers the run
        :param trigger_generator: The activity that generated the trigger entity
        :param program: The file defining the run
        :raises RuntimeError: The run has already started, or was cancelled
        """
        with self._run(run_id) as run:
            if run.state not in (State.NEW, State.QUEUED):
                raise RuntimeError(f"run '{run_id}' is {run.state} -- only a run that has not started can start")
            run.update(
                state=State.IN_PROGRESS,
                verdict=Verdict.UNAVAILABLE,
                start_time=started_time,
                trigger_entity=trigger_entity,
                trigger_generator=trigger_generator,
                program=file_ref(program) if program else None,
                recorder=Software(*get_pkg_info("rec")),
            )

    def log_run_heartbeat(self, run_id: str, beat_time: datetime):
        """Record that a started run is still alive; raises RuntimeError for any other run."""
        with self._run(run_id) as run:
            if run.state not in (State.IN_PROGRESS, State.CANCELING):
                raise RuntimeError(f"run '{run_id}' is {run.state} -- only a started run beats")
            run.update(heartbeat_time=beat_time)

    def log_completed_run(self, run_id: str, completed_time: datetime, result: str | None = None):
        """End a run in progress as passed, or a canceling one as canceled, with what its ``main`` returned.

        :raises RuntimeError: The run has not started, or has ended
        """
        self._end(run_id, completed_time, Verdict.PASSED, result=result)

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

    def log_repositories(self, run_id: str, repositories: list):
        """Record repositories, rows with a ``name`` and optionally ``url``, ``commit``; refuses a name twice."""
        with self._run(run_id) as run:
            for row in repositories:
                run.add("repositories", Repository(row["name"], row.get("url"), row.get("commit")))

    def log_dependencies(self, run_id: str, dependencies: list):
        """Record dependencies, rows with a ``name`` and optionally ``version``; refuses a name twice."""
        with self._run(run_id) as run:
            for row in dependencies:
                run.add("dependencies", Dependency(row["name"], row.get("version")))

    def log_scalar(self, run_id: str, metric_name: str, value, step: int | None = None, time: datetime | None = None):
        """Record a metric's value at ``step``, or at its next step; a step logged again gets the new value.

        :raises ValueError: The value is NaN or infinite
        """
        with self._run(run_id) as run:
            run.add_metric(metric_name, value, step, time)

    def add_agent(self, run_id: str, agent_id: str, agent_type: str, name: str | None = None):
        """Associate an agent with the run.

        :param agent_type: A PROV-O agent class: ``Agent``, ``SoftwareAgent``, ``Person`` or ``Organization``
        :raises ValueError: The type is not one of those, a software agent has no name, or the agent is already associated
        """
        if agent_type not in AGENT_TYPES:
            raise ValueError(f"'{agent_type}' is not a PROV agent type -- use one of {list(AGENT_TYPES)}")
        if agent_type == "SoftwareAgent" and not name:
            raise ValueError(f"'{agent_id}' is a software agent without a name -- the prov-extension shapes require one")
        with self._run(run_id) as run:
            run.add("agents", Agent(agent_id, agent_type, name))

    def add_activity(self, run_id: str, activity_id: str, kind: str, agent_id: str, start_time: datetime, end_time: datetime | None = None):
        """Add a step of the run, done by ``agent_id``; a file it used or generated names it as its activity.

        :param kind: A prov-extension activity class: ``Specification``, ``Transformation``, ``Generalization``,
            ``Execution`` or ``Assurance``
        :raises ValueError: The kind is not one of those, or the run already has the activity
        """
        if kind not in ACTIVITY_KINDS:
            raise ValueError(f"'{kind}' is not a prov-extension activity class -- use one of {list(ACTIVITY_KINDS)}")
        with self._run(run_id) as run:
            run.add("activities", Activity(activity_id, kind, agent_id, start_time, end_time))

    def add_resource(self, run_id: str, filename, usage_activity=None, usage_time=None, title=None, sha256=None, size_bytes=None):
        """Record a file used by ``usage_activity``, or by the run; refuses one file used twice by one activity."""
        with self._run(run_id) as run:
            run.add("resources", Usage(file_ref(filename, title, sha256, size_bytes), usage_activity, usage_time))

    def add_artefact(self, run_id: str, filename, gen_activity=None, generated_time=None, title=None, sha256=None, size_bytes=None):
        """Record a file generated by ``gen_activity``, or by the run; refuses a file generated twice."""
        with self._run(run_id) as run:
            run.add("artefacts", Generation(file_ref(filename, title, sha256, size_bytes), gen_activity, generated_time))

    def close(self):
        self.store.close()

    def _end(self, run_id, end_time, verdict, fail_trace=None, result=None):
        with self._run(run_id) as run:
            if run.state is State.CANCELING:
                # The shapes keep a fail trace for a failed or error verdict only.
                run.update(state=State.CANCELED, verdict=Verdict.UNAVAILABLE, end_time=end_time, result=result)
            elif run.state is State.IN_PROGRESS:
                run.update(state=State.COMPLETE, verdict=verdict, end_time=end_time, fail_trace=fail_trace, result=result)
            else:
                raise RuntimeError(f"run '{run_id}' is {run.state} -- only a started run can end")


def file_ref(path, title=None, sha256=None, size_bytes=None) -> FileRef:
    """A file as logged: an IRI as given, an absolute path resolved, a relative path from the working directory."""
    path = str(path)
    # An IRI's scheme is longer than a drive letter.
    if len(urlsplit(path).scheme) > 1:
        return FileRef(path, None, title, sha256, size_bytes)
    if Path(path).is_absolute():
        return FileRef(str(Path(path).resolve()), None, title, sha256, size_bytes)
    return FileRef(PurePath(path).as_posix(), os.getcwd(), title, sha256, size_bytes)
