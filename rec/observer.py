"""Records the events of runs in a store."""

import os
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path, PurePath
from urllib.parse import urlsplit

from rdf_utils.models.prov import get_pkg_info

from rec import State, Verdict, provenance
from rec.record import (
    ORDER,
    Agent,
    Dependency,
    FileRef,
    Generation,
    Host,
    Metric,
    Repository,
    Software,
    Usage,
    from_json,
    to_json,
)
from rec.stores import Store

# PROV-O's agent classes (PROV-O 4.1).
AGENT_TYPES = ("Agent", "SoftwareAgent", "Person", "Organization")


class Observer:
    """Records the events of any number of runs in one store; every call names its run.

    Whoever creates the observer closes it, once every run it records is over.
    """

    def __init__(self, store: Store):
        self.store = store
        # Per thread: a heartbeat must wait for the store, not join another thread's held record.
        self._held = threading.local()

    @contextmanager
    def _hold(self, run_id: str):
        """Keep the run's record open: the calls made meanwhile are kept together when the block ends, or none is."""
        with self._record(run_id) as record:
            outer = getattr(self._held, "record", None)
            self._held.record = record
            try:
                yield
            finally:
                self._held.record = outer

    @contextmanager
    def _record(self, run_id: str):
        held = getattr(self._held, "record", None)
        if held is not None and held.run_id == run_id:
            yield held
            return
        with self.store.edit(run_id) as record:
            yield record
            for name, key in ORDER.items():
                getattr(record, name).sort(key=key)
            # Checked for every store, so none keeps a record another would change or could not keep.
            try:
                kept = from_json(to_json(record))
            except (TypeError, ValueError) as error:
                raise ValueError(f"run '{run_id}' holds a value JSON cannot keep -- {error}") from error
            if kept != record:
                raise ValueError(f"run '{run_id}' holds a value JSON would change -- e.g. a non-string key or a tuple")
            as_prov = provenance.record(provenance.graph(record), run_id)
            changed = [name for name in vars(record) if getattr(as_prov, name) != getattr(record, name)]
            if changed:
                raise ValueError(
                    f"run '{run_id}' would read back from PROV with other {changed} -- "
                    "e.g. an empty string, or one file, agent, repository or dependency logged twice"
                )

    def log_queued_run(self, run_id: str, queued_time: datetime):
        with self._record(run_id) as record:
            if record.state is not State.NEW:
                raise RuntimeError(f"run '{run_id}' is {record.state} -- only a new run can be queued")
            record.state, record.verdict, record.queued_time = State.QUEUED, Verdict.UNAVAILABLE, queued_time

    def log_started_run(self, run_id: str, started_time: datetime, trigger=None, starter=None, program=None):
        with self._record(run_id) as record:
            if record.state not in (State.NEW, State.QUEUED):
                raise RuntimeError(f"run '{run_id}' is {record.state} -- only a run that has not started can start")
            record.state, record.verdict, record.start_time = State.IN_PROGRESS, Verdict.UNAVAILABLE, started_time
            record.trigger, record.starter = trigger, starter
            record.program = file_ref(program) if program else None
            record.recorder = Software(*get_pkg_info("rec"))

    def log_run_heartbeat(self, run_id: str, beat_time: datetime, result=None):
        with self._record(run_id) as record:
            if record.state is not State.IN_PROGRESS:
                raise RuntimeError(f"run '{run_id}' is {record.state} -- only a run in progress beats")
            record.heartbeat_time = beat_time
            if result is not None:
                record.result = result

    def log_completed_run(self, run_id: str, completed_time: datetime, result=None):
        self._end(run_id, completed_time, Verdict.PASSED, result=result)

    def log_interrupted_run(self, run_id: str, interrupted_time: datetime, fail_trace: str | None = None):
        self._end(run_id, interrupted_time, Verdict.ERROR, fail_trace=fail_trace)

    def log_failed_run(self, run_id: str, failed_time: datetime, fail_trace: str | None = None):
        self._end(run_id, failed_time, Verdict.FAILED, fail_trace=fail_trace)

    def log_cancelled_run(self, run_id: str, cancelled_time: datetime):
        with self._record(run_id) as record:
            if record.state is not State.QUEUED:
                raise RuntimeError(f"run '{run_id}' is {record.state} -- only a queued run can be cancelled")
            record.state, record.verdict, record.end_time = State.CANCELED, Verdict.UNAVAILABLE, cancelled_time

    def log_host_info(self, run_id: str, host_info: dict):
        with self._record(run_id) as record:
            record.host = Host(host_info.get("hostname"), host_info.get("os"), host_info.get("python"), host_info.get("cpu"))

    def log_sources(self, run_id: str, sources: list):
        with self._record(run_id) as record:
            record.sources += [file_ref(row["path"], row.get("title"), row.get("sha256"), row.get("size_bytes")) for row in sources]

    def log_repositories(self, run_id: str, repositories: list):
        with self._record(run_id) as record:
            record.repositories += [Repository(row["name"], row.get("url"), row.get("commit")) for row in repositories]

    def log_dependencies(self, run_id: str, dependencies: list):
        with self._record(run_id) as record:
            record.dependencies += [Dependency(row["name"], row.get("version")) for row in dependencies]

    def log_scalar(self, run_id: str, metric_name: str, value, step: int | None = None, time: datetime | None = None):
        with self._record(run_id) as record:
            if step is None:
                step = sum(1 for metric in record.metrics if metric.name == metric_name)
            # One value per step: logging a step again is a re-measurement, not a second one.
            kept = [metric for metric in record.metrics if (metric.name, metric.step) != (metric_name, step)]
            record.metrics = [*kept, Metric(metric_name, step, value, time)]

    def add_agent(self, run_id: str, agent_id: str, agent_type: str, name: str | None = None):
        if agent_type not in AGENT_TYPES:
            raise ValueError(f"'{agent_type}' is not a PROV agent type -- use one of {list(AGENT_TYPES)}")
        if agent_type == "SoftwareAgent" and not name:
            raise ValueError(f"'{agent_id}' is a software agent without a name -- the prov-extension shapes require one")
        with self._record(run_id) as record:
            record.agents.append(Agent(agent_id, agent_type, name))

    def add_resource(self, run_id: str, filename, usage_activity=None, usage_time=None, title=None, sha256=None, size_bytes=None):
        with self._record(run_id) as record:
            record.resources.append(Usage(file_ref(filename, title, sha256, size_bytes), usage_activity, usage_time))

    def add_artefact(self, run_id: str, filename, gen_activity=None, generated_time=None, title=None, sha256=None, size_bytes=None):
        ref = file_ref(filename, title, sha256, size_bytes)
        with self._record(run_id) as record:
            # PROV-Constraints, generation-uniqueness: an entity has at most one generation.
            if any((row.file.path, row.file.root) == (ref.path, ref.root) for row in record.artefacts):
                raise ValueError(f"'{ref.path}' is already an artefact of run '{run_id}' -- a file is generated once")
            record.artefacts.append(Generation(ref, gen_activity, generated_time))

    def close(self):
        self.store.close()

    def _end(self, run_id, end_time, verdict, result=None, fail_trace=None):
        with self._record(run_id) as record:
            if record.state is not State.IN_PROGRESS:
                raise RuntimeError(f"run '{run_id}' is {record.state} -- only a run in progress can end")
            record.state, record.verdict, record.end_time = State.COMPLETE, verdict, end_time
            if result is not None:
                record.result = result
            record.fail_trace = fail_trace


def file_ref(path, title=None, sha256=None, size_bytes=None) -> FileRef:
    """A file as logged: an IRI as given, an absolute path resolved, a relative path from the working directory."""
    path = str(path)
    # An IRI's scheme is longer than a drive letter.
    if len(urlsplit(path).scheme) > 1:
        return FileRef(path, None, title, sha256, size_bytes)
    if Path(path).is_absolute():
        return FileRef(str(Path(path).resolve()), None, title, sha256, size_bytes)
    return FileRef(PurePath(path).as_posix(), os.getcwd(), title, sha256, size_bytes)
