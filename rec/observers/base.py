import threading
from datetime import datetime

from rec import jsonld

RUN_BASE = "https://secoro.uni-bremen.de/rec/run/"


class BaseObserver:
    """One store, any number of runs: every call names the run it is about.

    A backend implements ``get_run``, ``update_run_data``, ``query_active_run`` and ``close``;
    the run's columns are the same in every store, and ``document`` is their JSON-LD form.
    """

    base = RUN_BASE

    def __init__(self):
        # A run's heartbeat thread and the run itself write the same row.
        self._lock = threading.RLock()

    def get_run(self, run_id: str) -> dict:
        raise NotImplementedError

    def update_run_data(self, run_id: str, column: str, data):
        raise NotImplementedError

    def query_active_run(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def run_iri(self, run_id: str) -> str:
        return self.base + run_id

    def document(self, run_id: str) -> dict:
        """The run as JSON-LD on the rec and prov-extension vocabularies."""
        return jsonld.document(self.run_iri(run_id), self.get_run(run_id))

    def log_queued_run(self, run_id: str, queued_time: datetime):
        self.update_run_data(run_id, "status", "QUEUED")
        self._run_info(run_id, queued_time=queued_time.isoformat())

    def log_started_run(self, run_id: str, started_time: datetime, trigger=None, starter=None):
        self.update_run_data(run_id, "status", "RUNNING")
        self._run_info(run_id, start_time=started_time.isoformat(), trigger=trigger, starter=starter)

    def log_run_heartbeat(self, run_id: str, beat_time: datetime, result):
        self._run_info(run_id, heartbeat_time=beat_time.isoformat(), result=result)

    def log_completed_run(self, run_id: str, completed_time: datetime):
        self.update_run_data(run_id, "status", "COMPLETED")
        self._run_info(run_id, end_time=completed_time.isoformat())

    def log_interrupted_run(self, run_id: str, interrupted_time: datetime, fail_trace: str | None = None):
        self.update_run_data(run_id, "status", "INTERRUPTED")
        self._run_info(run_id, end_time=interrupted_time.isoformat(), fail_trace=fail_trace)

    def log_failed_run(self, run_id: str, failed_time: datetime, fail_trace: str | None = None):
        self.update_run_data(run_id, "status", "FAILED")
        self._run_info(run_id, end_time=failed_time.isoformat(), fail_trace=fail_trace)

    def log_cancelled_run(self, run_id: str, cancelled_time: datetime):
        self.update_run_data(run_id, "status", "CANCELLED")
        self._run_info(run_id, end_time=cancelled_time.isoformat())

    def log_host_info(self, run_id: str, host_info: dict):
        self.update_run_data(run_id, "host_info", host_info)

    def log_sources(self, run_id: str, sources: list):
        self.update_run_data(run_id, "sources", sources)

    def log_repositories(self, run_id: str, repositories: list):
        self.update_run_data(run_id, "repositories", repositories)

    def log_dependencies(self, run_id: str, dependencies: list):
        self.update_run_data(run_id, "dependencies", dependencies)

    def log_scalar(self, run_id: str, metric_name: str, value, step: int | None = None, time: datetime | None = None):
        with self._lock:
            metrics = self.get_run(run_id).get("metrics") or []
            if step is None:
                step = sum(1 for metric in metrics if metric["name"] == metric_name)
            row = {"name": metric_name, "value": value, "step": step}
            if time is not None:
                row["time"] = time.isoformat()
            self.update_run_data(run_id, "metrics", metrics + [row])

    def add_agent(self, run_id: str, agent_id: str, agent_type: str, name: str | None = None):
        row = {"id": agent_id, "type": agent_type}
        if name is not None:
            row["name"] = name
        self._append(run_id, "agents", row)

    def add_resource(self, run_id: str, filename, usage_activity=None, usage_time=None, title=None, sha256=None, size_bytes=None):
        self._append(run_id, "resources", _file_row(filename, usage_activity, usage_time, title, sha256, size_bytes))

    def add_artefact(self, run_id: str, filename, gen_activity=None, generated_time=None, title=None, sha256=None, size_bytes=None):
        self._append(run_id, "artefacts", _file_row(filename, gen_activity, generated_time, title, sha256, size_bytes))

    def _run_info(self, run_id, **fields):
        with self._lock:
            info = self.get_run(run_id).get("run_info") or {}
            info.update({key: value for key, value in fields.items() if value is not None})
            self.update_run_data(run_id, "run_info", info)

    def _append(self, run_id, column, row):
        with self._lock:
            self.update_run_data(run_id, column, (self.get_run(run_id).get(column) or []) + [row])


def _file_row(filename, activity, time, title, sha256, size_bytes):
    row = {"path": str(filename)}
    for key, value in (("activity", activity), ("time", time.isoformat() if time else None), ("title", title), ("sha256", sha256), ("size_bytes", size_bytes)):
        if value is not None:
            row[key] = value
    return row
