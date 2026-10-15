"""What rec records of a run, independent of where it is stored."""

import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime

from rec import State, Verdict

# RFC 3986 unreserved characters (2.3), so an id is the same in an IRI, a file name and a database row.
RUN_ID = re.compile(r"(?!\.\.?$)[A-Za-z0-9._~-]+")
TIMES = ("queued_time", "start_time", "end_time", "heartbeat_time")
# What makes a row of each list one of a kind.
KEYS = {
    "agents": lambda agent: agent.id,
    "activities": lambda activity: activity.id,
    "repositories": lambda row: row.name,
    "dependencies": lambda row: row.name,
    "resources": lambda row: (row.activity, row.file.path, row.file.root),
    # PROV-Constraints, generation-uniqueness: an entity has at most one generation.
    "artefacts": lambda row: (row.file.path, row.file.root),
    "metrics": lambda row: (row.name, row.step),
}


@dataclass
class FileRef:
    """A file as logged: an absolute path, an IRI, or a path relative to ``root``."""

    path: str
    root: str | None = None
    title: str | None = None
    sha256: str | None = None
    size_bytes: int | None = None


@dataclass
class Usage:
    """A file an activity used; the activity is the run itself when None."""

    file: FileRef
    activity: str | None = None
    time: datetime | None = None


@dataclass
class Generation:
    """A file an activity generated; the activity is the run itself when None."""

    file: FileRef
    activity: str | None = None
    time: datetime | None = None


@dataclass
class Activity:
    """A step of the run, ``dcterms:isPartOf`` it; ``kind`` is its prov-extension class, e.g. ``Execution``."""

    id: str
    kind: str
    agent: str
    start_time: datetime
    end_time: datetime | None = None


@dataclass
class Agent:
    """A PROV agent associated with the run; ``type`` is its PROV-O class, e.g. ``Person``."""

    id: str
    type: str
    name: str | None = None


@dataclass
class Repository:
    name: str
    url: str | None = None
    commit: str | None = None


@dataclass
class Dependency:
    name: str
    version: str | None = None


@dataclass
class Metric:
    """A value of the metric ``name`` at ``step``; raises ValueError for a NaN or infinite value."""

    name: str
    step: int
    value: float | int
    time: datetime | None = None

    def __post_init__(self):
        # JSON (RFC 8259) and MariaDB's DOUBLE have no NaN or infinity.
        if not math.isfinite(self.value):
            raise ValueError(f"metric '{self.name}' is {self.value} -- a metric is finite")


@dataclass
class Software:
    """The package that recorded the run, as it was when the run started."""

    name: str
    version: str | None = None
    commit: str | None = None
    repository: str | None = None


@dataclass
class Host:
    hostname: str | None = None
    os: str | None = None
    python: str | None = None
    cpu: str | None = None


@dataclass
class RunRecord:
    """What rec knows of a run; also what FileStore.edit yields to change it (rec.stores.RunHandle)."""

    run_id: str
    state: State = State.NEW
    verdict: Verdict = Verdict.UNAVAILABLE
    queued_time: datetime | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    heartbeat_time: datetime | None = None
    fail_trace: str | None = None
    result: str | None = None
    program: FileRef | None = None
    recorder: Software | None = None
    trigger_entity: str | None = None
    trigger_generator: str | None = None
    host: Host | None = None
    agents: list[Agent] = field(default_factory=list)
    activities: list[Activity] = field(default_factory=list)
    repositories: list[Repository] = field(default_factory=list)
    dependencies: list[Dependency] = field(default_factory=list)
    resources: list[Usage] = field(default_factory=list)
    artefacts: list[Generation] = field(default_factory=list)
    metrics: list[Metric] = field(default_factory=list)

    def __post_init__(self):
        if not RUN_ID.fullmatch(self.run_id):
            raise ValueError(f"'{self.run_id}' is not a run id -- use letters, digits, '-', '.', '_' or '~', not '.' or '..'")

    def update(self, **values):
        for name, value in values.items():
            setattr(self, name, value)

    def add(self, kind: str, row, replace: bool = False):
        rows = getattr(self, kind)
        key = KEYS[kind](row)
        activity = row.activity if isinstance(row, (Usage, Generation)) else None
        if activity is not None and all(kept.id != activity for kept in self.activities):
            raise ValueError(f"run '{self.run_id}' has no activity '{activity}' -- add it to the run first")
        if not replace and any(KEYS[kind](kept) == key for kept in rows):
            raise ValueError(f"run '{self.run_id}' already has {key} among its {kind} -- each is logged once")
        rows[:] = [*(kept for kept in rows if KEYS[kind](kept) != key), row]

    def add_metric(self, name: str, value: float, step: int | None = None, time: datetime | None = None):
        if step is None:
            step = max((kept.step for kept in self.metrics if kept.name == name), default=-1) + 1
        self.add("metrics", Metric(name, step, value, time), replace=True)


def to_json(record: RunRecord) -> str:
    """The record as JSON (RFC 8259, so no NaN or infinity), its times in ISO 8601."""
    return json.dumps(asdict(record), default=datetime.isoformat, allow_nan=False, indent=2)


def from_json(text: str) -> RunRecord:
    """The record ``to_json`` wrote."""
    data = json.loads(text)
    for row in [data, *data.get("activities", []), *data["resources"], *data["artefacts"], *data["metrics"]]:
        for key in (*TIMES, "time"):
            if row.get(key):
                row[key] = datetime.fromisoformat(row[key])
    return RunRecord(
        **{key: data[key] for key in ("run_id", "fail_trace", "trigger_entity", "trigger_generator", *TIMES)},
        result=data.get("result"),
        state=State(data["state"]),
        verdict=Verdict(data["verdict"]),
        program=FileRef(**data["program"]) if data["program"] else None,
        recorder=Software(**data["recorder"]) if data["recorder"] else None,
        host=Host(**data["host"]) if data["host"] else None,
        agents=[Agent(**row) for row in data["agents"]],
        activities=[Activity(**row) for row in data.get("activities", [])],
        repositories=[Repository(**row) for row in data["repositories"]],
        dependencies=[Dependency(**row) for row in data["dependencies"]],
        resources=[Usage(FileRef(**row["file"]), row["activity"], row["time"]) for row in data["resources"]],
        artefacts=[Generation(FileRef(**row["file"]), row["activity"], row["time"]) for row in data["artefacts"]],
        metrics=[Metric(**row) for row in data["metrics"]],
    )
