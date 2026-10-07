"""What rec records of a run, independent of where it is stored."""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from rec import State, Verdict

TIMES = ("queued_time", "start_time", "end_time", "heartbeat_time")


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
class Agent:
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
    name: str
    step: int
    value: float | int
    time: datetime | None = None


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
    run_id: str
    state: State = State.NEW
    verdict: Verdict = Verdict.UNAVAILABLE
    queued_time: datetime | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    heartbeat_time: datetime | None = None
    result: Any = None
    fail_trace: str | None = None
    program: FileRef | None = None
    recorder: Software | None = None
    trigger: str | None = None
    starter: str | None = None
    host: Host | None = None
    agents: list[Agent] = field(default_factory=list)
    sources: list[FileRef] = field(default_factory=list)
    repositories: list[Repository] = field(default_factory=list)
    dependencies: list[Dependency] = field(default_factory=list)
    resources: list[Usage] = field(default_factory=list)
    artefacts: list[Generation] = field(default_factory=list)
    metrics: list[Metric] = field(default_factory=list)


def to_json(record: RunRecord) -> str:
    """The record as JSON, its times in ISO 8601."""
    return json.dumps(asdict(record), default=datetime.isoformat, indent=2)


def from_json(text: str) -> RunRecord:
    """The record ``to_json`` wrote."""
    data = json.loads(text)
    for row in [data, *data["resources"], *data["artefacts"], *data["metrics"]]:
        for key in (*TIMES, "time"):
            if row.get(key):
                row[key] = datetime.fromisoformat(row[key])
    return RunRecord(
        **{key: data[key] for key in ("run_id", "result", "fail_trace", "trigger", "starter", *TIMES)},
        state=State(data["state"]),
        verdict=Verdict(data["verdict"]),
        program=FileRef(**data["program"]) if data["program"] else None,
        recorder=Software(**data["recorder"]) if data["recorder"] else None,
        host=Host(**data["host"]) if data["host"] else None,
        agents=[Agent(**row) for row in data["agents"]],
        sources=[FileRef(**row) for row in data["sources"]],
        repositories=[Repository(**row) for row in data["repositories"]],
        dependencies=[Dependency(**row) for row in data["dependencies"]],
        resources=[Usage(FileRef(**row["file"]), row["activity"], row["time"]) for row in data["resources"]],
        artefacts=[Generation(FileRef(**row["file"]), row["activity"], row["time"]) for row in data["artefacts"]],
        metrics=[Metric(**row) for row in data["metrics"]],
    )
