"""What rec records of a run, independent of where it is stored."""

import re
from dataclasses import dataclass
from datetime import datetime

from rdf_utils.models.prov import PackageInfo
from rdflib import URIRef

from rec import State, Verdict

# RFC 3986 unreserved characters (2.3), so an id is the same in an IRI, a file name and a database row.
RUN_ID = re.compile(r"(?!\.\.?$)[A-Za-z0-9._~-]+")


@dataclass
class Host:
    hostname: str | None = None
    os: str | None = None
    python: str | None = None
    cpu: str | None = None


@dataclass
class RunRecord:
    """What rec knows of a run; also what FileStore.edit yields to change it (rec.stores.RunHandle).

    ``bundle`` names the run's provenance bundle and ``recorder`` is the rec that writes it, both
    set on the run's first edit.
    """

    run_id: str
    bundle: URIRef | None = None
    state: State = State.NEW
    verdict: Verdict = Verdict.UNAVAILABLE
    queued_time: datetime | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    heartbeat_time: datetime | None = None
    fail_trace: str | None = None
    recorder: PackageInfo | None = None
    host: Host | None = None

    def __post_init__(self):
        if not RUN_ID.fullmatch(self.run_id):
            raise ValueError(f"'{self.run_id}' is not a run id -- use letters, digits, '-', '.', '_' or '~', not '.' or '..'")

    def update(self, **values):
        for name, value in values.items():
            setattr(self, name, value)
