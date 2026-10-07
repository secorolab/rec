"""A store keeping each run as one file in a directory."""

import fcntl
import json
from contextlib import contextmanager
from pathlib import Path

from rdflib import Graph

from rec import State, provenance
from rec.record import RunRecord, from_json, to_json

# The file each format keeps a run in: its record, or its PROV document.
SUFFIXES = {"json": ".json", "rdf": ".ld.json"}


class FileStore:
    def __init__(self, directory, fmt: str = "rdf", base: str = provenance.RUN_BASE):
        """
        :param directory: Where the files go; created on first write
        :param fmt: ``rdf`` for the run's PROV document, ``json`` for its record as JSON
        :param base: IRI base of the run nodes in a PROV document
        """
        if fmt not in SUFFIXES:
            raise ValueError(f"'{fmt}' is not a file format -- use one of {list(SUFFIXES)}")
        self.directory = Path(directory)
        self.fmt = fmt
        self.base = base

    def path(self, run_id: str) -> Path:
        if not run_id or run_id in (".", "..") or Path(run_id).name != run_id:
            raise ValueError(f"'{run_id}' is not a file name -- a run id names its file in the directory")
        return self.directory / f"{run_id}{SUFFIXES[self.fmt]}"

    def load(self, run_id: str) -> RunRecord | None:
        path = self.path(run_id)
        if not path.exists():
            return None
        if self.fmt == "json":
            return from_json(path.read_text())
        return provenance.record(Graph().parse(path, format="json-ld"), run_id, self.base)

    @contextmanager
    def edit(self, run_id: str):
        path = self.path(run_id)
        self.directory.mkdir(parents=True, exist_ok=True)
        # The lock file stays: removing it would let a waiting process lock a file no longer in the directory.
        with open(path.with_name(f"{run_id}.lock"), "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            record = self.load(run_id) or RunRecord(run_id)
            yield record
            self.save(record)

    def save(self, record: RunRecord) -> None:
        if self.fmt == "json":
            text = to_json(record)
        else:
            text = json.dumps(provenance.document(provenance.graph(record, self.base)), indent=2)
        self.directory.mkdir(parents=True, exist_ok=True)
        path = self.path(record.run_id)
        temporary = path.with_name(f"{path.name}.tmp")
        temporary.write_text(text + "\n")
        temporary.replace(path)

    def run_ids(self, state: State | None = None) -> list[str]:
        suffix = SUFFIXES[self.fmt]
        run_ids = sorted(path.name.removesuffix(suffix) for path in self.directory.glob(f"*{suffix}"))
        return [run_id for run_id in run_ids if state is None or self.load(run_id).state is state]

    def close(self) -> None:
        """Nothing stays open between writes."""
