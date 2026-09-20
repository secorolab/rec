"""Observer that keeps one JSON-LD document per run in a directory."""

import json
from pathlib import Path

from rec import State, jsonld
from rec.observers.base import BaseObserver


class FileObserver(BaseObserver):
    def __init__(self, directory, base=None):
        """
        Observer that writes each run to ``<directory>/<run_id>.ld.json``

        :param directory: Where the documents go; created on first write
        :param base: IRI base of the run nodes, ``BaseObserver.base`` by default
        """
        super().__init__()
        self.directory = Path(directory)
        if base is not None:
            self.base = base
        # Columns of a run whose document cannot be written yet: no state or verdict so far.
        self._pending = {}

    def path(self, run_id: str) -> Path:
        if not run_id or run_id == ".." or Path(run_id).name != run_id:
            raise ValueError(f"{run_id!r} is not a file name")
        return self.directory / f"{run_id}.ld.json"

    def get_run(self, run_id):
        path = self.path(run_id)
        if path.exists():
            return jsonld.record(json.loads(path.read_text()))
        return dict(self._pending.get(run_id, {}))

    def update_run_data(self, run_id, column, data):
        with self._lock:
            record = self.get_run(run_id)
            record[column] = data
            if "state" not in record or "verdict" not in record:
                self._pending[run_id] = record
                return
            self._pending.pop(run_id, None)
            self.directory.mkdir(parents=True, exist_ok=True)
            temporary = self.path(run_id).with_suffix(".tmp")
            temporary.write_text(json.dumps(self.document_of(run_id, record), indent=2) + "\n")
            temporary.replace(self.path(run_id))

    def document_of(self, run_id, record):
        return jsonld.document(self.run_iri(run_id), record)

    def query_active_run(self):
        for path in sorted(self.directory.glob("*.ld.json")):
            run_id = path.name.removesuffix(".ld.json")
            if self.get_run(run_id).get("state") is State.IN_PROGRESS:
                return run_id
        return None

    def close(self):
        pass
