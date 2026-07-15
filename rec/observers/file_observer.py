# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Vamsi Kalagaturu

"""File-backed REC graph observer."""

from pathlib import Path
from uuid import uuid4

from rdflib import Literal
from rdflib.namespace import XSD

from rec.observers.graph_observer import GraphObserver, REC


class FileObserver(GraphObserver):
    """Persist the shared REC graph as one JSON-LD file."""

    def __init__(self, path, file_id=None):
        self.path = Path(path)
        super().__init__("unbound")
        if self.path.exists():
            self.graph.parse(self.path, format="json-ld")
            run = next(self.graph.subjects(REC["run-id"], None), None)
            if run is None:
                raise ValueError("existing file has no rec:run-id")
            self.run_id = str(self.graph.value(run, REC["run-id"]))
            file_id = self.graph.value(run, REC["file-id"])
            if file_id is None:
                raise ValueError("existing file has no rec:file-id")
            self.file_id = str(file_id)
        else:
            self.file_id = file_id or f"file-{uuid4()}"

    def _persist(self):
        self.graph.set((self.run, REC["file-id"], Literal(self.file_id, datatype=XSD.string)))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(self.serialize() + "\n")
        temporary.replace(self.path)
