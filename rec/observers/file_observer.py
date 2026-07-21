# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Vamsi Kalagaturu

"""File-backed REC graph observer."""

from pathlib import Path

from rec.observers.graph_observer import GraphObserver, REC


class FileObserver(GraphObserver):
    """Persist one REC graph as JSON-LD.

    Args:
        path: Archive file to create or reopen.
    """

    def __init__(self, path):
        self.path = Path(path)
        super().__init__("unbound")
        if self.path.exists():
            self.graph.parse(self.path, format="json-ld")
            run = next(self.graph.subjects(REC["run-id"], None), None)
            if run is None:
                raise ValueError("existing file has no rec:run-id")
            self.run_id = str(self.graph.value(run, REC["run-id"]))

    def _persist(self):
        self._set_location(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(self.serialize() + "\n")
        temporary.replace(self.path)
