# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Vamsi Kalagaturu

"""File-backed REC graph observer."""

import json
from pathlib import Path

from rec.observers.graph_observer import OSLC_AUTO, GraphObserver, local_name, run_node, serialize


class FileObserver(GraphObserver):
    """Persist one REC graph as JSON-LD.

    Args:
        path: Archive file to create or reopen.
        run_iri: IRI of the run node, when the caller already minted it elsewhere.
    """

    def __init__(self, path, run_iri=None):
        self.path = Path(path)
        super().__init__("unbound", run_iri)
        if self.path.exists():
            self.graph.parse(self.path, format="json-ld")
            run = run_node(self.graph)
            if run is None:
                raise ValueError("existing file describes no run")
            self.run_id = local_name(run)
            # Reopening must continue the archive's own run node, never fork a second one.
            self.run_iri = self.run_iri or run

    def _write(self):
        # The archive is the directory this file sits in, so its own name is the portable path.
        self._set_location(self.path.name)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(serialize(self.graph) + "\n")
        temporary.replace(self.path)

    def _stored_state(self):
        # A plain JSON scan of our own compaction: parsing with rdflib fetches the context.
        try:
            document = json.loads(self.path.read_text())
        except (OSError, ValueError):
            return None
        for node in document.get("@graph", [document]):
            state = node.get("oslc_auto:state")
            if isinstance(state, dict):
                return OSLC_AUTO[state["@id"].removeprefix("oslc_auto:")]
        return None
