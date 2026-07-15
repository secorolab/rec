# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Argentina Ortega

"""MariaDB-backed REC graph observer."""

import io
import os
import re
from pathlib import Path
from uuid import uuid4

import mariadb
from dotenv import load_dotenv
from rdflib import Graph, Literal
from rdflib.namespace import PROV, RDF, XSD

from rec.observers.graph_observer import CONTEXT, GraphObserver, REC


class MariaDBObserver(GraphObserver):
    """Persist the same JSON-LD REC graph as ``FileObserver`` in MariaDB."""

    def __init__(self, run_id=None, db_name="logbook", table="runs"):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
            raise ValueError("table must be a simple SQL identifier")
        self.table = table
        self.file_id = None
        self.archive_path = None
        load_dotenv()
        self.conn = mariadb.connect(
            user=os.getenv("MARIADB_USER"),
            password=os.getenv("MARIADB_PASSWORD"),
            host=os.getenv("MARIADB_HOST"),
            port=int(os.getenv("MARIADB_PORT", "3306")),
            database=db_name,
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {self.table} ("
            "db_id BIGINT AUTO_INCREMENT PRIMARY KEY, "
            "run_id VARCHAR(255) NOT NULL UNIQUE, "
            "status VARCHAR(255) NOT NULL, "
            "jsonld LONGTEXT NOT NULL)"
        )
        self.file_sources_table = f"{self.table}_file_sources"
        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {self.file_sources_table} ("
            "file_id VARCHAR(255) PRIMARY KEY, "
            "run_id VARCHAR(255) NOT NULL, "
            "archive_path TEXT NOT NULL, "
            "started_at DATETIME(6) NOT NULL, "
            "synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
            "INDEX (run_id), INDEX (started_at))"
        )
        super().__init__(run_id or self._active_run_id() or f"run-{uuid4()}")
        self._load_existing()

    def query_active_run(self):
        return self._active_run_id()

    def _active_run_id(self):
        self.cursor.execute(
            f"SELECT run_id FROM {self.table} WHERE status = ? LIMIT 1",
            (str(REC.RunningRun),),
        )
        row = self.cursor.fetchone()
        return row[0] if row else None

    def _load_existing(self):
        self.cursor.execute(f"SELECT jsonld FROM {self.table} WHERE run_id = ?", (self.run_id,))
        row = self.cursor.fetchone()
        if row:
            self.graph.parse(io.StringIO(row[0]), format="json-ld")

    def _persist(self):
        if self.file_id is not None:
            self.graph.set((self.run, REC["file-id"], Literal(self.file_id, datatype=XSD.string)))
        self._upsert(self.run_id, self.graph)
        if self.file_id is not None:
            started_at = self.graph.value(self.run, PROV.startedAtTime)
            if started_at is not None:
                self._upsert_file_source(self.file_id, self.run_id, self.archive_path, started_at.toPython())

    def set_file_source(self, file_id, archive_path):
        """Associate this live database run with its file-backed source."""
        self.file_id = file_id
        self.archive_path = str(archive_path)

    def sync_file(self, path):
        """Import one completed file-backed run without changing its identities."""
        graph = Graph().parse(path, format="json-ld")
        run = next(graph.subjects(REC["run-id"], None), None)
        if run is None:
            raise ValueError("file has no rec:run-id")
        run_id = str(graph.value(run, REC["run-id"]))
        file_id = graph.value(run, REC["file-id"])
        started_at = graph.value(run, PROV.startedAtTime)
        if file_id is None or started_at is None:
            raise ValueError("file has no rec:file-id or prov:startedAtTime")
        self._upsert(run_id, graph)
        self._upsert_file_source(str(file_id), run_id, path, started_at.toPython())

    def sync_files(self, directory, started_after=None):
        """Import file runs in start-time order, optionally after a start-time cursor."""
        files = []
        for path in sorted(Path(directory).rglob("*.jsonld")):
            graph = Graph().parse(path, format="json-ld")
            run = next(graph.subjects(REC["run-id"], None), None)
            started_at = graph.value(run, PROV.startedAtTime) if run else None
            if started_at is not None and (started_after is None or started_at.toPython() > started_after):
                files.append((started_at.toPython(), path))
        for _, path in sorted(files):
            self.sync_file(path)
        return len(files)

    def _upsert(self, run_id, graph):
        run = next(graph.subjects(REC["run-id"], None), None)
        status = next(
            (str(run_type) for run_type in (REC.QueuedRun, REC.RunningRun, REC.CompletedRun, REC.FailedRun, REC.InterruptedRun, REC.CancelledRun) if (run, RDF.type, run_type) in graph),
            str(REC.QueuedRun),
        )
        self.cursor.execute(
            f"INSERT INTO {self.table} (run_id, status, jsonld) VALUES (?, ?, ?) "
            "ON DUPLICATE KEY UPDATE status = VALUES(status), jsonld = VALUES(jsonld)",
            (run_id, status, graph.serialize(format="json-ld", context=CONTEXT, auto_compact=True)),
        )

    def _upsert_file_source(self, file_id, run_id, archive_path, started_at):
        self.cursor.execute(
            f"INSERT INTO {self.file_sources_table} (file_id, run_id, archive_path, started_at) "
            "VALUES (?, ?, ?, ?) "
            "ON DUPLICATE KEY UPDATE run_id = VALUES(run_id), archive_path = VALUES(archive_path), started_at = VALUES(started_at)",
            (file_id, run_id, str(archive_path), started_at),
        )

    def close(self):
        self._persist()
        self.cursor.close()
        self.conn.close()
