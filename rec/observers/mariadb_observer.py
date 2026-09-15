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
    """Persist REC JSON-LD in MariaDB and import file archives.

    Args:
        run_id: Existing run to reopen, or an ID generated for a new run.
        db_name: Database selected from the configured MariaDB server.
        table: Base table name for runs and file-source mappings.

    Attributes:
        db_id: Short sequential number this database gave the run, for display
            and ordering. The canonical identity stays ``rec:run-id``, which is
            portable across databases as ``db_id`` is not. Archives synchronised
            with :meth:`sync_files` are numbered in start-time order.
    """

    def __init__(self, run_id=None, db_name="logbook", table="runs"):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
            raise ValueError("table must be a simple SQL identifier")
        self.table = table
        self.archive_path = None
        self.db_id = None
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
            "run_id VARCHAR(255) PRIMARY KEY, "
            "archive_path TEXT NOT NULL, "
            "started_at DATETIME(6) NOT NULL, "
            "synced_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
            "INDEX (started_at))"
        )
        super().__init__(run_id or self._active_run_id() or f"run-{uuid4()}")
        self._load_existing()

    def query_active_run(self):
        """Return one currently running database run, if present."""
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
        if self.archive_path is not None:
            self._set_location(self.archive_path)
        self._upsert(self.run_id, self.graph)
        if self.archive_path is not None:
            started_at = self.graph.value(self.run, PROV.startedAtTime)
            if started_at is not None:
                self._upsert_file_source(self.run_id, self.archive_path, started_at.toPython())

    def set_file_source(self, archive_path):
        """Associate this live database run with its file-backed source."""
        self.archive_path = str(archive_path)

    def sync_file(self, path):
        """Import one file-backed run without changing its REC identities."""
        graph = Graph().parse(path, format="json-ld")
        run = next(graph.subjects(REC["run-id"], None), None)
        if run is None:
            raise ValueError("file has no rec:run-id")
        run_id = str(graph.value(run, REC["run-id"]))
        started_at = graph.value(run, PROV.startedAtTime)
        if started_at is None:
            raise ValueError("file has no prov:startedAtTime")
        self._upsert(run_id, graph)
        self._upsert_file_source(run_id, path, started_at.toPython())

    def sync_files(self, directory, started_after=None):
        """Import archive files in start-time order, optionally after a cursor."""
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
        if run_id == self.run_id and self.db_id is None:
            self.db_id = self._db_id(run_id)

    def _db_id(self, run_id):
        self.cursor.execute(f"SELECT db_id FROM {self.table} WHERE run_id = ?", (run_id,))
        row = self.cursor.fetchone()
        return row[0] if row else None

    def _upsert_file_source(self, run_id, archive_path, started_at):
        self.cursor.execute(
            f"INSERT INTO {self.file_sources_table} (run_id, archive_path, started_at) "
            "VALUES (?, ?, ?) "
            "ON DUPLICATE KEY UPDATE archive_path = VALUES(archive_path), started_at = VALUES(started_at)",
            (run_id, str(archive_path), started_at),
        )

    def close(self):
        """Flush a live run, then close the database cursor and connection."""
        if self.graph.value(self.run, REC["run-id"]) is not None:
            self._persist()
        self.cursor.close()
        self.conn.close()
