# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Vamsi Kalagaturu

"""MariaDB integration tests for REC observer storage and file synchronisation."""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from rdflib import Graph
from rdflib.namespace import RDF

from rec.observers import FileObserver, MariaDBObserver
from rec.observers.graph_observer import REC
from rec.run import Run


TEST_DATABASE = os.getenv("REC_TEST_MARIADB_DATABASE")
pytestmark = pytest.mark.skipif(not TEST_DATABASE, reason="set REC_TEST_MARIADB_DATABASE to run MariaDB integration tests")


@pytest.fixture
def database():
    """Provide isolated tables in the explicitly configured test database."""
    tables = []

    def create():
        table = f"rec_test_{uuid4().hex}"
        observer = MariaDBObserver(db_name=TEST_DATABASE, table=table)
        tables.append((observer.conn, observer.cursor, table))
        return observer

    yield create

    for conn, cursor, table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table}_file_sources")
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        cursor.close()
        conn.close()


def stored_graph(observer, run_id):
    observer.cursor.execute(f"SELECT jsonld FROM {observer.table} WHERE run_id = ?", (run_id,))
    return Graph().parse(data=observer.cursor.fetchone()[0], format="json-ld")


def file_run(path, run_id, started_at):
    observer = FileObserver(path)
    observer.log_started_run(run_id, started_at)
    observer.log_completed_run(started_at + timedelta(seconds=1))
    return observer


def test_mariadb_only_run(database):
    observer = database()
    run = Run(observers=[observer], run_id="db-only")
    run._emit_started()
    run._emit_completed()

    graph = stored_graph(observer, "db-only")
    node = REC["activity/db-only"]
    assert (node, RDF.type, REC.CompletedRun) in graph
    assert graph.value(node, REC["file-id"]) is None


def test_file_and_mariadb_share_file_id(database, tmp_path):
    path = tmp_path / "run.jsonld"
    db = database()
    file = FileObserver(path)
    run = Run(observers=[file, db], run_id="both")
    run._emit_started()
    run._emit_completed()

    graph = stored_graph(db, "both")
    node = REC["activity/both"]
    assert str(graph.value(node, REC["file-id"])) == file.file_id
    db.cursor.execute(f"SELECT file_id, archive_path FROM {db.file_sources_table} WHERE run_id = ?", ("both",))
    assert db.cursor.fetchone() == (file.file_id, str(path))


def test_sync_file_preserves_file_identity(database, tmp_path):
    path = tmp_path / "run.jsonld"
    file = file_run(path, "file-only", datetime(2026, 1, 1, tzinfo=UTC))
    db = database()

    db.sync_file(path)

    graph = stored_graph(db, "file-only")
    node = REC["activity/file-only"]
    assert str(graph.value(node, REC["file-id"])) == file.file_id


def test_sync_files_uses_started_at_time_order_and_cursor(database, tmp_path):
    early = file_run(tmp_path / "early.jsonld", "early", datetime(2026, 1, 1, tzinfo=UTC))
    late = file_run(tmp_path / "late.jsonld", "late", datetime(2026, 1, 2, tzinfo=UTC))
    db = database()
    synced = []
    sync_file = db.sync_file
    db.sync_file = lambda path: (synced.append(path.name), sync_file(path))[1]

    assert db.sync_files(tmp_path) == 2
    assert synced == ["early.jsonld", "late.jsonld"]
    assert db.sync_files(tmp_path, started_after=datetime(2026, 1, 1, 12, tzinfo=UTC)) == 1
    db.cursor.execute(f"SELECT file_id FROM {db.file_sources_table} ORDER BY started_at")
    assert [row[0] for row in db.cursor] == [early.file_id, late.file_id]
