import json
import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest

pytest.importorskip("mariadb", reason="the MariaDB driver is an optional extra")
from rec import State, Verdict  # noqa: E402
from rec.observer import Observer  # noqa: E402
from rec.run import Run  # noqa: E402
from rec.stores.file_store import FileStore  # noqa: E402
from rec.stores.mariadb_store import MariaDBStore  # noqa: E402

TEST_DATABASE = os.getenv("REC_TEST_MARIADB_DATABASE")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE,
    reason="set REC_TEST_MARIADB_DATABASE to run MariaDB integration tests",
)


@pytest.fixture
def store():
    table = f"rec_test_{uuid4().hex}"
    db = MariaDBStore(db_name=TEST_DATABASE, table=table)
    yield db
    db.cursor.execute(f"DROP TABLE IF EXISTS {table}")
    db.close()


class QuickRun(Run):
    def main(self):
        self.add_artefact("/tmp/rec data/50% done.json")
        self.log_scalar("frames", 1)
        return "ok"


def test_a_row_keeps_the_whole_record(store, tmp_path):
    files = FileStore(tmp_path, fmt="json")
    runs = [QuickRun(observers=[Observer(store), Observer(files)], run_id=f"run-{i}") for i in range(3)]
    for run in runs:
        run.beat_interval = 0
        run.run()
    for number, run in enumerate(runs, start=1):
        assert store.load(run.id) == files.load(run.id)
        assert store.number(run.id) == number
    # The state and verdict columns are the record's, to query runs by.
    store.cursor.execute(f"SELECT state, verdict FROM {store.table} WHERE run_id = 'run-0'")
    assert store.cursor.fetchone() == ("complete", "passed")
    assert store.run_ids(State.COMPLETE) == ["run-0", "run-1", "run-2"]


def test_a_queued_run_is_cancelled_by_id(store):
    QuickRun(observers=[Observer(store)], run_id="queued").queue()
    assert store.run_ids(State.IN_PROGRESS) == []
    Observer(MariaDBStore(db_name=TEST_DATABASE, table=store.table)).log_cancelled_run("queued", datetime.now(UTC))
    assert (store.load("queued").state, store.load("queued").verdict) == (State.CANCELED, Verdict.UNAVAILABLE)


def test_a_table_from_the_previous_release_is_migrated(store):
    table = f"{store.table}_old"
    # The table as the previous release created it, and what it recorded of a run.
    store.cursor.execute(
        f"""
        CREATE TABLE {table} (
            id INT AUTO_INCREMENT PRIMARY KEY, status VARCHAR(20) NOT NULL, scenario_id VARCHAR(20),
            host_info JSON, sources JSON, repositories JSON, dependencies JSON, metrics JSON,
            agents JSON, resources JSON, artefacts JSON, run_info JSON, data JSON
        )
        """
    )
    started = datetime(2026, 1, 1, tzinfo=UTC)
    store.cursor.execute(
        f"INSERT INTO {table} (status, run_info) VALUES ('COMPLETED', ?), ('RUNNING', ?)",
        (json.dumps({"start_time": started.isoformat()}), json.dumps({"start_time": started.isoformat()})),
    )
    try:
        migrated = MariaDBStore(db_name=TEST_DATABASE, table=table)
        assert (migrated.load("1").state, migrated.load("1").verdict) == (State.COMPLETE, Verdict.PASSED)
        assert migrated.load("1").start_time == started
        assert migrated.run_ids(State.IN_PROGRESS) == ["2"]
        QuickRun(observers=[Observer(migrated)], run_id="run-new").run()
        assert migrated.number("run-new") == 3
        migrated.close()
    finally:
        store.cursor.execute(f"DROP TABLE IF EXISTS {table}")
