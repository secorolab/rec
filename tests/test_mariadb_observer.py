import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest

pytest.importorskip("mariadb", reason="the MariaDB driver is an optional extra")
from rec import State, Verdict  # noqa: E402
from rec.observers.mariadb_observer import MariaDBObserver  # noqa: E402
from rec.run import Run  # noqa: E402

TEST_DATABASE = os.getenv("REC_TEST_MARIADB_DATABASE")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE,
    reason="set REC_TEST_MARIADB_DATABASE to run MariaDB integration tests",
)


@pytest.fixture
def observer():
    table = f"rec_test_{uuid4().hex}"
    db = MariaDBObserver(db_name=TEST_DATABASE, table=table)
    yield db
    db.cursor.execute(f"DROP TABLE IF EXISTS {table}")
    db.close()


class QuickRun(Run):
    def main(self):
        self.log_scalar("frames", 1)
        return "ok"


def test_one_connection_records_many_runs(observer):
    runs = [QuickRun(observers=[observer], run_id=f"run-{i}") for i in range(3)]
    for run in runs:
        run.beat_interval = 0
        run.run()
    for number, run in enumerate(runs, start=1):
        record = observer.get_run(run.id)
        assert (record["state"], record["verdict"]) == (State.COMPLETE, Verdict.PASSED)
        assert record["id"] == number
        assert record["run_info"]["result"] == "ok"
        assert record["metrics"] == [{"name": "frames", "value": 1, "step": 0, "time": record["metrics"][0]["time"]}]
    assert observer.document("run-0")["@graph"][0]["state"] == "complete"


def test_a_queued_run_is_cancelled_by_id(observer):
    QuickRun(observers=[observer], run_id="queued").queue()
    assert observer.query_active_run() is None
    observer.log_cancelled_run("queued", datetime.now(UTC))
    record = observer.get_run("queued")
    assert (record["state"], record["verdict"]) == (State.CANCELED, Verdict.UNAVAILABLE)


def test_a_table_from_before_run_ids_is_migrated(observer):
    table = f"{observer.table}_old"
    # The table as the previous release created it
    observer.cursor.execute(
        f"""
        CREATE TABLE {table} (
            id INT AUTO_INCREMENT PRIMARY KEY, status VARCHAR(20) NOT NULL, scenario_id VARCHAR(20),
            host_info JSON, sources JSON, repositories JSON, dependencies JSON, metrics JSON,
            agents JSON, resources JSON, artefacts JSON, run_info JSON, data JSON
        )
        """
    )
    observer.cursor.execute(f"INSERT INTO {table} (status) VALUES ('COMPLETED'), ('RUNNING')")
    try:
        migrated = MariaDBObserver(db_name=TEST_DATABASE, table=table)
        assert (migrated.get_run("1")["state"], migrated.get_run("1")["verdict"]) == (State.COMPLETE, Verdict.PASSED)
        assert migrated.get_run("2")["state"] is State.IN_PROGRESS
        assert migrated.query_active_run() == "2"
        QuickRun(observers=[migrated], run_id="run-new").run()
        assert migrated.get_run("run-new")["id"] == 3
        migrated.close()
    finally:
        observer.cursor.execute(f"DROP TABLE IF EXISTS {table}")
