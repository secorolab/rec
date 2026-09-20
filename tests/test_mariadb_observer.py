import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest

pytest.importorskip("mariadb", reason="the MariaDB driver is an optional extra")
from rec.observers.mariadb_observer import MariaDBObserver  # noqa: E402
from rec.run import Run, RunStatus  # noqa: E402

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
    db.cursor.close()
    db.conn.close()


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
        assert record["status"] == "COMPLETED"
        assert record["id"] == number
        assert record["metrics"] == [{"name": "frames", "value": 1, "step": 0, "time": record["metrics"][0]["time"]}]
    assert observer.document("run-0")["@graph"][0]["state"] == "complete"


def test_a_queued_run_is_cancelled_by_id(observer):
    QuickRun(observers=[observer], run_id="queued").queue()
    assert observer.query_active_run() is None
    observer.log_cancelled_run("queued", datetime.now(UTC))
    assert observer.get_run("queued")["status"] == RunStatus.CANCELLED
