import os
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from rdflib.compare import isomorphic

pytest.importorskip("mariadb", reason="the MariaDB driver is an optional extra")
from rec import State, Verdict, provenance  # noqa: E402
from rec.observer import Observer  # noqa: E402
from rec.run import Run  # noqa: E402
from rec.stores.file_store import FileStore  # noqa: E402
from rec.stores.mariadb_store import TABLES, MariaDBStore  # noqa: E402

TEST_DATABASE = os.getenv("REC_TEST_MARIADB_DATABASE")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE,
    reason="set REC_TEST_MARIADB_DATABASE to run MariaDB integration tests",
)


@pytest.fixture
def store():
    prefix = f"rec_test_{uuid4().hex}"
    db = MariaDBStore(db_name=TEST_DATABASE, prefix=prefix)
    yield db
    db.cursor.execute(f"DROP TABLE IF EXISTS {', '.join(f'{prefix}_{kind}' for kind in TABLES)}, {prefix}_runs")
    db.close()


class QuickRun(Run):
    def main(self):
        self.add_artefact("/tmp/rec data/50% done.json")
        self.log_scalar("frames", 1)
        return "ok"


def test_the_tables_keep_the_whole_record(store, tmp_path):
    files = FileStore(tmp_path, fmt="json")
    runs = [QuickRun(observers=[Observer(store), Observer(files)], run_id=f"run-{i}") for i in range(3)]
    for run in runs:
        run.beat_interval = 0
        run.run()
    for run in runs:
        from_tables, from_json = store.load(run.id), files.load(run.id)
        assert from_tables is not None and from_json is not None
        assert isomorphic(provenance.graph(from_tables), provenance.graph(from_json))
    store.cursor.execute(f"SELECT state, verdict FROM {store.prefix}_runs WHERE run_id = 'run-0'")
    assert store.cursor.fetchone() == ("complete", "passed")
    # A metric is a row of its own, its int kept an int.
    store.cursor.execute(f"SELECT name, step, value, is_integer FROM {store.prefix}_metrics")
    assert store.cursor.fetchall() == [("frames", 0, 1.0, 1)] * 3
    assert store.load("run-0").metrics[0].value == 1 and isinstance(store.load("run-0").metrics[0].value, int)
    assert store.run_ids(State.COMPLETE) == ["run-0", "run-1", "run-2"]


def test_the_tables_refuse_a_file_of_an_unknown_step(store):
    observer = Observer(store)
    observer.add_activity("run-a", "https://example.org/step", "Execution", "https://example.org/agent", datetime.now(UTC))
    observer.add_resource("run-a", "config.yaml", usage_activity="https://example.org/step")
    with pytest.raises(ValueError, match="has no activity"):
        observer.add_artefact("run-a", "out.csv", gen_activity="https://example.org/missing")
    record = store.load("run-a")
    assert [activity.id for activity in record.activities] == ["https://example.org/step"]
    assert [usage.activity for usage in record.resources] == ["https://example.org/step"]
    assert record.artefacts == []


def test_a_queued_run_is_cancelled_by_id(store):
    QuickRun(observers=[Observer(store)], run_id="queued").queue()
    assert store.run_ids(State.IN_PROGRESS) == []
    Run(observers=[Observer(MariaDBStore(db_name=TEST_DATABASE, prefix=store.prefix))], run_id="queued").cancel()
    assert (store.load("queued").state, store.load("queued").verdict) == (State.CANCELED, Verdict.UNAVAILABLE)
