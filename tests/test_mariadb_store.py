import os
from uuid import uuid4

import pytest
from rdf_utils.models.prov import PackageInfo
from rdflib import URIRef
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
CONTROLLER = PackageInfo(
    name="controller",
    purl=URIRef("pkg:git/example.org/lab/controller@0123abcd"),
    version="1.2.0",
    revision="0123abcd",
    repository="https://example.org/lab/controller",
    dirty=True,
)


@pytest.fixture
def store():
    prefix = f"rec_test_{uuid4().hex}"
    db = MariaDBStore(db_name=TEST_DATABASE, prefix=prefix)
    yield db
    db.cursor.execute(f"DROP TABLE IF EXISTS {', '.join(f'{prefix}_{table}' for table in TABLES)}")
    db.close()


class QuickRun(Run):
    def main(self):
        self.log_packages([CONTROLLER])
        return "ok"


def test_the_tables_keep_the_whole_record(store, tmp_path):
    files = FileStore(tmp_path)
    runs = [QuickRun(observers=[Observer(store), Observer(files)], run_id=f"run-{i}") for i in range(3)]
    for run in runs:
        run.beat_interval = 0
        run.run()
    for run in runs:
        from_tables, from_file = store.load(run.id), files.load(run.id)
        assert from_tables is not None and from_file is not None
        # Each store mints its own bundle on the run's first edit; the run inside it is the same.
        from_tables.bundle = from_file.bundle
        assert isomorphic(
            provenance.dataset(from_tables).graph(from_tables.bundle),
            provenance.dataset(from_file).graph(from_file.bundle),
        )
    store.cursor.execute(f"SELECT state, verdict FROM {store.prefix}_runs WHERE run_id = 'run-0'")
    assert store.cursor.fetchone() == ("complete", "passed")
    store.cursor.execute(f"SELECT name, recorder FROM {store.prefix}_packages ORDER BY run, recorder")
    assert store.cursor.fetchall() == [("controller", 0), ("rec", 1)] * 3
    assert store.run_ids(State.COMPLETE) == ["run-0", "run-1", "run-2"]


def test_a_queued_run_is_cancelled_by_id(store):
    QuickRun(observers=[Observer(store)], run_id="queued").queue()
    assert store.run_ids(State.IN_PROGRESS) == []
    Run(observers=[Observer(MariaDBStore(db_name=TEST_DATABASE, prefix=store.prefix))], run_id="queued").cancel()
    assert (store.load("queued").state, store.load("queued").verdict) == (State.CANCELED, Verdict.UNAVAILABLE)
