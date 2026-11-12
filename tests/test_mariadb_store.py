import os
from pathlib import Path
from uuid import uuid4

import pytest
from pyshacl import validate
from rdflib import Graph

pytest.importorskip("mariadb", reason="the MariaDB driver is an optional extra")
from rec import State, Verdict  # noqa: E402
from rec.run import Run  # noqa: E402
from rec.stores.mariadb_store import MariaDBStore  # noqa: E402

METAMODELS = Path(os.getenv("REC_METAMODELS_DIR", Path(__file__).resolve().parents[2] / "metamodels"))
SHAPES = Graph()
for name in ("prov.shacl.ttl", "prov-extension.shacl.ttl", "rec/rec.shacl.ttl"):
    SHAPES.parse(METAMODELS / name, format="turtle")
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
    db.cursor.execute(f"DROP TABLE IF EXISTS {prefix}_runs")
    db.close()


def succeed(run):
    return "ok"


def test_a_row_keeps_the_run_as_a_valid_bundle(store):
    run = Run(store, "run-0")
    run.run(succeed)
    assert run.lifecycle() == (State.COMPLETE, Verdict.PASSED)
    ok, _, report = validate(store.load("run-0"), shacl_graph=SHAPES, inference="rdfs")
    assert ok, report


def test_a_queued_run_is_cancelled_by_id_from_another_connection(store):
    run = Run(store, "queued")
    run.queue()
    scheduler = MariaDBStore(db_name=TEST_DATABASE, prefix=store.prefix)
    Run(scheduler, "queued").cancel()
    scheduler.close()
    assert run.lifecycle() == (State.CANCELED, Verdict.UNAVAILABLE)


def test_run_ids_differing_in_case_are_two_runs(store):
    Run(store, "Pick-1").run(succeed)
    Run(store, "pick-1").queue()
    assert Run(store, "Pick-1").lifecycle() == (State.COMPLETE, Verdict.PASSED)
    assert Run(store, "pick-1").lifecycle() == (State.QUEUED, Verdict.UNAVAILABLE)


@pytest.mark.parametrize("prefix", ["team-a", "logs; DROP TABLE x", ""])
def test_a_prefix_that_is_not_a_table_name_is_refused(prefix):
    with pytest.raises(ValueError, match="is not a table prefix"):
        MariaDBStore(db_name=TEST_DATABASE, prefix=prefix)
