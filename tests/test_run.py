import os
import threading
from pathlib import Path

import pytest
from pyshacl import validate
from rdf_utils import DatasetFormat
from rdflib import Graph, URIRef
from rdflib.namespace import PROV, RDF, TIME

from rec import State, Verdict, provenance
from rec.observer import Observer
from rec.provenance import OSLC_AUTO, PROV_EXT, REC
from rec.record import RunRecord
from rec.run import Run
from rec.stores.file_store import FileStore

METAMODELS = Path(os.getenv("REC_METAMODELS_DIR", Path(__file__).resolve().parents[2] / "metamodels"))
SHAPES = Graph()
for name in ("prov.shacl.ttl", "prov-extension.shacl.ttl", "rec/rec.shacl.ttl"):
    SHAPES.parse(METAMODELS / name, format="turtle")


class CalibrationRun(Run):
    def main(self):
        return "calibrated"


class FailingRun(Run):
    def main(self):
        raise ValueError("no gripper attached")


class InterruptedRun(Run):
    def main(self):
        raise KeyboardInterrupt


class StoppedRun(Run):
    def main(self):
        # A scheduler holding only the id asks the run to stop while it is in progress.
        Run(observers=[Observer(self.observers[0].store)], run_id=self.id).cancel()
        if self.state is State.CANCELING:
            return "stopped"
        return "finished"


@pytest.fixture(params=list(DatasetFormat))
def store(request, tmp_path):
    return FileStore(tmp_path, fmt=request.param)


def test_a_completed_run_is_an_execution_by_rec(store):
    run = CalibrationRun(observers=[Observer(store)], run_id="run-1")
    run.beat_interval = 0
    assert run.run() == "calibrated"

    record = store.load("run-1")
    g = provenance.dataset(record).graph(record.bundle)
    ok, _, report = validate(g, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    node = URIRef(provenance.RUN_BASE + "run-1")
    assert (node, RDF.type, PROV_EXT.Execution) in g
    assert (record.bundle, RDF.type, PROV.Bundle) in g
    assert g.value(record.bundle, PROV.wasAttributedTo) == record.recorder.purl == g.value(node, PROV.wasAssociatedWith)
    assert record.recorder.name == "rec"


def test_a_run_keeps_one_bundle_through_its_lifecycle(store):
    run = CalibrationRun(observers=[Observer(store)], run_id="run-b")
    run.beat_interval = 0
    run.queue()
    queued = store.load("run-b").bundle
    run.run()
    assert queued is not None and store.load("run-b").bundle == queued


def test_a_queued_run_has_not_executed_yet(store):
    CalibrationRun(observers=[Observer(store)], run_id="run-q").queue()
    record = store.load("run-q")
    g = provenance.dataset(record).graph(record.bundle)
    ok, _, report = validate(g, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    node = URIRef(provenance.RUN_BASE + "run-q")
    assert {PROV_EXT.Execution, OSLC_AUTO.AutomationResult} <= set(g.objects(node, RDF.type))
    assert g.value(node, TIME.hasBeginning) is None and g.value(node, PROV.wasAssociatedWith) is None
    assert g.value(node, REC["queued-time"]) is not None


# Each starts from a completed run and breaks one rule of the run's shape.
@pytest.mark.parametrize(
    ("removed", "added"),
    [
        ([(OSLC_AUTO.verdict, None)], []),
        (
            [(OSLC_AUTO.state, None), (OSLC_AUTO.verdict, None)],
            [(OSLC_AUTO.state, OSLC_AUTO.queued), (OSLC_AUTO.verdict, OSLC_AUTO.unavailable)],
        ),
        ([(RDF.type, PROV_EXT.Execution)], []),
        ([(RDF.type, OSLC_AUTO.AutomationResult)], []),
    ],
    ids=["without a verdict", "queued with a start time", "not an execution", "not an automation result"],
)
def test_the_shapes_refuse_a_run_that_contradicts_its_state(store, removed, added):
    run = CalibrationRun(observers=[Observer(store)], run_id="run-x")
    run.beat_interval = 0
    run.run()
    record = store.load("run-x")
    g = provenance.dataset(record).graph(record.bundle)
    node = URIRef(provenance.RUN_BASE + "run-x")
    for predicate, value in removed:
        g.remove((node, predicate, value))
    for predicate, value in added:
        g.add((node, predicate, value))
    assert not validate(g, shacl_graph=SHAPES, inference="rdfs")[0]


@pytest.mark.parametrize(
    ("cls", "verdict", "trace"),
    [(FailingRun, Verdict.FAILED, "ValueError: no gripper attached"), (InterruptedRun, Verdict.ERROR, "KeyboardInterrupt")],
)
def test_a_run_that_stops_early_records_how(store, cls, verdict, trace):
    run = cls(observers=[Observer(store)], run_id="run-3")
    run.beat_interval = 0
    run.run()
    record = store.load("run-3")
    assert (record.state, record.verdict) == (State.COMPLETE, verdict)
    assert trace in record.fail_trace
    ok, _, report = validate(provenance.dataset(record).graph(record.bundle), shacl_graph=SHAPES, inference="rdfs")
    assert ok, report


def test_only_a_queued_run_is_cancelled(store):
    queued = CalibrationRun(observers=[Observer(store)], run_id="run-q")
    queued.queue()
    # A scheduler holding only the id cancels a run it did not create, and the store decides.
    Run(observers=[Observer(store)], run_id="run-q").cancel()
    assert queued.state is State.CANCELED
    with pytest.raises(RuntimeError, match="is canceled"):
        queued.run()
    record = store.load("run-q")
    assert (record.state, record.verdict) == (State.CANCELED, Verdict.UNAVAILABLE)
    g = provenance.dataset(record).graph(record.bundle)
    ok, _, report = validate(g, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    assert g.value(URIRef(provenance.RUN_BASE + "run-q"), TIME.hasBeginning) is None

    done = CalibrationRun(observers=[Observer(store)], run_id="run-d")
    done.beat_interval = 0
    done.run()
    with pytest.raises(RuntimeError, match="only a queued run or one in progress"):
        Run(observers=[Observer(store)], run_id="run-d").cancel()
    assert store.load("run-d").verdict is Verdict.PASSED
    assert store.run_ids(State.CANCELED) == ["run-q"]


def test_a_run_in_progress_stops_when_cancelled(store):
    run = StoppedRun(observers=[Observer(store)], run_id="run-s")
    run.beat_interval = 0
    assert run.run() == "stopped"
    record = store.load("run-s")
    assert (record.state, record.verdict) == (State.CANCELED, Verdict.UNAVAILABLE)
    assert record.start_time is not None and record.end_time is not None
    ok, _, report = validate(provenance.dataset(record).graph(record.bundle), shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    with pytest.raises(RuntimeError, match="only a queued run or one in progress"):
        run.cancel()


def test_one_observer_records_many_runs_at_once(store):
    """Every run is addressed by its id, and none closes the observer."""
    observer = Observer(store)
    runs = [CalibrationRun(observers=[observer], run_id=f"run-{i}") for i in range(6)]
    for run in runs:
        run.beat_interval = 0.01
    threads = [threading.Thread(target=run.run) for run in runs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert store.run_ids(State.COMPLETE) == sorted(run.id for run in runs)


@pytest.mark.parametrize("run_id", ["../escape", "/tmp/escape", "nested/run", "a run", "läuft", "", ".", ".."])
def test_a_run_id_is_unreserved_characters_only(store, run_id):
    with pytest.raises(ValueError, match="is not a run id"):
        RunRecord(run_id)
    with pytest.raises(ValueError, match="is not a run id"):
        store.path(run_id)
