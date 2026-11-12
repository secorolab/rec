import os
import threading
from datetime import datetime
from pathlib import Path

import pytest
from pyshacl import validate
from rdf_utils.models.prov import create_bundle
from rdflib import Graph, URIRef
from rdflib.namespace import PROV, RDF, TIME

from rec import State, Verdict
from rec.observer import Observer
from rec.provenance import OSLC_AUTO, PROV_EXT, REC, RUN_BASE
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


class ExitingRun(Run):
    def main(self):
        raise SystemExit(3)


class StoppedRun(Run):
    def main(self):
        # A scheduler holding only the id asks the run to stop while it is in progress.
        Run(Observer(self.observer.store), run_id=self.id).cancel()
        if self.state is State.CANCELING:
            return "stopped"
        return "finished"


class CrashedWhileStoppingRun(Run):
    def main(self):
        Run(Observer(self.observer.store), run_id=self.id).cancel()
        raise ValueError("gripper jammed while releasing")


class SlowRun(Run):
    def main(self):
        threading.Event().wait(0.05)


class HostlessRun(Run):
    def log_host_info(self, host_info):
        raise OSError("no hostname")

    def main(self):
        raise AssertionError("main must not run when the start cannot be completed")


@pytest.fixture
def store(tmp_path):
    return FileStore(tmp_path)


def test_a_completed_run_is_an_execution_by_rec_in_a_bundle_attributed_to_rec(store):
    observer = Observer(store)
    assert CalibrationRun(observer, run_id="run-1").run() == "calibrated"

    ds = store.load("run-1")
    ok, _, report = validate(ds, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    [bundle] = [graph for graph in ds.graphs() if (graph.identifier, RDF.type, PROV.Bundle) in graph]
    node = URIRef(RUN_BASE + "run-1")
    assert (node, RDF.type, PROV_EXT.Execution) in bundle
    assert bundle.value(bundle.identifier, PROV.wasAttributedTo) == observer.recorder.purl == bundle.value(node, PROV.wasAssociatedWith)
    assert observer.lifecycle("run-1") == (State.COMPLETE, Verdict.PASSED)


def test_a_run_keeps_one_bundle_through_its_lifecycle(store):
    run = CalibrationRun(Observer(store), run_id="run-b")
    run.queue()
    queued = {graph.identifier for graph in store.load("run-b").graphs() if (graph.identifier, RDF.type, PROV.Bundle) in graph}
    run.run()
    ended = {graph.identifier for graph in store.load("run-b").graphs() if (graph.identifier, RDF.type, PROV.Bundle) in graph}
    assert len(queued) == 1 and ended == queued


def test_an_event_changes_only_its_own_triples(store):
    """A triple another tool put in the run's bundle survives rec's later events."""
    observer = Observer(store)
    CalibrationRun(observer, run_id="run-k").queue()
    with store.edit("run-k") as ds:
        [bundle] = [graph for graph in ds.graphs() if (graph.identifier, RDF.type, PROV.Bundle) in graph]
        bundle.add((URIRef(RUN_BASE + "run-k"), PROV.wasInfluencedBy, URIRef("https://example.org/schedule/1")))
    CalibrationRun(observer, run_id="run-k").run()
    ds = store.load("run-k")
    ds.default_union = True
    assert (URIRef(RUN_BASE + "run-k"), PROV.wasInfluencedBy, URIRef("https://example.org/schedule/1")) in ds


def test_another_tools_bundle_in_the_dataset_is_left_alone(store):
    tool = URIRef("https://example.org/agent/scheduler")
    with store.edit("run-t") as ds:
        foreign = create_bundle(ds, tool)
    observer = Observer(store)
    CalibrationRun(observer, run_id="run-t").run()
    assert observer.lifecycle("run-t") == (State.COMPLETE, Verdict.PASSED)
    ds = store.load("run-t")
    assert set(ds.graph(foreign)) == {(foreign, RDF.type, PROV.Bundle), (foreign, PROV.wasAttributedTo, tool)}


def test_a_run_that_cannot_finish_starting_still_ends(store):
    observer = Observer(store)
    HostlessRun(observer, run_id="run-n").run()
    assert observer.lifecycle("run-n") == (State.COMPLETE, Verdict.FAILED)
    ds = store.load("run-n")
    ds.default_union = True
    assert "OSError: no hostname" in str(ds.value(URIRef(RUN_BASE + "run-n"), REC["fail-trace"]))


def test_a_queued_run_has_not_executed_yet(store):
    CalibrationRun(Observer(store), run_id="run-q").queue()
    ds = store.load("run-q")
    ok, _, report = validate(ds, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    ds.default_union = True
    node = URIRef(RUN_BASE + "run-q")
    assert {PROV_EXT.Execution, OSLC_AUTO.AutomationResult} <= set(ds.objects(node, RDF.type))
    assert ds.value(node, TIME.hasBeginning) is None and ds.value(node, PROV.wasAssociatedWith) is None
    assert ds.value(node, REC["queued-time"]) is not None


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
    CalibrationRun(Observer(store), run_id="run-x").run()
    ds = store.load("run-x")
    [bundle] = [graph for graph in ds.graphs() if (graph.identifier, RDF.type, PROV.Bundle) in graph]
    node = URIRef(RUN_BASE + "run-x")
    for predicate, value in removed:
        bundle.remove((node, predicate, value))
    for predicate, value in added:
        bundle.add((node, predicate, value))
    assert not validate(ds, shacl_graph=SHAPES, inference="rdfs")[0]


def test_a_run_whose_main_raises_fails_with_its_trace(store):
    observer = Observer(store)
    FailingRun(observer, run_id="run-f").run()
    assert observer.lifecycle("run-f") == (State.COMPLETE, Verdict.FAILED)
    ds = store.load("run-f")
    ok, _, report = validate(ds, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    ds.default_union = True
    assert "ValueError: no gripper attached" in str(ds.value(URIRef(RUN_BASE + "run-f"), REC["fail-trace"]))


@pytest.mark.parametrize(("cls", "interrupt"), [(InterruptedRun, KeyboardInterrupt), (ExitingRun, SystemExit)])
def test_an_interrupted_run_ends_as_error_and_the_interrupt_goes_on(store, cls, interrupt):
    observer = Observer(store)
    with pytest.raises(interrupt):
        cls(observer, run_id="run-i").run()
    assert observer.lifecycle("run-i") == (State.COMPLETE, Verdict.ERROR)
    ok, _, report = validate(store.load("run-i"), shacl_graph=SHAPES, inference="rdfs")
    assert ok, report


def test_only_a_queued_run_or_one_in_progress_is_cancelled(store):
    observer = Observer(store)
    queued = CalibrationRun(observer, run_id="run-q")
    queued.queue()
    # A scheduler holding only the id cancels a run it did not create, and the store decides.
    Run(Observer(store), run_id="run-q").cancel()
    assert (queued.state, queued.verdict) == (State.CANCELED, Verdict.UNAVAILABLE)
    with pytest.raises(RuntimeError, match="is canceled"):
        queued.run()
    ds = store.load("run-q")
    ok, _, report = validate(ds, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    ds.default_union = True
    # It never started, so it has neither a beginning nor an end.
    assert ds.value(URIRef(RUN_BASE + "run-q"), TIME.hasBeginning) is None
    assert ds.value(URIRef(RUN_BASE + "run-q"), TIME.hasEnd) is None

    CalibrationRun(observer, run_id="run-d").run()
    with pytest.raises(RuntimeError, match="only a queued run or one in progress"):
        Run(Observer(store), run_id="run-d").cancel()
    assert observer.lifecycle("run-d") == (State.COMPLETE, Verdict.PASSED)


def test_a_run_in_progress_stops_when_cancelled(store):
    observer = Observer(store)
    run = StoppedRun(observer, run_id="run-s")
    assert run.run() == "stopped"
    assert observer.lifecycle("run-s") == (State.CANCELED, Verdict.UNAVAILABLE)
    ds = store.load("run-s")
    ok, _, report = validate(ds, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    ds.default_union = True
    node = URIRef(RUN_BASE + "run-s")
    assert ds.value(node, TIME.hasBeginning) is not None and ds.value(node, TIME.hasEnd) is not None


def test_a_run_that_fails_while_stopping_keeps_its_verdict(store):
    observer = Observer(store)
    CrashedWhileStoppingRun(observer, run_id="run-c").run()
    assert observer.lifecycle("run-c") == (State.COMPLETE, Verdict.FAILED)
    ok, _, report = validate(store.load("run-c"), shacl_graph=SHAPES, inference="rdfs")
    assert ok, report


def test_a_running_run_beats(store):
    run = SlowRun(Observer(store), run_id="run-h")
    run.beat_interval = 0.01
    run.run()
    ds = store.load("run-h")
    ds.default_union = True
    assert ds.value(URIRef(RUN_BASE + "run-h"), REC["heartbeat-time"]) is not None
    ok, _, report = validate(ds, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report


def test_one_observer_records_many_runs_at_once(store):
    """Every run is addressed by its id, each locked on its own."""
    observer = Observer(store)
    runs = [SlowRun(observer, run_id=f"run-{i}") for i in range(6)]
    for run in runs:
        run.beat_interval = 0.01
    threads = [threading.Thread(target=run.run) for run in runs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert all(observer.lifecycle(run.id) == (State.COMPLETE, Verdict.PASSED) for run in runs)


def test_a_time_without_a_zone_is_refused_and_nothing_kept(store):
    with pytest.raises(ValueError, match="has no time zone"):
        Observer(store).log_queued_run("run-z", datetime(2026, 11, 12, 9, 30))
    assert store.load("run-z") is None


def test_a_run_recorded_under_another_base_is_not_read_as_this_one(store):
    CalibrationRun(Observer(store), run_id="run-o").queue()
    elsewhere = Observer(store, base="https://example.org/runs/")
    with pytest.raises(ValueError, match="recorded under another base"):
        elsewhere.lifecycle("run-o")
    with pytest.raises(ValueError, match="recorded under another base"):
        elsewhere.log_cancelled_run("run-o")


@pytest.mark.parametrize("run_id", ["../escape", "/tmp/escape", "nested/run", "a run", "läuft", "", ".", ".."])
def test_a_run_id_is_unreserved_characters_only(store, run_id):
    with pytest.raises(ValueError, match="is not a run id"):
        Observer(store).log_queued_run(run_id, datetime.now().astimezone())


@pytest.mark.parametrize("run_id", ["../escape", "/tmp/escape", "nested/run"])
def test_a_file_store_reads_no_file_outside_its_directory(store, run_id):
    with pytest.raises(ValueError, match="names a file outside"):
        store.load(run_id)
