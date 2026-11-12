import os
import threading
from pathlib import Path

import pytest
from pyshacl import validate
from rdflib import Graph, URIRef
from rdflib.namespace import PROV, RDF, TIME

from rec import State, Verdict
from rec.provenance import OSLC_AUTO, PROV_EXT, REC, RUN_BASE
from rec.run import Run
from rec.stores.file_store import FileStore

METAMODELS = Path(os.getenv("REC_METAMODELS_DIR", Path(__file__).resolve().parents[2] / "metamodels"))
SHAPES = Graph()
for name in ("prov.shacl.ttl", "prov-extension.shacl.ttl", "rec/rec.shacl.ttl"):
    SHAPES.parse(METAMODELS / name, format="turtle")


def calibrate(run):
    return "calibrated"


def drop_gripper(run):
    raise ValueError("no gripper attached")


def interrupt(run):
    raise KeyboardInterrupt


def exit_process(run):
    raise SystemExit(3)


def stop_when_asked(run):
    # A scheduler holding only the id asks the run to stop while it is in progress.
    Run(run.store, run.id).cancel()
    return "stopped" if run.lifecycle()[0] is State.CANCELING else "finished"


def jam_while_stopping(run):
    Run(run.store, run.id).cancel()
    raise ValueError("gripper jammed while releasing")


def wait_a_little(run):
    threading.Event().wait(0.05)


def end_then_interrupt(run):
    # Ended by someone else before the work returns, so the run's own end is refused.
    Run(run.store, run.id).end(Verdict.PASSED)
    raise KeyboardInterrupt


@pytest.fixture
def store(tmp_path):
    return FileStore(tmp_path)


def test_a_completed_run_is_an_execution_by_rec_in_a_bundle_attributed_to_rec(store):
    run = Run(store, "run-1")
    assert run.run(calibrate) == "calibrated"

    ds = store.load("run-1")
    ok, _, report = validate(ds, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    [bundle] = [graph for graph in ds.graphs() if (graph.identifier, RDF.type, PROV.Bundle) in graph]
    node = URIRef(RUN_BASE + "run-1")
    assert (node, RDF.type, PROV_EXT.Execution) in bundle
    assert bundle.value(bundle.identifier, PROV.wasAttributedTo) == run.recorder.purl == bundle.value(node, PROV.wasAssociatedWith)
    assert bundle.value(bundle.value(node, PROV.atLocation), RDF.type) is not None
    assert run.lifecycle() == (State.COMPLETE, Verdict.PASSED)


def test_a_run_keeps_one_bundle_through_its_lifecycle(store):
    run = Run(store, "run-b")
    run.queue()
    queued = {graph.identifier for graph in store.load("run-b").graphs() if (graph.identifier, RDF.type, PROV.Bundle) in graph}
    run.run(calibrate)
    ended = {graph.identifier for graph in store.load("run-b").graphs() if (graph.identifier, RDF.type, PROV.Bundle) in graph}
    assert len(queued) == 1 and ended == queued


def test_a_step_changes_only_its_own_triples(store):
    """A triple another tool put in the run's bundle survives rec's later steps."""
    Run(store, "run-k").queue()
    with store.edit("run-k") as ds:
        [bundle] = [graph for graph in ds.graphs() if (graph.identifier, RDF.type, PROV.Bundle) in graph]
        bundle.add((URIRef(RUN_BASE + "run-k"), PROV.wasInfluencedBy, URIRef("https://example.org/schedule/1")))
    Run(store, "run-k").run(calibrate)
    ds = store.load("run-k")
    ds.default_union = True
    assert (URIRef(RUN_BASE + "run-k"), PROV.wasInfluencedBy, URIRef("https://example.org/schedule/1")) in ds


def test_a_queued_run_has_not_executed_yet(store):
    Run(store, "run-q").queue()
    ds = store.load("run-q")
    ok, _, report = validate(ds, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    ds.default_union = True
    node = URIRef(RUN_BASE + "run-q")
    assert {PROV_EXT.Execution, OSLC_AUTO.AutomationResult} <= set(ds.objects(node, RDF.type))
    assert ds.value(node, TIME.hasBeginning) is None and ds.value(node, PROV.wasAssociatedWith) is None
    assert ds.value(node, REC["queued-time"]) is not None


def test_a_run_whose_work_raises_fails_with_its_trace(store):
    run = Run(store, "run-f")
    assert run.run(drop_gripper) is None
    assert run.lifecycle() == (State.COMPLETE, Verdict.FAILED)
    ds = store.load("run-f")
    ok, _, report = validate(ds, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    ds.default_union = True
    assert "ValueError: no gripper attached" in str(ds.value(URIRef(RUN_BASE + "run-f"), REC["fail-trace"]))


@pytest.mark.parametrize(("work", "raised"), [(interrupt, KeyboardInterrupt), (exit_process, SystemExit)])
def test_an_interrupted_run_ends_as_error_and_the_interrupt_goes_on(store, work, raised):
    run = Run(store, "run-i")
    with pytest.raises(raised):
        run.run(work)
    assert run.lifecycle() == (State.COMPLETE, Verdict.ERROR)
    ok, _, report = validate(store.load("run-i"), shacl_graph=SHAPES, inference="rdfs")
    assert ok, report


def test_an_interrupt_goes_on_when_the_end_cannot_be_recorded(store):
    with pytest.raises(KeyboardInterrupt):
        Run(store, "run-e").run(end_then_interrupt)


@pytest.mark.parametrize(
    ("verdict", "trace", "message"),
    [(Verdict.UNAVAILABLE, None, "without a verdict"), (Verdict.PASSED, "Traceback", "passed with a fail trace")],
    ids=["no verdict", "passed with a trace"],
)
def test_a_contradictory_end_is_refused_and_the_run_kept_running(store, verdict, trace, message):
    run = Run(store, "run-v")
    run.start()
    with pytest.raises(ValueError, match=message):
        run.end(verdict, trace)
    assert run.lifecycle() == (State.IN_PROGRESS, Verdict.UNAVAILABLE)


def test_only_a_queued_run_or_one_in_progress_is_cancelled(store):
    queued = Run(store, "run-q")
    queued.queue()
    # A scheduler holding only the id cancels a run it did not create, and the store decides.
    Run(store, "run-q").cancel()
    assert queued.lifecycle() == (State.CANCELED, Verdict.UNAVAILABLE)
    with pytest.raises(RuntimeError, match="is canceled"):
        queued.run(calibrate)
    ds = store.load("run-q")
    ok, _, report = validate(ds, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    ds.default_union = True
    # It never started, so it has neither a beginning nor an end.
    assert ds.value(URIRef(RUN_BASE + "run-q"), TIME.hasBeginning) is None
    assert ds.value(URIRef(RUN_BASE + "run-q"), TIME.hasEnd) is None

    done = Run(store, "run-d")
    done.run(calibrate)
    with pytest.raises(RuntimeError, match="only a queued run or one in progress"):
        Run(store, "run-d").cancel()
    assert done.lifecycle() == (State.COMPLETE, Verdict.PASSED)


def test_a_run_in_progress_stops_when_cancelled(store):
    run = Run(store, "run-s")
    assert run.run(stop_when_asked) == "stopped"
    assert run.lifecycle() == (State.CANCELED, Verdict.UNAVAILABLE)
    ds = store.load("run-s")
    ok, _, report = validate(ds, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    ds.default_union = True
    node = URIRef(RUN_BASE + "run-s")
    assert ds.value(node, TIME.hasBeginning) is not None and ds.value(node, TIME.hasEnd) is not None


def test_a_run_that_fails_while_stopping_keeps_its_verdict(store):
    run = Run(store, "run-c")
    run.run(jam_while_stopping)
    assert run.lifecycle() == (State.COMPLETE, Verdict.FAILED)
    ok, _, report = validate(store.load("run-c"), shacl_graph=SHAPES, inference="rdfs")
    assert ok, report


def test_a_running_run_beats(store):
    run = Run(store, "run-h")
    run.beat_interval = 0.01
    run.run(wait_a_little)
    ds = store.load("run-h")
    ds.default_union = True
    assert ds.value(URIRef(RUN_BASE + "run-h"), REC["heartbeat-time"]) is not None
    ok, _, report = validate(ds, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report


@pytest.mark.parametrize("run_id", ["../escape", "/tmp/escape", "nested/run", "a run", "läuft", ".", ".."])
def test_a_run_id_is_unreserved_characters_only(store, run_id):
    with pytest.raises(ValueError, match="is not a run id"):
        Run(store, run_id)


@pytest.mark.parametrize("run_id", ["../escape", "/tmp/escape", "nested/run"])
def test_a_file_store_reads_no_file_outside_its_directory(store, run_id):
    with pytest.raises(ValueError, match="names a file outside"):
        store.load(run_id)
