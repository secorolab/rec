import os
import threading
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyshacl import validate
from rdf_utils.models.prov import _path_from_file_url, resolve_location
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import PROV, RDF, RDFS, SDO

from rec import State, Verdict, provenance
from rec.observer import Observer
from rec.provenance import PROV_EXT, REC
from rec.run import Run
from rec.stores.file_store import FileStore

METAMODELS = Path(os.getenv("REC_METAMODELS_DIR", Path(__file__).resolve().parents[2] / "metamodels"))
SHAPES = Graph()
for name in ("prov.shacl.ttl", "prov-extension.shacl.ttl", "rec/rec.shacl.ttl"):
    SHAPES.parse(METAMODELS / name, format="turtle")


class CalibrationRun(Run):
    def main(self):
        self.add_agent("https://example.org/agent/calibrator", "SoftwareAgent", name="calibrator")
        self.log_sources([{"path": "model.ld.json", "sha256": "deadbeef", "size_bytes": 4}])
        self.log_repositories([{"name": "controller", "url": "https://example.org/lab/controller", "commit": "0123abcd"}])
        self.log_dependencies([{"name": "rdflib", "version": "7.7.0"}])
        # One file used by two activities whose IRIs end in the same segment.
        self.add_resource("config/robot.yaml", usage_activity="https://example.org/calibration/1")
        self.add_resource("config/robot.yaml", usage_activity="https://example.org/verification/1")
        # A path with spaces and '%', an IRI, and a path relative to where it is logged.
        self.add_artefact("/tmp/rec data/50% done.json")
        self.add_artefact("https://example.org/data/scan.bag")
        self.add_artefact("results/calibration.json", sha256="deadbeef", size_bytes=4)
        self.log_scalar("position-error", 0.5)
        self.log_scalar("position-error", 0.3)
        return "calibrated"


class FailingRun(Run):
    def main(self):
        raise ValueError("no gripper attached")


class InterruptedRun(Run):
    def main(self):
        raise KeyboardInterrupt


@pytest.fixture(params=["json", "rdf"])
def store(request, tmp_path):
    return FileStore(tmp_path / request.param, fmt=request.param)


def test_a_completed_run_is_an_execution_of_its_program(store, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    run = CalibrationRun(observers=[Observer(store)], run_id="run-1")
    run.beat_interval = 0
    assert run.run() == "calibrated"

    g = provenance.graph(store.load("run-1"))
    ok, _, report = validate(g, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    node = URIRef(provenance.RUN_BASE + "run-1")
    assert (node, RDF.type, PROV_EXT.Execution) in g
    plan = g.value(g.value(node, PROV.qualifiedAssociation), PROV.hadPlan)
    assert _path_from_file_url(str(resolve_location(g, plan))) == Path(__file__).resolve()
    assert {str(g.value(agent, SDO.name)) for agent in g.objects(node, PROV.wasAssociatedWith)} == {"rec", "calibrator"}
    # A dependency is software the run used, not an agent that ran it.
    [dependency] = g.subjects(RDFS.label, Literal("rdflib"))
    assert (node, PROV.used, dependency) in g and (dependency, RDF.type, PROV.SoftwareAgent) not in g
    artefacts = {str(resolve_location(g, entity)) for entity in g.subjects(PROV.qualifiedGeneration, None)}
    assert artefacts == {
        "file:///tmp/rec%20data/50%25%20done.json",
        "https://example.org/data/scan.bag",
        (tmp_path.resolve() / "results/calibration.json").as_uri(),
    }
    assert len(list(g.subjects(RDF.type, PROV.Usage))) == 2
    assert len(list(g.subjects(RDF.type, REC.Metric))) == 2


def test_a_record_is_the_same_in_every_store(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    as_json, as_rdf = FileStore(tmp_path / "json", fmt="json"), FileStore(tmp_path / "rdf", fmt="rdf")
    run = CalibrationRun(observers=[Observer(as_json), Observer(as_rdf)], run_id="run-1")
    run.beat_interval = 0
    run.run()
    # The run logged its lists in the order a graph reads them back.
    assert as_rdf.load("run-1") == as_json.load("run-1")


def test_a_queued_run_has_not_executed_yet(store):
    CalibrationRun(observers=[Observer(store)], run_id="run-q").queue()
    g = provenance.graph(store.load("run-q"))
    ok, _, report = validate(g, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    node = URIRef(provenance.RUN_BASE + "run-q")
    assert (node, RDF.type, PROV.Activity) in g and (node, RDF.type, PROV_EXT.Execution) not in g
    assert g.value(node, PROV.used) is None and g.value(node, REC["queued-time"]) is not None


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
    ok, _, report = validate(provenance.graph(record), shacl_graph=SHAPES, inference="rdfs")
    assert ok, report


def test_a_post_run_hook_that_raises_leaves_the_run_passed(store):
    def broken_hook():
        raise RuntimeError("hook")

    run = CalibrationRun(observers=[Observer(store)], run_id="run-h", post_run_hooks=[broken_hook])
    run.beat_interval = 0
    with pytest.raises(RuntimeError, match="hook"):
        run.run()
    assert (store.load("run-h").state, store.load("run-h").verdict) == (State.COMPLETE, Verdict.PASSED)


def test_only_a_run_that_has_not_started_is_cancelled(store):
    queued = CalibrationRun(observers=[Observer(store)], run_id="run-q")
    queued.queue()
    # A scheduler holding only the store cancels a run it did not create, and the store decides.
    Observer(store).log_cancelled_run("run-q", datetime.now(UTC))
    with pytest.raises(RuntimeError, match="is canceled"):
        queued.run()
    assert (store.load("run-q").state, store.load("run-q").verdict) == (State.CANCELED, Verdict.UNAVAILABLE)

    done = CalibrationRun(observers=[Observer(store)], run_id="run-d")
    done.beat_interval = 0
    done.run()
    with pytest.raises(RuntimeError, match="has not started"):
        Observer(store).log_cancelled_run("run-d", datetime.now(UTC))
    assert store.load("run-d").verdict is Verdict.PASSED
    assert store.run_ids(State.CANCELED) == ["run-q"]


def test_logging_a_step_again_replaces_its_value(store):
    observer = Observer(store)
    observer.log_scalar("run-m", "error", 0.5, step=1)
    observer.log_scalar("run-m", "error", 0.1, step=1)
    assert [(metric.step, metric.value) for metric in store.load("run-m").metrics] == [(1, 0.1)]


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
    assert all(len(store.load(run.id).metrics) == 2 for run in runs)


@pytest.mark.parametrize("run_id", ["../escape", "/tmp/escape", "nested/run", "", ".", ".."])
def test_a_run_id_names_a_file_inside_the_directory(store, run_id):
    with pytest.raises(ValueError, match="not a file name"):
        store.path(run_id)
