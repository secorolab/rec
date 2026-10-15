import os
import threading
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyshacl import validate
from rdf_utils.models.prov import _path_from_file_url, resolve_location
from rdflib import Graph, Literal, URIRef
from rdflib.compare import isomorphic
from rdflib.namespace import DCTERMS, PROV, RDF, RDFS, SDO

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
        self.add_agent("https://example.org/agent/calibrator", "SoftwareAgent", name="calibrator")
        self.log_repositories([{"name": "controller", "url": "https://example.org/lab/controller", "commit": "0123abcd"}])
        self.log_dependencies([{"name": "rdflib", "version": "7.7.0"}])
        calibration, verification = "https://example.org/calibration/1", "https://example.org/verification/1"
        now = datetime.now(UTC)
        self.add_activity(calibration, "Execution", "https://example.org/agent/calibrator", now, now)
        self.add_activity(verification, "Assurance", "https://example.org/agent/calibrator", now, now)
        # One file used by two activities whose IRIs end in the same segment.
        self.add_resource("config/robot.yaml", usage_activity=calibration)
        self.add_resource("config/robot.yaml", usage_activity=verification)
        # A path with spaces and '%', an IRI, and a path relative to where it is logged.
        self.add_artefact("/tmp/rec data/50% done.json")
        self.add_artefact("https://example.org/data/scan.bag")
        self.add_artefact("results/calibration.json", gen_activity=verification, sha256="deadbeef", size_bytes=4)
        self.log_scalar("position-error", 0.5)
        self.log_scalar("position-error", 0.3)
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
    from_rdf, from_json = as_rdf.load("run-1"), as_json.load("run-1")
    assert from_rdf is not None and from_json is not None
    assert isomorphic(provenance.graph(from_rdf), provenance.graph(from_json))


def test_a_queued_run_has_not_executed_yet(store):
    CalibrationRun(observers=[Observer(store)], run_id="run-q").queue()
    g = provenance.graph(store.load("run-q"))
    ok, _, report = validate(g, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    node = URIRef(provenance.RUN_BASE + "run-q")
    assert {PROV_EXT.Execution, OSLC_AUTO.AutomationResult} <= set(g.objects(node, RDF.type))
    assert g.value(node, PROV.startedAtTime) is None and g.value(node, PROV.wasAssociatedWith) is None
    assert g.value(node, PROV.used) is None and g.value(node, REC["queued-time"]) is not None


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
def test_the_shapes_refuse_a_run_that_contradicts_its_state(store, tmp_path, monkeypatch, removed, added):
    monkeypatch.chdir(tmp_path)
    run = CalibrationRun(observers=[Observer(store)], run_id="run-x")
    run.beat_interval = 0
    run.run()
    g = provenance.graph(store.load("run-x"))
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
    ok, _, report = validate(provenance.graph(record), shacl_graph=SHAPES, inference="rdfs")
    assert ok, report


def test_only_a_queued_run_is_cancelled(store):
    queued = CalibrationRun(observers=[Observer(store)], run_id="run-q")
    queued.queue()
    # A scheduler holding only the id cancels a run it did not create, and the store decides.
    Run(observers=[Observer(store)], run_id="run-q").cancel()
    assert queued.state is State.CANCELED
    with pytest.raises(RuntimeError, match="is canceled"):
        queued.run()
    assert (store.load("run-q").state, store.load("run-q").verdict) == (State.CANCELED, Verdict.UNAVAILABLE)
    g = provenance.graph(store.load("run-q"))
    ok, _, report = validate(g, shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    assert g.value(URIRef(provenance.RUN_BASE + "run-q"), PROV.startedAtTime) is None

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
    assert (record.state, record.verdict, record.result) == (State.CANCELED, Verdict.UNAVAILABLE, "stopped")
    assert record.start_time is not None and record.end_time is not None
    ok, _, report = validate(provenance.graph(record), shacl_graph=SHAPES, inference="rdfs")
    assert ok, report
    with pytest.raises(RuntimeError, match="only a queued run or one in progress"):
        run.cancel()


def test_a_step_is_part_of_its_run(store, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    run = CalibrationRun(observers=[Observer(store)], run_id="run-a")
    run.beat_interval = 0
    run.run()
    record = store.load("run-a")
    assert {(activity.id, activity.kind) for activity in record.activities} == {
        ("https://example.org/calibration/1", "Execution"),
        ("https://example.org/verification/1", "Assurance"),
    }
    g = provenance.graph(record)
    node = URIRef(provenance.RUN_BASE + "run-a")
    verification = URIRef("https://example.org/verification/1")
    assert set(g.subjects(DCTERMS.isPartOf, node)) == {URIRef("https://example.org/calibration/1"), verification}
    assert (verification, RDF.type, PROV_EXT.Assurance) in g
    assert g.value(verification, PROV.wasAssociatedWith) == URIRef("https://example.org/agent/calibrator")
    assert (g.value(verification, PROV.used), RDF.type, PROV.Entity) in g
    assert (None, PROV.wasGeneratedBy, verification) in g


@pytest.mark.parametrize(
    ("method", "args", "message"),
    [
        ("add_activity", ("https://example.org/step", "Calibration", "https://example.org/agent", datetime.now(UTC)), "not a prov-extension activity class"),
        ("add_resource", ("config.yaml", "https://example.org/missing"), "has no activity"),
        ("add_artefact", ("out.csv", "https://example.org/missing"), "has no activity"),
    ],
    ids=["unknown kind", "resource of an unknown step", "artefact of an unknown step"],
)
def test_a_step_must_be_known_to_its_run(store, method, args, message):
    with pytest.raises(ValueError, match=message):
        getattr(Observer(store), method)("run-u", *args)
    assert store.load("run-u") is None


def test_logging_a_step_again_replaces_its_value(store):
    observer = Observer(store)
    observer.log_scalar("run-m", "error", 0.5, step=1)
    observer.log_scalar("run-m", "error", 0.1, step=1)
    assert [(metric.step, metric.value) for metric in store.load("run-m").metrics] == [(1, 0.1)]


@pytest.mark.parametrize(
    ("method", "args"),
    [
        ("log_scalar", ("error", float("nan"))),
        ("log_scalar", ("error", float("inf"))),
        ("log_dependencies", ([{"name": "rdflib"}, {"name": "rdflib"}],)),
    ],
    ids=["nan metric", "infinite metric", "logged twice"],
)
def test_a_value_every_store_cannot_keep_is_refused_and_nothing_kept(store, method, args):
    with pytest.raises(ValueError):
        getattr(Observer(store), method)("run-v", *args)
    assert store.load("run-v") is None


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


@pytest.mark.parametrize("run_id", ["../escape", "/tmp/escape", "nested/run", "a run", "läuft", "", ".", ".."])
def test_a_run_id_is_unreserved_characters_only(store, run_id):
    with pytest.raises(ValueError, match="is not a run id"):
        RunRecord(run_id)
    with pytest.raises(ValueError, match="is not a run id"):
        store.path(run_id)
