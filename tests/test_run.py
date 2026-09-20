import json
import os
import threading
from pathlib import Path

import pytest
from pyshacl import validate
from rdflib import Graph, Namespace, URIRef
from rdflib.namespace import PROV, RDF

from rec import State, Verdict, jsonld
from rec.observers.file_observer import FileObserver
from rec.run import Run

OSLC_AUTO = Namespace("http://open-services.net/ns/auto#")
PROV_EXT = Namespace("https://secorolab.github.io/metamodels/prov#")
REC = Namespace("https://secorolab.github.io/metamodels/rec#")
METAMODELS = Path(os.getenv("REC_METAMODELS_DIR", Path(__file__).resolve().parents[2] / "metamodels"))


class CalibrationRun(Run):
    def main(self):
        self.add_agent("https://example.org/agent/calibrator", "SoftwareAgent", name="calibrator")
        self.log_sources([{"path": "model.ld.json", "sha256": "deadbeef", "size_bytes": 4}])
        self.log_repositories([{"name": "controller", "url": "git@example.org:lab/controller.git", "commit": "0123abcd"}])
        # The package built from the repository shares its name: two agents, not one.
        self.log_dependencies([{"name": "rdflib", "version": "7.7.0"}, {"name": "controller", "version": "1.2"}])
        # One file used by two activities, and two files whose names differ by one character.
        self.add_resource("config/robot.yaml", usage_activity="https://example.org/activity/calibration")
        self.add_resource("config/robot.yaml", usage_activity="https://example.org/activity/verification")
        self.add_artefact("results/calibration.json", sha256="deadbeef", size_bytes=4)
        self.add_artefact("results/a b.json")
        self.add_artefact("results/a_b.json")
        self.log_scalar("position-error", 0.5)
        self.log_scalar("position-error", 0.3)
        self.log_scalar("frames", 10, step=7)
        return "calibrated"


class FailingRun(Run):
    def main(self):
        raise ValueError("no gripper attached")


class InterruptedRun(Run):
    def main(self):
        raise KeyboardInterrupt


class CountingObserver(FileObserver):
    closed = 0

    def close(self):
        self.closed += 1


def graph(observer, run_id):
    """The run's document as RDF, its metamodel contexts read from the checkout."""
    doc = observer.document(run_id)
    doc["@context"] = [
        (METAMODELS / entry.removeprefix(jsonld.METAMODELS)).resolve().as_uri() if isinstance(entry, str) else entry
        for entry in doc["@context"]
    ]
    return Graph().parse(data=json.dumps(doc), format="json-ld")


def lifecycle(g, run):
    return g.value(run, OSLC_AUTO.state), g.value(run, OSLC_AUTO.verdict)


def test_a_completed_run_conforms_to_the_metamodels(tmp_path):
    observer = FileObserver(tmp_path)
    run = CalibrationRun(observers=[observer], run_id="run-1")
    run.beat_interval = 0
    assert run.run() == "calibrated"

    g = graph(observer, "run-1")
    shapes = Graph()
    for name in ("prov.shacl.ttl", "prov-extension.shacl.ttl", "rec/rec.shacl.ttl"):
        shapes.parse(METAMODELS / name, format="turtle")
    conforms, _, report = validate(g, shacl_graph=shapes, inference="rdfs")
    assert conforms, report

    node = URIRef(observer.run_iri("run-1"))
    assert (node, RDF.type, PROV_EXT.Execution) in g
    assert lifecycle(g, node) == (OSLC_AUTO.complete, OSLC_AUTO.passed)
    assert g.value(node, PROV.startedAtTime) is not None and g.value(node, PROV.endedAtTime) is not None
    assert len(list(g.subjects(RDF.type, REC.Metric))) == 3
    assert (URIRef("https://example.org/activity/calibration"), PROV.used, None) in g
    assert len(list(g.subjects(RDF.type, PROV.SoftwareAgent))) == 4
    assert g.value(node, REC.result).toPython() == "calibrated"


def test_the_document_round_trips_through_its_record(tmp_path):
    observer = FileObserver(tmp_path)
    run = CalibrationRun(observers=[observer], run_id="run-2")
    run.beat_interval = 0
    run.run()
    record = observer.get_run("run-2")
    assert (record["state"], record["verdict"]) == (State.COMPLETE, Verdict.PASSED)
    assert record["run_info"]["result"] == "calibrated"
    assert [m["step"] for m in record["metrics"]] == [0, 1, 7]
    assert [row["name"] for row in record["repositories"]] == ["controller"]
    assert [row["name"] for row in record["dependencies"]] == ["rdflib", "controller"]
    assert [row["activity"].rsplit("/", 1)[-1] for row in record["resources"]] == ["calibration", "verification"]
    assert [row["path"] for row in record["artefacts"]] == ["results/calibration.json", "results/a b.json", "results/a_b.json"]
    assert jsonld.record(observer.document("run-2")) == record
    # Logging a step again replaces its value: one metric node per name and step.
    observer.log_scalar("run-2", "frames", 11, step=7)
    assert [(m["step"], m["value"]) for m in observer.get_run("run-2")["metrics"]] == [(0, 0.5), (1, 0.3), (7, 11)]


@pytest.mark.parametrize(
    ("cls", "verdict", "pair", "trace"),
    [
        (FailingRun, Verdict.FAILED, (OSLC_AUTO.complete, OSLC_AUTO.failed), "ValueError: no gripper attached"),
        (InterruptedRun, Verdict.ERROR, (OSLC_AUTO.complete, OSLC_AUTO.error), "KeyboardInterrupt"),
    ],
)
def test_a_run_that_stops_early_records_how(tmp_path, cls, verdict, pair, trace):
    observer = FileObserver(tmp_path)
    run = cls(observers=[observer], run_id="run-3")
    run.beat_interval = 0
    run.run()
    assert (run.state, run.verdict) == (State.COMPLETE, verdict)
    g = graph(observer, "run-3")
    node = URIRef(observer.run_iri("run-3"))
    assert lifecycle(g, node) == pair
    assert trace in str(g.value(node, REC["fail-trace"]))


def test_one_observer_records_many_runs_at_once(tmp_path):
    """The store is shared; every run is addressed by its id, and none closes it."""
    observer = CountingObserver(tmp_path)
    runs = [CalibrationRun(observers=[observer], run_id=f"run-{i}") for i in range(6)]
    for run in runs:
        run.beat_interval = 0.01
    threads = [threading.Thread(target=run.run) for run in runs]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    for run in runs:
        assert (run.state, run.verdict) == (State.COMPLETE, Verdict.PASSED)
        assert observer.get_run(run.id)["state"] is State.COMPLETE
        assert len(observer.get_run(run.id)["metrics"]) == 3
    assert observer.closed == 0


def test_a_queued_run_is_cancelled_by_id_from_the_store(tmp_path):
    """A scheduler holding only the observer cancels a queued run it did not create."""
    observer = FileObserver(tmp_path)
    run = CalibrationRun(observers=[observer], run_id="run-q")
    run.queue()
    assert observer.query_active_run() is None
    assert observer.get_run("run-q")["state"] is State.QUEUED

    from datetime import UTC, datetime

    FileObserver(tmp_path).log_cancelled_run("run-q", datetime.now(UTC))

    g = graph(observer, "run-q")
    node = URIRef(observer.run_iri("run-q"))
    assert lifecycle(g, node) == (OSLC_AUTO.canceled, OSLC_AUTO.unavailable)
    assert g.value(node, PROV.startedAtTime) is None
    assert g.value(node, REC["queued-time"]) is not None

    # The Run object never saw the cancellation; the store did, and that is what decides.
    with pytest.raises(RuntimeError, match="cancelled"):
        run.run()
    assert run.state is State.QUEUED
    assert observer.get_run("run-q")["state"] is State.CANCELED


def test_a_run_mints_one_id_for_every_observer(tmp_path):
    first, second = FileObserver(tmp_path / "a"), FileObserver(tmp_path / "b")
    run = CalibrationRun(observers=[first, second])
    run.beat_interval = 0
    run.run()
    assert first.path(run.id).exists() and second.path(run.id).exists()
    assert first.document(run.id)["@graph"][0]["@id"] == second.document(run.id)["@graph"][0]["@id"]


def test_the_run_iri_base_is_the_callers(tmp_path):
    observer = FileObserver(tmp_path, base="https://lab.example.org/runs/")
    run = CalibrationRun(observers=[observer], run_id="run-b")
    run.beat_interval = 0
    run.run()
    assert observer.document("run-b")["@graph"][0]["@id"] == "https://lab.example.org/runs/run-b"


def test_a_verdict_comes_only_with_completion():
    with pytest.raises(ValueError, match="in-progress"):
        jsonld.document("https://example.org/run/x", {"state": State.IN_PROGRESS, "verdict": Verdict.PASSED})


@pytest.mark.parametrize("run_id", ["../escape", "/tmp/escape", "nested/run", "", ".."])
def test_a_run_id_stays_inside_the_directory(tmp_path, run_id):
    with pytest.raises(ValueError):
        FileObserver(tmp_path).path(run_id)
