# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)

import platform
import socket
import threading

import pytest
from rdflib import Graph, Namespace, RDF, URIRef
from rdflib.namespace import PROV, RDFS, SDO

from rec.observers import FileObserver
from rec.observers.graph_observer import OSLC_AUTO, PROV_EXT, REC_RUN
from rec.run import Run, RunStatus

REC = Namespace("https://secorolab.github.io/metamodels/rec#")
QUDT = Namespace("http://qudt.org/schema/qudt/")


class InterruptedRun(Run):
    def main(self):
        raise KeyboardInterrupt


class FailingRun(Run):
    def main(self):
        raise ValueError("no gripper attached")


def lifecycle(graph, run):
    return graph.value(run, OSLC_AUTO.state), graph.value(run, OSLC_AUTO.verdict)


def test_queued_run_cancels(tmp_path):
    path = tmp_path / "rec.ld.json"
    run = Run(observers=[FileObserver(path)], run_id="run-1")
    run.queue()
    assert run.status is RunStatus.QUEUED
    run.cancel()

    graph = Graph().parse(path, format="json-ld")
    activity = REC_RUN["run-1"]
    assert (activity, RDF.type, PROV_EXT.Execution) in graph
    assert lifecycle(graph, activity) == (OSLC_AUTO.canceled, OSLC_AUTO.unavailable)
    assert graph.value(activity, PROV.startedAtTime) is None
    assert graph.value(activity, REC["queued-time"]) is not None
    assert run.end_time is not None


def test_failed_run_records_its_stacktrace(tmp_path):
    path = tmp_path / "rec.ld.json"
    run = FailingRun(observers=[FileObserver(path)], run_id="run-6")
    run.beat_interval = 0
    run.run()

    graph = Graph().parse(path, format="json-ld")
    activity = REC_RUN["run-6"]
    assert run.status is RunStatus.FAILED
    assert lifecycle(graph, activity) == (OSLC_AUTO.complete, OSLC_AUTO.failed)
    trace = str(graph.value(activity, REC["fail-trace"]))
    assert "ValueError: no gripper attached" in trace
    assert "Traceback (most recent call last)" in trace


def test_running_run_interrupts_not_cancels(tmp_path):
    path = tmp_path / "rec.ld.json"
    run = InterruptedRun(observers=[FileObserver(path)], run_id="run-2")
    run.beat_interval = 0
    run.run()

    graph = Graph().parse(path, format="json-ld")
    activity = REC_RUN["run-2"]
    assert run.status is RunStatus.INTERRUPTED
    assert lifecycle(graph, activity) == (OSLC_AUTO.complete, OSLC_AUTO.error)
    assert graph.value(activity, PROV.startedAtTime) is not None

    with pytest.raises(RuntimeError):
        run.cancel()


def test_a_heartbeat_keeps_only_its_latest_time(tmp_path):
    path = tmp_path / "rec.ld.json"
    observer = FileObserver(path)
    run = Run(observers=[observer], run_id="run-9")
    run._emit_started()
    run.result = "half"
    run._emit_heartbeat()
    run.result = "done"
    run._emit_heartbeat()

    graph = Graph().parse(path, format="json-ld")
    activity = REC_RUN["run-9"]
    assert len(list(graph.objects(activity, REC["heartbeat-time"]))) == 1
    assert str(graph.value(activity, REC.result)) == "done"


def test_stepless_scalars_do_not_overwrite(tmp_path):
    path = tmp_path / "rec.ld.json"
    run = Run(observers=[FileObserver(path)], run_id="run-4")
    run._emit_started()
    run.log_scalar("loss", 0.5)
    run.log_scalar("loss", 0.3)
    run.log_scalar("frames", 10, step=7)
    # a reopened archive must continue the counter, not restart it
    Run(observers=[FileObserver(path)]).log_scalar("loss", 0.1)

    graph = Graph().parse(path, format="json-ld")
    assert points(graph, "loss") == {(0, 0.5), (1, 0.3), (2, 0.1)}
    assert points(graph, "frames") == {(7, 10)}
    frames = next(metric for metric in graph.subjects(RDF.type, REC.Metric) if "frames" in str(metric))
    assert graph.value(frames, PROV.generatedAtTime) is not None
    assert (frames, PROV.wasGeneratedBy, REC_RUN["run-4"]) in graph


def points(graph, metric_name):
    """Return the ``(step, value)`` pairs recorded for ``metric_name``."""
    return {
        (int(graph.value(metric, REC.step)), float(graph.value(metric, QUDT.value)))
        for metric in graph.subjects(RDF.type, REC.Metric)
        if str(graph.value(metric, RDFS.label)) == metric_name
    }


def test_the_heartbeat_does_not_race_the_run(tmp_path, monkeypatch):
    """The heartbeat thread writes the same graph and file the run is filling."""
    thread_errors = []
    monkeypatch.setattr(threading, "excepthook", lambda args: thread_errors.append(args.exc_value))

    class BusyRun(Run):
        def main(self):
            for step in range(20):
                self.log_scalar("x", step, step=step)
            return "ok"

    path = tmp_path / "rec.ld.json"
    run = BusyRun(observers=[FileObserver(path)], run_id="run-10")
    run.beat_interval = 0.01
    run.run()

    assert thread_errors == []
    assert run.status is RunStatus.COMPLETED
    assert len(points(Graph().parse(path, format="json-ld"), "x")) == 20


def test_host_info_is_collected_at_start(tmp_path):
    path = tmp_path / "rec.ld.json"
    run = Run(observers=[FileObserver(path)], run_id="run-5")
    run._emit_started()

    graph = Graph().parse(path, format="json-ld")
    host = next(
        location
        for location in graph.objects(REC_RUN["run-5"], PROV.atLocation)
        if (location, RDF.type, REC.Host) in graph
    )
    assert str(graph.value(host, SDO.identifier)) == socket.gethostname()
    assert graph.value(host, REC.os) is not None
    assert str(graph.value(host, REC.runtime)) == platform.python_version()


def test_started_run_records_its_trigger_and_starter(tmp_path):
    path = tmp_path / "rec.ld.json"
    run = Run(observers=[FileObserver(path)], run_id="run-7")
    trigger = URIRef("https://example.org/entity/schedule")
    starter = URIRef("https://example.org/activity/scheduler")
    run._emit_started(trigger=trigger, starter=starter)

    graph = Graph().parse(path, format="json-ld")
    activity = REC_RUN["run-7"]
    assert (activity, PROV.wasStartedBy, trigger) in graph
    start = graph.value(activity, PROV.qualifiedStart)
    assert (start, RDF.type, PROV.Start) in graph
    assert graph.value(start, PROV.entity) == trigger
    assert graph.value(start, PROV.hadActivity) == starter
    assert graph.value(start, PROV.atTime) == graph.value(activity, PROV.startedAtTime)


def test_started_run_without_trigger_is_unqualified(tmp_path):
    path = tmp_path / "rec.ld.json"
    run = Run(observers=[FileObserver(path)], run_id="run-8")
    run._emit_started()

    graph = Graph().parse(path, format="json-ld")
    assert graph.value(REC_RUN["run-8"], PROV.qualifiedStart) is None


def test_cancelled_run_cannot_start(tmp_path):
    run = Run(observers=[FileObserver(tmp_path / "rec.ld.json")], run_id="run-3")
    run.queue()
    run.cancel()
    with pytest.raises(RuntimeError):
        run.run()
