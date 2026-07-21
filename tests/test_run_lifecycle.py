# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)

import platform
import socket

import pytest
from rdflib import Graph, Namespace, RDF
from rdflib.namespace import PROV

from rec.observers import FileObserver
from rec.run import Run, RunStatus

REC = Namespace("https://secorolab.github.io/metamodels/rec#")
QUDT = Namespace("http://qudt.org/schema/qudt/")


class InterruptedRun(Run):
    def main(self):
        raise KeyboardInterrupt


class FailingRun(Run):
    def main(self):
        raise ValueError("no gripper attached")


def test_queued_run_cancels(tmp_path):
    path = tmp_path / "rec.jsonld"
    run = Run(observers=[FileObserver(path)], run_id="run-1")
    run.queue()
    assert run.status is RunStatus.QUEUED
    run.cancel()

    graph = Graph().parse(path, format="json-ld")
    activity = REC["activity/run-1"]
    assert (activity, RDF.type, REC.CancelledRun) in graph
    assert (activity, RDF.type, REC.RunningRun) not in graph
    assert graph.value(activity, PROV.startedAtTime) is None
    assert graph.value(activity, REC["queued-time"]) is not None
    assert run.info()["end_time"] is not None


def test_failed_run_records_its_stacktrace(tmp_path):
    path = tmp_path / "rec.jsonld"
    run = FailingRun(observers=[FileObserver(path)], run_id="run-6")
    run.beat_interval = 0
    run.run()

    graph = Graph().parse(path, format="json-ld")
    activity = REC["activity/run-6"]
    assert run.status is RunStatus.FAILED
    assert (activity, RDF.type, REC.FailedRun) in graph
    trace = str(graph.value(activity, REC["fail-trace"]))
    assert "ValueError: no gripper attached" in trace
    assert "Traceback (most recent call last)" in trace


def test_running_run_interrupts_not_cancels(tmp_path):
    path = tmp_path / "rec.jsonld"
    run = InterruptedRun(observers=[FileObserver(path)], run_id="run-2")
    run.beat_interval = 0
    run.run()

    graph = Graph().parse(path, format="json-ld")
    activity = REC["activity/run-2"]
    assert run.status is RunStatus.INTERRUPTED
    assert (activity, RDF.type, REC.InterruptedRun) in graph
    assert graph.value(activity, PROV.startedAtTime) is not None

    with pytest.raises(RuntimeError):
        run.cancel()


def test_stepless_scalars_do_not_overwrite(tmp_path):
    path = tmp_path / "rec.jsonld"
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
    frames = next(metric for metric in graph.objects(None, REC.metrics) if "frames" in str(metric))
    assert graph.value(frames, PROV.generatedAtTime) is not None


def points(graph, metric_name):
    """Return the ``(step, value)`` pairs recorded for ``metric_name``."""
    return {
        (int(graph.value(metric, REC.step)), float(graph.value(metric, QUDT.value)))
        for metric in graph.objects(None, REC.metrics)
        if str(graph.value(metric, REC.label)) == metric_name
    }


def test_host_info_is_collected_at_start(tmp_path):
    path = tmp_path / "rec.jsonld"
    run = Run(observers=[FileObserver(path)], run_id="run-5")
    run._emit_started()

    graph = Graph().parse(path, format="json-ld")
    host = graph.value(REC["activity/run-5"], REC["host-info"])
    assert (host, RDF.type, REC.Host) in graph
    assert str(graph.value(host, REC.hostname)) == socket.gethostname()
    assert graph.value(host, REC.os) is not None
    assert str(graph.value(host, REC.runtime)) == platform.python_version()


def test_started_run_records_its_trigger_and_starter(tmp_path):
    path = tmp_path / "rec.jsonld"
    run = Run(observers=[FileObserver(path)], run_id="run-7")
    run._emit_started(trigger="rec:entity/schedule", starter="rec:activity/scheduler")

    graph = Graph().parse(path, format="json-ld")
    activity = REC["activity/run-7"]
    trigger = REC["entity/schedule"]
    assert (activity, PROV.wasStartedBy, trigger) in graph
    start = graph.value(activity, PROV.qualifiedStart)
    assert (start, RDF.type, PROV.Start) in graph
    assert graph.value(start, PROV.entity) == trigger
    assert graph.value(start, PROV.hadActivity) == REC["activity/scheduler"]
    assert graph.value(start, PROV.atTime) == graph.value(activity, PROV.startedAtTime)


def test_started_run_without_trigger_is_unqualified(tmp_path):
    path = tmp_path / "rec.jsonld"
    run = Run(observers=[FileObserver(path)], run_id="run-8")
    run._emit_started()

    graph = Graph().parse(path, format="json-ld")
    assert graph.value(REC["activity/run-8"], PROV.qualifiedStart) is None


def test_cancelled_run_cannot_start(tmp_path):
    run = Run(observers=[FileObserver(tmp_path / "rec.jsonld")], run_id="run-3")
    run.queue()
    run.cancel()
    with pytest.raises(RuntimeError):
        run.run()
