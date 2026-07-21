# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Argentina Ortega

import datetime
import logging
import platform
import socket
import threading
import traceback
from typing import Sequence
from uuid import uuid4

from rdflib import Namespace

from rec.observers.base import BaseObserver

REC = Namespace("https://secorolab.github.io/metamodels/rec#")

logger = logging.getLogger(__name__)


class IntervalTimer(threading.Thread):
    @classmethod
    def create(cls, func, interval=10):
        stop_event = threading.Event()
        timer_thread = cls(stop_event, func, interval)
        return stop_event, timer_thread

    def __init__(self, event, func, interval=10.0):
        super().__init__()
        self.stopped = event
        self.func = func
        self.interval = interval

    def run(self):
        while not self.stopped.wait(self.interval):
            self.func()


def _fail_trace(error):
    return "".join(traceback.format_exception(error)) if error is not None else None


def host_info():
    """Return this machine's hostname, operating system, runtime, and CPU."""
    return {
        "hostname": socket.gethostname(),
        "os": platform.platform(),
        "python": platform.python_version(),
        "cpu": platform.processor() or None,
    }


class Run:
    """Coordinate one execution and forward its provenance to observers.

    Args:
        observers: Storage backends that receive every run event.
        run_id: Canonical REC run identifier. A UUID-based ID is generated when omitted.
        pre_run_hooks: Callables run after the run starts.
        post_run_hooks: Callables run after successful completion.
    """

    def __init__(
        self,
        observers: Sequence[BaseObserver] = (),
        run_id: str | None = None,
        pre_run_hooks: list = None,
        post_run_hooks: list = None,
    ):
        self._id = run_id
        self.observers = observers
        file_observer = next((observer for observer in observers if hasattr(observer, "path")), None)
        if file_observer is not None:
            for observer in observers:
                if hasattr(observer, "set_file_source"):
                    observer.set_file_source(file_observer.path)
        self.start_time = None
        self.end_time = None
        self.status = None
        self.result = None
        self.pre_run_hooks = pre_run_hooks or []
        self.post_run_hooks = post_run_hooks or []

        self.beat_interval = 10
        self._heartbeat = None

    def _get_active_run(self):
        if self._id is not None:
            return self._id
        if self.observers:
            active_run = self.observers[0].query_active_run()
            if active_run is not None:
                return active_run
        return f"run-{uuid4()}"

    def _stop_time(self):
        self.end_time = datetime.datetime.now(datetime.UTC)
        return self.end_time

    def elapsed_time(self):
        """Return how long the run has been going, or ``None`` before it starts."""
        if self.start_time is None:
            return None
        return (self.end_time or datetime.datetime.now(datetime.UTC)) - self.start_time

    def _start_heartbeat(self):
        if self.beat_interval > 0:
            self._stop_heartbeat_event, self._heartbeat = IntervalTimer.create(
                self._emit_heartbeat, self.beat_interval
            )
            self._heartbeat.start()

    def _stop_heartbeat(self):
        # only stop if heartbeat was started
        if self._heartbeat is not None:
            self._stop_heartbeat_event.set()
            self._heartbeat.join(timeout=2)

    def _emit_heartbeat(self):
        beat_time = datetime.datetime.now(datetime.UTC)
        logger.debug("Run %s still running, result so far: %s", self._id, self.result)
        for observer in self.observers:
            observer.log_run_heartbeat(beat_time, self.result)

    def _emit_cancelled(self):
        self.status = RunStatus.CANCELLED
        cancelled_time = self._stop_time()
        logger.info("Cancelled run %s", self._id)

        # Update info on observers
        for observer in self.observers:
            observer.log_cancelled_run(cancelled_time)

    def _emit_queued(self):
        self.status = RunStatus.QUEUED
        queued_time = datetime.datetime.now(datetime.UTC)
        logger.info("Queued run %s", self._id)
        for observer in self.observers:
            observer.log_queued_run(self._id, queued_time)

    def _emit_started(self):
        self.status = RunStatus.RUNNING
        self._id = self._get_active_run()
        self.start_time = datetime.datetime.now(datetime.UTC)

        for observer in self.observers:
            _id = observer.log_started_run(self._id, self.start_time)
            self._id = _id
        logger.info("Starting run %s", self._id)
        self.log_host_info(host_info())

    def _emit_completed(self):
        self.status = RunStatus.COMPLETED
        completed_time = self._stop_time()
        logger.info("Completed run %s after %s, result: %s", self._id, self.elapsed_time(), self.result)
        for observer in self.observers:
            observer.log_completed_run(completed_time)

    def _emit_interrupted(self, error=None):
        self.status = RunStatus.INTERRUPTED
        interrupted_time = self._stop_time()
        logger.warning("Interrupted run %s after %s", self._id, self.elapsed_time())
        for observer in self.observers:
            observer.log_interrupted_run(interrupted_time, _fail_trace(error))

    def _emit_failed(self, error=None):
        self.status = RunStatus.FAILED
        failed_time = self._stop_time()
        logger.error("Failed run %s after %s", self._id, self.elapsed_time(), exc_info=error)
        for observer in self.observers:
            observer.log_failed_run(failed_time, _fail_trace(error))

    def _execute_hooks(self, hooks):
        for hook in hooks:
            hook()

    def queue(self):
        """Record this run as queued, before :meth:`run` starts it."""
        if self.status is not None:
            raise RuntimeError(f"cannot queue a run with status {self.status}")
        self._id = self._id or f"run-{uuid4()}"
        self._emit_queued()
        return self._id

    def cancel(self):
        """Cancel a queued run before it starts.

        A run stopped while already running is interrupted, not cancelled.
        """
        if self.status is not RunStatus.QUEUED:
            raise RuntimeError(f"only a queued run can be cancelled, not {self.status}")
        self._emit_cancelled()
        for observer in self.observers:
            observer.close()

    def main(self):
        """Execute the work represented by this run.

        Subclasses override this method and return their result.
        """
        raise NotImplementedError

    def run(self):
        """Run ``main`` and record completion, failure, or interruption."""
        if self.status is RunStatus.CANCELLED:
            raise RuntimeError("cannot start a cancelled run")

        try:
            self._emit_started()
            self._start_heartbeat()
            self._execute_hooks(self.pre_run_hooks)
            self.result = self.main()
            self._emit_completed()
            self._stop_heartbeat()
            self._execute_hooks(self.post_run_hooks)
        except KeyboardInterrupt as interrupt:
            self._stop_heartbeat()
            self._emit_interrupted(interrupt)
        except Exception as error:
            self._stop_heartbeat()
            self._emit_failed(error)
        finally:
            for observer in self.observers:
                observer.close()

        return self.result

    def log_scalar(self, metric_name, value, step: int = None):
        """Record a dimensionless scalar metric, optionally at a step."""
        for observer in self.observers:
            observer.log_scalar(metric_name, value, step)

    def log_sources(self, sources):
        """Record source-file metadata rows containing at least a ``path``."""
        for observer in self.observers:
            observer.log_sources(sources)

    def log_repositories(self, repositories):
        """Record repository metadata rows such as name, URL, and revision."""
        for observer in self.observers:
            observer.log_repositories(repositories)

    def log_dependencies(self, dependencies):
        """Record dependency metadata rows with a name and optional version."""
        for observer in self.observers:
            observer.log_dependencies(dependencies)

    def log_host_info(self, host_info):
        """Record host metadata such as hostname, operating system, and runtime."""
        for observer in self.observers:
            observer.log_host_info(host_info)

    def add_agent(self, agent_id: str, agent_type: str):
        """Add a PROV agent with the supplied identifier and RDF type."""
        for observer in self.observers:
            observer.add_agent(agent_id, agent_type)

    def add_activity(self, activity_id: str, activity_type: str, associated_with=None):
        """Add a PROV activity and optionally associate it with an agent."""
        for observer in self.observers:
            observer.add_activity(activity_id, activity_type, associated_with)

    def add_resource(
        self,
        filename,
        usage_activity=None,
        usage_time=None,
        title=None,
        archive_path=None,
        sha256=None,
        size_bytes=None,
    ):
        """Record a resource used by an activity, including optional file metadata."""
        if usage_time is None:
            usage_time = datetime.datetime.now(datetime.UTC)
        for observer in self.observers:
            observer.add_resource(
                filename,
                used_by=usage_activity,
                used_at=usage_time,
                label=title,
                archive_path=archive_path,
                sha256=sha256,
                size_bytes=size_bytes,
            )

    def add_artefact(
        self,
        filename,
        gen_activity=None,
        generated_time=None,
        title=None,
        archive_path=None,
        sha256=None,
        size_bytes=None,
    ):
        """Record an artefact generated by an activity, with optional file metadata."""
        if generated_time is None:
            generated_time = datetime.datetime.now(datetime.UTC)
        for observer in self.observers:
            observer.add_artefact(
                filename,
                generated_by=gen_activity,
                generated_at=generated_time,
                label=title,
                archive_path=archive_path,
                sha256=sha256,
                size_bytes=size_bytes,
            )

    def info(self):
        """Return the run ID, lifecycle type, timestamps, and result."""
        return {
            "id": self._id,
            "status": self.status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "result": self.result,
        }


class RunStatus:
    """REC lifecycle RDF types exposed by :attr:`Run.status`."""
    QUEUED = REC.QueuedRun
    RUNNING = REC.RunningRun
    COMPLETED = REC.CompletedRun
    FAILED = REC.FailedRun
    INTERRUPTED = REC.InterruptedRun
    CANCELLED = REC.CancelledRun
