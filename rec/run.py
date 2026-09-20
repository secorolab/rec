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

from rec.observers import graph_observer
from rec.observers.base import BaseObserver
from rec.observers.file_observer import FileObserver

logger = logging.getLogger(__name__)


class IntervalTimer(threading.Thread):
    def __init__(self, func, interval=10.0):
        super().__init__()
        self.stopped = threading.Event()
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
    """

    def __init__(self, observers: Sequence[BaseObserver] = (), run_id: str | None = None):
        self._id = run_id
        self.observers = observers
        archive = next((observer.path for observer in observers if isinstance(observer, FileObserver)), None)
        if archive is not None:
            for observer in observers:
                observer.set_file_source(archive)
        self.start_time = None
        self.end_time = None
        self.status = None
        self.result = None
        # Set when the run is asked to stop; a cooperative main() polls it and returns.
        self.cancel_requested = threading.Event()

        self.beat_interval = 10
        self._heartbeat = None

    def _stop_time(self):
        self.end_time = datetime.datetime.now(datetime.UTC)
        return self.end_time

    def _start_heartbeat(self):
        if self.beat_interval > 0:
            self._heartbeat = IntervalTimer(self._emit_heartbeat, self.beat_interval)
            self._heartbeat.start()

    def _stop_heartbeat(self):
        if self._heartbeat is not None:
            self._heartbeat.stopped.set()
            self._heartbeat.join(timeout=2)

    def _emit_heartbeat(self):
        beat_time = datetime.datetime.now(datetime.UTC)
        logger.debug("Run %s still running, result so far: %s", self._id, self.result)
        for observer in self.observers:
            observer.log_run_heartbeat(beat_time, self.result)
        if self.status is RunStatus.RUNNING and any(o.cancel_requested() for o in self.observers):
            self._emit_canceling()

    def _emit_canceling(self):
        self.status = RunStatus.CANCELING
        self.cancel_requested.set()
        logger.info("Cancelling run %s", self._id)
        for observer in self.observers:
            observer.request_cancel()

    def _emit_cancelled(self):
        self.status = RunStatus.CANCELLED
        cancelled_time = self._stop_time()
        logger.info("Cancelled run %s", self._id)
        for observer in self.observers:
            observer.log_cancelled_run(cancelled_time)

    def _emit_queued(self):
        self.status = RunStatus.QUEUED
        queued_time = datetime.datetime.now(datetime.UTC)
        logger.info("Queued run %s", self._id)
        for observer in self.observers:
            observer.log_queued_run(self._id, queued_time)

    def _emit_started(self, trigger=None, starter=None):
        """Record the run as started, optionally with the PROV entity that triggered it
        and the activity that generated that trigger."""
        self._id = self._id or f"run-{uuid4()}"
        self.start_time = datetime.datetime.now(datetime.UTC)

        for observer in self.observers:
            _id = observer.log_started_run(self._id, self.start_time, trigger, starter)
            self._id = _id
        # Running only once every observer says so: a cancel that arrives earlier is refused.
        self.status = RunStatus.RUNNING
        logger.info("Starting run %s", self._id)
        self.log_host_info(host_info())

    def _emit_completed(self):
        self.status = RunStatus.COMPLETED
        completed_time = self._stop_time()
        logger.info("Completed run %s, result: %s", self._id, self.result)
        for observer in self.observers:
            observer.log_completed_run(completed_time)

    def _emit_interrupted(self, error=None):
        self.status = RunStatus.INTERRUPTED
        interrupted_time = self._stop_time()
        logger.warning("Interrupted run %s", self._id)
        for observer in self.observers:
            observer.log_interrupted_run(interrupted_time, _fail_trace(error))

    def _emit_failed(self, error=None):
        self.status = RunStatus.FAILED
        failed_time = self._stop_time()
        logger.error("Failed run %s", self._id, exc_info=error)
        for observer in self.observers:
            observer.log_failed_run(failed_time, _fail_trace(error))

    def queue(self):
        """Record this run as queued, before :meth:`run` starts it."""
        if self.status is not None:
            raise RuntimeError(f"cannot queue a run with status {self.status}")
        self._id = self._id or f"run-{uuid4()}"
        self._emit_queued()
        return self._id

    def cancel(self):
        """Cancel this run: a queued run ends now, a running one once ``main`` returns.

        A run in another process is cancelled through an observer bound to its id:
        ``FileObserver(path).request_cancel()`` or ``MariaDBObserver(run_id).request_cancel()``.
        """
        if self.status is RunStatus.QUEUED:
            self._emit_cancelled()
            for observer in self.observers:
                observer.close()
        elif self.status is RunStatus.RUNNING:
            self._emit_canceling()
        else:
            raise RuntimeError(f"only a queued or running run can be cancelled, not {self.status}")

    def main(self):
        """Execute the work represented by this run.

        Subclasses override this method and return their result.
        """
        raise NotImplementedError

    def run(self, trigger=None, starter=None):
        """Run ``main`` and record completion, failure, or interruption.

        Args:
            trigger: Entity whose creation started this run.
            starter: Activity that generated the trigger.
        """
        if self.status is RunStatus.CANCELLED:
            raise RuntimeError("cannot start a cancelled run")
        if any(observer.cancel_requested() for observer in self.observers):
            self.cancel_requested.set()
            self._emit_cancelled()
            for observer in self.observers:
                observer.close()
            return None

        try:
            self._emit_started(trigger, starter)
            self._start_heartbeat()
            self.result = self.main()
            self._stop_heartbeat()
            self._emit_cancelled() if self.cancel_requested.is_set() else self._emit_completed()
        except KeyboardInterrupt as interrupt:
            self._stop_heartbeat()
            self._emit_cancelled() if self.cancel_requested.is_set() else self._emit_interrupted(interrupt)
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

    def log_host_info(self, host_info):
        """Record host metadata such as hostname, operating system, and runtime."""
        for observer in self.observers:
            observer.log_host_info(host_info)

    def add_agent(self, agent_id: str, agent_type: str, name: str | None = None):
        """Add a PROV agent with the supplied identifier and RDF type; software agents are named."""
        for observer in self.observers:
            observer.add_agent(agent_id, agent_type, name)

    def add_activity(self, activity_id: str, activity_type: str, associated_with=None):
        """Add a PROV activity and optionally associate it with an agent."""
        for observer in self.observers:
            observer.add_activity(activity_id, activity_type, associated_with)

    def add_software(self, name: str, version: str | None = None, commit: str | None = None, repository=None):
        """Record a software package the run ran with, as a software agent it is associated with."""
        for observer in self.observers:
            observer.add_software(name, version, commit, repository)

    def add_resource(
        self,
        path,
        used_by=None,
        used_at=None,
        label=None,
        archive_path=None,
        sha256=None,
        size_bytes=None,
    ):
        """Record a file used by an activity (the run by default); return the entity IRI minted."""
        if used_at is None:
            used_at = datetime.datetime.now(datetime.UTC)
        entity = None
        for observer in self.observers:
            entity = observer.add_resource(path, used_by, used_at, label, sha256, size_bytes, archive_path) or entity
        return entity

    def add_artefact(
        self,
        path,
        generated_by=None,
        generated_at=None,
        label=None,
        archive_path=None,
        sha256=None,
        size_bytes=None,
    ):
        """Record a file generated by an activity (the run by default); return the entity IRI minted."""
        if generated_at is None:
            generated_at = datetime.datetime.now(datetime.UTC)
        entity = None
        for observer in self.observers:
            entity = (
                observer.add_artefact(path, generated_by, generated_at, label, sha256, size_bytes, archive_path)
                or entity
            )
        return entity


class RunStatus:
    """Lifecycle values exposed by :attr:`Run.status`: an OSLC Automation (state, verdict) pair."""

    QUEUED = graph_observer.QUEUED
    RUNNING = graph_observer.IN_PROGRESS
    COMPLETED = graph_observer.COMPLETED
    FAILED = graph_observer.FAILED
    INTERRUPTED = graph_observer.INTERRUPTED
    CANCELING = graph_observer.CANCELING
    CANCELLED = graph_observer.CANCELLED
