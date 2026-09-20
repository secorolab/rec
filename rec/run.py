import datetime
import logging
import platform
import socket
import threading
import traceback
from typing import Sequence
from uuid import uuid4

from rec.observers.base import BaseObserver

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
        self.func()


def host_info():
    """Information about the machine executing a run"""
    return {
        "hostname": socket.gethostname(),
        "os": platform.platform(),
        "python": platform.python_version(),
        "cpu": platform.processor() or None,
    }


class Run:
    def __init__(
        self,
        observers: Sequence[BaseObserver] = (),
        ingredients: list = (),
        run_id: str = None,
        scenario=None,
        pre_run_hooks: list = (),
        post_run_hooks: list = (),
        **kwargs,
    ):
        self._id = run_id
        self.observers = list(observers)
        self.ingredients = list(ingredients)
        self.scenario = scenario
        self.start_time = None
        self.end_time = None
        self.status = None
        self.result = None
        self.pre_run_hooks = list(pre_run_hooks)
        self.post_run_hooks = list(post_run_hooks)

        self._heartbeat = None
        self.beat_interval = 10

    @property
    def id(self):
        """The run's identity, the same in every observer; minted here when the caller gave none"""
        if self._id is None:
            self._id = str(uuid4())
        return self._id

    def _stop_time(self):
        self.end_time = datetime.datetime.now(datetime.UTC)
        return self.end_time

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
        logger.debug("Run %s still running, result so far: %s", self.id, self.result)

        # Update info on observers
        for observer in self.observers:
            observer.log_run_heartbeat(self.id, beat_time, result=self.result)

    def _emit_cancelled(self):
        self.status = RunStatus.CANCELLED
        cancelled_time = self._stop_time()
        logger.info("Cancelled run %s", self.id)

        # Update info on observers
        for observer in self.observers:
            observer.log_cancelled_run(self.id, cancelled_time)

    def _emit_queued(self):
        self.status = RunStatus.QUEUED
        queued_time = datetime.datetime.now(datetime.UTC)
        logger.info("Queued run %s", self.id)

        # Update info on observers
        for observer in self.observers:
            observer.log_queued_run(self.id, queued_time)

    def _emit_started(self, trigger=None, starter=None):
        """
        Records the metadata of the starting run
        :param trigger: An entity that triggers the run
        :param starter: The activity that generated the trigger
        :return:
        """
        self.status = RunStatus.RUNNING
        self.start_time = datetime.datetime.now(datetime.UTC)
        logger.info("Starting run %s", self.id)

        # Update info on observers
        for observer in self.observers:
            observer.log_started_run(self.id, self.start_time, trigger=trigger, starter=starter)
        self.log_host_info(host_info())

    def _emit_completed(self):
        self.status = RunStatus.COMPLETED
        completed_time = self._stop_time()
        logger.info("Completed run %s after %s", self.id, completed_time - self.start_time)

        # Update info on observers
        for observer in self.observers:
            observer.log_completed_run(self.id, completed_time)

    def _emit_interrupted(self, error=None):
        self.status = RunStatus.INTERRUPTED
        interrupted_time = self._stop_time()
        logger.warning("Interrupted run %s after %s", self.id, interrupted_time - self.start_time)

        # Update info on observers
        for observer in self.observers:
            observer.log_interrupted_run(self.id, interrupted_time, _fail_trace(error))

    def _emit_failed(self, error=None):
        self.status = RunStatus.FAILED
        failed_time = self._stop_time()
        logger.error("Failed run %s after %s", self.id, failed_time - self.start_time, exc_info=error)

        # Update info on observers
        for observer in self.observers:
            observer.log_failed_run(self.id, failed_time, _fail_trace(error))

    def _execute_hooks(self, hooks):
        for hook in hooks:
            hook()

    def main(self):
        pass

    def queue(self):
        """Record the run as queued, before a runner starts it"""
        if self.status is not None:
            raise RuntimeError(f"cannot queue a run with status {self.status}")
        self._emit_queued()
        return self.id

    def cancel(self):
        """Cancel a queued run before it starts"""
        if self.status is not RunStatus.QUEUED:
            raise RuntimeError(f"only a queued run can be cancelled, not {self.status}")
        self._emit_cancelled()

    def run(self, trigger=None, starter=None):
        """
        A centralized runner can start and complete a run. Decentralized runners should not use this method.
        :param trigger: An entity that triggers the run
        :param starter: The activity that generated the trigger
        :return:
        """
        if self.status is RunStatus.CANCELLED:
            raise RuntimeError("cannot start a cancelled run")

        try:
            self._emit_started(trigger, starter)
            self._start_heartbeat()
            self._execute_hooks(self.pre_run_hooks)
            self.result = self.main()
            logger.info("Result of run %s: %s", self.id, self.result)
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
        """Log a measurement at runtime

        :param metric_name: Name of the metric being logged
        :param value: The measured value
        :param step: Optional. Integer value representing the iteration number
        :return:
        """
        now = datetime.datetime.now(datetime.UTC)
        for observer in self.observers:
            observer.log_scalar(self.id, metric_name, value, step, now)

    def log_sources(self, sources: list):
        """Log the source code files used to execute this run (e.g., file name/path, md5)
        :param sources: rows with a ``path`` and optionally ``title``, ``sha256``, ``size_bytes``
        :return:
        """
        for observer in self.observers:
            observer.log_sources(self.id, sources)

    def log_repositories(self, repositories: list):
        """
        Log the git information of the sources used in this run (url, commit/tag, uncommited changes)
        :param repositories: rows with a ``name`` and optionally ``url``, ``commit``
        :return:
        """
        for observer in self.observers:
            observer.log_repositories(self.id, repositories)

    def log_dependencies(self, dependencies: list):
        """
        Log the dependencies of the sources used in this run (e.g., package name, version)
        :param dependencies: rows with a ``name`` and optionally ``version``
        :return:
        """
        for observer in self.observers:
            observer.log_dependencies(self.id, dependencies)

    def log_host_info(self, host_info: dict):
        """
        Log information about the machine executing this run (e.g., cpu, gpu, OS, user, hostname, python version and venv, env variables, etc.)
        :param host_info: ``hostname``, ``os``, ``python``, ``cpu``; see :func:`host_info`
        :return:
        """
        for observer in self.observers:
            observer.log_host_info(self.id, host_info)

    def add_agent(self, agent_id: str, agent_type: str, name: str = None):
        """
        Add an agent (e.g., a robot) to the run
        :param agent_id: A unique ID for this agent
        :param agent_type: The type of agent being added, e.g., SoftwareAgent, Person
        :param name: The agent's name; a software agent must have one
        :return:
        """
        for observer in self.observers:
            observer.add_agent(self.id, agent_id, agent_type, name)

    def add_resource(
        self,
        filename,
        usage_activity=None,
        usage_time=None,
        title=None,
        sha256=None,
        size_bytes=None,
    ):
        """
        Add a resource to the run

        This method only records the filename and minimal metadata.
        If you need to process file contents in some way, use a Feature.

        :param filename: The file path of the resource
        :param usage_activity: The activity that uses this resource. Default: This run
        :param usage_time: The time this resource is used by the activity.
        :param title: A short title for the resource. Default: The file name
        :param sha256: The file's SHA-256 checksum, as hex
        :param size_bytes: The file's size
        :return:
        """
        if usage_time is None:
            usage_time = datetime.datetime.now(datetime.UTC)
        for observer in self.observers:
            observer.add_resource(self.id, filename, usage_activity, usage_time, title, sha256, size_bytes)

    def add_artefact(
        self,
        filename,
        gen_activity=None,
        generated_time=None,
        title=None,
        sha256=None,
        size_bytes=None,
    ):
        """
        Add an artefact to the run

        This method only records the filename and minimal metadata.
        If you need to process file contents in some way, use a Feature.

        :param filename: he file path of the generated artefact
        :param gen_activity: The generating activity. Default: This run
        :param generated_time: The time the artefact was generated
        :param title: A short title for the generated artefact
        :param sha256: The file's SHA-256 checksum, as hex
        :param size_bytes: The file's size
        :return:
        """
        if generated_time is None:
            generated_time = datetime.datetime.now(datetime.UTC)
        for observer in self.observers:
            observer.add_artefact(self.id, filename, gen_activity, generated_time, title, sha256, size_bytes)

    def info(self):
        """A summary of the run data

        :return:
        """
        return {
            "id": self._id,
            "status": self.status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "result": self.result,
        }


def _fail_trace(error):
    return "".join(traceback.format_exception(error)) if error is not None else None


class RunStatus:
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"
    CANCELLED = "CANCELLED"
    QUEUED = "QUEUED"
    TIMED_OUT = "TIMED_OUT"
    DEAD = "DEAD"
