import datetime
import logging
import platform
import socket
import threading
import traceback
from collections.abc import Sequence
from contextlib import ExitStack, contextmanager
from uuid import uuid4

from rec import State, Verdict
from rec.observer import Observer

logger = logging.getLogger(__name__)


class IntervalTimer(threading.Thread):
    def __init__(self, event, func, interval=10.0):
        # A daemon: a beat stuck on a store must not keep the process alive once the run is over.
        super().__init__(daemon=True)
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
        observers: Sequence[Observer] = (),
        run_id: str | None = None,
    ):
        self._id = run_id
        self.observers = list(observers)
        # A start holds every store's record at once; a store held twice would wait on itself.
        if len({id(observer.store) for observer in self.observers}) < len(self.observers):
            raise ValueError("'observers' share a store -- give each its own store")
        self.result = None
        self._heartbeat = None
        self.beat_interval = 10

    @property
    def id(self):
        """The run's identity, the same in every observer; minted here when the caller gave none"""
        if self._id is None:
            self._id = str(uuid4())
        return self._id

    @property
    def state(self):
        """The run's state as the first observer's store keeps it, ``new`` until it is stored"""
        record = self.observers[0].store.load(self.id) if self.observers else None
        return record.state if record else State.NEW

    @property
    def verdict(self):
        """The run's verdict as the first observer's store keeps it"""
        record = self.observers[0].store.load(self.id) if self.observers else None
        return record.verdict if record else Verdict.UNAVAILABLE

    @contextmanager
    def _held(self):
        """Every store's record open at once: one refusal leaves them all unchanged; a failed save can still split them."""
        with ExitStack() as held:
            for observer in self.observers:
                held.enter_context(observer._hold(self.id))
            yield

    def _start_heartbeat(self):
        if self.beat_interval > 0:
            self._stop_heartbeat_event = threading.Event()
            self._heartbeat = IntervalTimer(self._stop_heartbeat_event, self._emit_heartbeat, self.beat_interval)
            self._heartbeat.start()

    def _stop_heartbeat(self):
        if self._heartbeat is not None:
            self._stop_heartbeat_event.set()
            self._heartbeat.join(timeout=2)

    def _emit_heartbeat(self):
        beat_time = datetime.datetime.now(datetime.UTC)
        logger.debug("Run %s still running", self.id)

        # Update info on observers
        for observer in self.observers:
            observer.log_run_heartbeat(self.id, beat_time)

    def _emit_started(self):
        """Record the run as started, on the machine executing it"""
        start_time = datetime.datetime.now(datetime.UTC)
        logger.info("Starting run %s", self.id)
        with self._held():
            for observer in self.observers:
                observer.log_started_run(self.id, start_time)
        self.log_host_info(host_info())

    def _emit_ended(self, error=None):
        """End the run as passed, error or failed by ``error``, or as canceled if a cancel came while it ran"""
        end_time = datetime.datetime.now(datetime.UTC)
        with self._held():
            for observer in self.observers:
                if error is None:
                    observer.log_completed_run(self.id, end_time)
                elif isinstance(error, KeyboardInterrupt):
                    observer.log_interrupted_run(self.id, end_time, _fail_trace(error))
                else:
                    observer.log_failed_run(self.id, end_time, _fail_trace(error))
        logger.log(logging.INFO if error is None else logging.ERROR, "Ended run %s", self.id, exc_info=error)

    def main(self) -> object:
        pass

    def queue(self):
        """Record the run as queued, before a runner starts it; the store refuses a run that is not new"""
        queued_time = datetime.datetime.now(datetime.UTC)
        with self._held():
            for observer in self.observers:
                observer.log_queued_run(self.id, queued_time)
        logger.info("Queued run %s", self.id)
        return self.id

    def cancel(self):
        """Cancel a queued run, or ask one in progress to stop: it ends as canceled once ``main`` returns.

        Any holder of the run's id can cancel it, through a ``Run`` with that ``run_id`` on the same stores.
        ``main`` sees the request as ``self.state`` being ``State.CANCELING``.
        """
        cancel_time = datetime.datetime.now(datetime.UTC)
        with self._held():
            for observer in self.observers:
                observer.log_cancelled_run(self.id, cancel_time)
        logger.info("Cancelled run %s", self.id)

    def run(self):
        """
        Start the run, execute ``main`` and record how it ended
        :returns: What ``main`` returned; rec does not record it
        :raises RuntimeError: The store holds the run as started or canceled
        """
        self._emit_started()
        failure = None
        try:
            # Stopped before the end is recorded: a late beat would find the run ended and raise.
            try:
                self._start_heartbeat()
                self.result = self.main()
            finally:
                self._stop_heartbeat()
        except (KeyboardInterrupt, Exception) as error:
            failure = error
        self._emit_ended(failure)
        return self.result

    def log_host_info(self, host_info: dict):
        """
        Log information about the machine executing this run
        :param host_info: ``hostname``, ``os``, ``python``, ``cpu``; see :func:`host_info`
        """
        for observer in self.observers:
            observer.log_host_info(self.id, host_info)


def _fail_trace(error):
    return "".join(traceback.format_exception(error)) if error is not None else None
