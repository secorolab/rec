import datetime
import logging
import platform
import socket
import threading
import traceback
from uuid import uuid4

from rec import State, Verdict
from rec.observer import Observer

logger = logging.getLogger(__name__)


def host_info():
    """Information about the machine executing a run"""
    return {
        "hostname": socket.gethostname(),
        "os": platform.platform(),
        "python": platform.python_version(),
        "cpu": platform.processor() or None,
    }


class Run:
    def __init__(self, observer: Observer, run_id: str | None = None):
        """
        :param observer: Records the run in its store
        :param run_id: The run's id in the store; a UUID is minted when none is given
        """
        self.observer = observer
        self._id = run_id
        self.result = None
        self.beat_interval = 10
        self._stop_beating = threading.Event()

    @property
    def id(self):
        """The run's identity in the store; minted here when the caller gave none"""
        if self._id is None:
            self._id = str(uuid4())
        return self._id

    @property
    def state(self) -> State:
        """The run's state as the store keeps it, ``new`` until it is stored"""
        return self.observer.lifecycle(self.id)[0]

    @property
    def verdict(self) -> Verdict:
        """The run's verdict as the store keeps it"""
        return self.observer.lifecycle(self.id)[1]

    def _beat(self):
        while not self._stop_beating.wait(self.beat_interval):
            # A store briefly out of reach must not stop the beats that follow.
            try:
                self.observer.log_run_heartbeat(self.id, datetime.datetime.now(datetime.UTC))
            except Exception:
                logger.exception("Run %s could not record its heartbeat", self.id)

    def main(self) -> object:
        pass

    def queue(self):
        """Record the run as queued, before a runner starts it; the store refuses a run that is not new"""
        self.observer.log_queued_run(self.id, datetime.datetime.now(datetime.UTC))
        logger.info("Queued run %s", self.id)
        return self.id

    def cancel(self):
        """Cancel a queued run, or ask one in progress to stop: it ends as canceled once ``main`` returns.

        Any holder of the run's id can cancel it, through a ``Run`` with that ``run_id`` on the same store.
        ``main`` sees the request as ``self.state`` being ``State.CANCELING``.
        """
        self.observer.log_cancelled_run(self.id)
        logger.info("Cancelled run %s", self.id)

    def run(self):
        """
        Start the run, execute ``main`` and record how it ended
        :returns: What ``main`` returned; rec does not record it
        :raises RuntimeError: The store holds the run as started or canceled
        :raises BaseException: What interrupted ``main``, e.g. ``KeyboardInterrupt`` or ``SystemExit``, once the run is ended as error
        """
        self.observer.log_started_run(self.id, datetime.datetime.now(datetime.UTC))
        logger.info("Started run %s", self.id)
        beating = threading.Thread(target=self._beat, daemon=True) if self.beat_interval > 0 else None
        error = None
        # Everything after the start is inside: whatever fails, the started run gets its end.
        try:
            self.log_host_info(host_info())
            if beating is not None:
                beating.start()
            self.result = self.main()
        except BaseException as caught:
            error = caught
        finally:
            # Stopped before the end is recorded: a late beat would find the run ended.
            self._stop_beating.set()
            if beating is not None and beating.is_alive():
                beating.join()
        if error is None:
            verdict = Verdict.PASSED
        elif isinstance(error, Exception):
            verdict = Verdict.FAILED
        else:
            verdict = Verdict.ERROR
        self.observer.log_ended_run(
            self.id,
            datetime.datetime.now(datetime.UTC),
            verdict,
            "".join(traceback.format_exception(error)) if error is not None else None,
        )
        logger.log(logging.INFO if error is None else logging.ERROR, "Ended run %s", self.id, exc_info=error)
        # An interrupt is the process being asked to stop: recorded, then passed on.
        if error is not None and not isinstance(error, Exception):
            raise error
        return self.result

    def log_host_info(self, host_info: dict):
        """
        Log information about the machine executing this run
        :param host_info: ``hostname``, ``os``, ``python``, ``cpu``; see :func:`host_info`
        """
        self.observer.log_host_info(self.id, host_info)
