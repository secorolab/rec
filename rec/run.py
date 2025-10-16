import datetime
import threading
from typing import Sequence

from rec.observers.base import BaseObserver


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


class Run:
    def __init__(
        self,
        observers: Sequence[BaseObserver] = [],
        ingredients: list = [],
        run_id: int = None,
        scenario=None,
        pre_run_hooks: list = [],
        post_run_hooks: list = [],
        **kwargs,
    ):
        self._id = run_id
        self.observers = observers
        self.ingredients = ingredients
        self.start_time = None
        self.end_time = None
        self.status = None
        self.result = None
        self.pre_run_hooks = pre_run_hooks
        self.post_run_hooks = post_run_hooks

        self.heartbeat = None
        self.beat_interval = 10

    def _get_active_run(self):
        if self._id is not None:
            # We start with a known ID, we probably don't need to do anything
            pass
        if self._id is None:
            # First query the DB to see if there is any run marked as active.
            # If no run is marked as active, create one in the DB and return the ID
            pass

    def _stop_time(self):
        self.stop_time = datetime.datetime.now(datetime.UTC)
        elapsed_time = datetime.timedelta(
            seconds=round((self.stop_time - self.start_time).total_seconds())
        )
        return elapsed_time

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
        print("Running....")

        # Update info on observers
        for observer in self.observers:
            observer.log_run_heartbeat(self._id, beat_time, result=self.result)

    def _emit_cancelled(self):
        self.status = RunStatus.CANCELLED
        cancelled_time = datetime.datetime.now(datetime.UTC)

        # Update info on observers
        for observer in self.observers:
            observer.log_cancelled_run(self._id, cancelled_time)

    def _emit_queued(self):
        self.status = RunStatus.QUEUED
        queued_time = datetime.datetime.now(datetime.UTC)

        # Update info on observers
        for observer in self.observers:
            observer.log_queued_run(self._id, queued_time)

    def _emit_started(self, trigger=None, starter=None):
        """
        Records the metadata of the starting run
        :param trigger: An entity that triggers the run
        :param starter: The activity that generated the trigger
        :return:
        """
        self.status = RunStatus.RUNNING
        self._id = self._get_active_run()
        self.start_time = datetime.datetime.now(datetime.UTC)
        print("Starting run {}".format(self._id))

        # Update info on observers
        for observer in self.observers:
            observer.log_started_run(
                self._id,
                trigger=trigger,
                starter=starter,
            )

    def _emit_completed(self):
        self.status = RunStatus.COMPLETED
        elapsed_time = self._stop_time()
        print("Completed after {}".format(elapsed_time))

        # Update info on observers
        for observer in self.observers:
            observer.log_completed_run(elapsed_time)

    def _emit_interrupted(self):
        self.status = RunStatus.INTERRUPTED
        elapsed_time = self._stop_time()

        # Update info on observers
        for observer in self.observers:
            observer.log_interrupted_run(elapsed_time)

    def _emit_failed(self):
        self.status = RunStatus.FAILED
        elapsed_time = self._stop_time()

        # Update info on observers
        for observer in self.observers:
            observer.log_failed_run(elapsed_time)

    def _execute_hooks(self, hooks):
        for hook in hooks:
            hook()

    def main(self):
        pass

    def run(self):
        """
        A centralized runner can start and complete a run. Decentralized runners should not use this method.
        :return:
        """
        import time

        try:
            self._emit_started()
            self._start_heartbeat()
            self._execute_hooks(self.pre_run_hooks)
            self.result = self.main()
            print("Result: {}".format(self.result))
            self._emit_completed()
            self._stop_heartbeat()
            self._execute_hooks(self.post_run_hooks)
        except KeyboardInterrupt as k:
            self._emit_interrupted()
        except Exception as ex:
            self._emit_failed()
        finally:
            for observer in self.observers:
                observer.close()

        return self.result

    def log_scalar(self, metric_name, value, step=None):
        pass

    def sources(self):
        pass

    def repositories(self):
        pass

    def dependencies(self):
        pass

    def host_info(self):
        pass

    def add_agent(self, agent_id: str, agent_type: str, **kwargs):
        """
        Add an agent (e.g., a robot) to the run
        :param agent_id: A unique ID for this agent
        :param agent_type: The type of agent being added, e.g., software agent, robot etc.
        :param kwargs:
        :return:
        """
        pass

    def add_resource(
        self,
        filename,
        usage_activity=None,
        usage_time=None,
        title=None,
        **kwargs,
    ):
        """
        Add a resource to the run
        :param filename: The file path of the resource
        :param usage_activity: The activity that uses this resource. Default: This run
        :param usage_time: The time this resource is used by the activity.
        :param title: A short title for the resource. Default: The file name
        :param kwargs:
        :return:
        """
        if usage_time is None:
            usage_time = datetime.datetime.now(datetime.UTC)

    def add_artefact(
        self,
        filename,
        gen_activity=None,
        generated_time=None,
        title=None,
        **kwargs,
    ):
        """
        Add an artefact to the run
        :param filename: he file path of the generated artefact
        :param gen_activity: The generating activity. Default: This run
        :param generated_time: The time the artefact was generated
        :param title: A short title for the generated artefact
        :param kwargs:
        :return:
        """
        if generated_time is None:
            generated_time = datetime.datetime.now(datetime.UTC)

    def info(self):
        pass


class RunStatus:
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"
    CANCELLED = "CANCELLED"
    QUEUED = "QUEUED"
    TIMED_OUT = "TIMED_OUT"
    DEAD = "DEAD"
