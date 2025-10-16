from datetime import timedelta, datetime


class BaseObserver:
    def log_run_heartbeat(self, run_id: int, beat_time: datetime, result):
        raise NotImplementedError

    def log_cancelled_run(self, run_id: int, cancelled_time: datetime):
        raise NotImplementedError

    def log_queued_run(self, run_id: int, queued_time: datetime):
        raise NotImplementedError

    def log_started_run(
        self, run_id: int, started_time: datetime, trigger=None, starter=None
    ):
        raise NotImplementedError

    def log_completed_run(self, run_id: int, completed_time: datetime):
        raise NotImplementedError

    def log_interrupted_run(self, run_id: int, elapsed_time: timedelta):
        raise NotImplementedError

    def log_failed_run(self, run_id: int, elapsed_time: timedelta):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError
