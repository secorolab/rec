class BaseObserver:
    def log_run_heartbeat(self):
        raise NotImplementedError

    def log_cancelled_run(self):
        raise NotImplementedError

    def log_queued_run(self):
        raise NotImplementedError

    def log_started_run(self):
        raise NotImplementedError

    def log_completed_run(self):
        raise NotImplementedError

    def log_interrupted_run(self):
        raise NotImplementedError

    def log_failed_run(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError
