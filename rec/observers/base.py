# SPDX-License-Identifier: MPL-2.0
# SPDX-FileCopyrightText: 2026 SECORO AG (secoro.uni-bremen.de)
# Author: Argentina Ortega

from datetime import datetime


class BaseObserver:
    def log_run_heartbeat(self, beat_time: datetime, result: object | None):
        raise NotImplementedError

    def log_cancelled_run(self, cancelled_time: datetime):
        raise NotImplementedError

    def log_queued_run(self, run_id: str):
        raise NotImplementedError

    def log_started_run(self, run_id: str, started_time: datetime) -> str:
        raise NotImplementedError

    def log_completed_run(self, completed_time: datetime):
        raise NotImplementedError

    def log_interrupted_run(self, interrupted_time: datetime):
        raise NotImplementedError

    def log_failed_run(self, failed_time: datetime):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def log_sources(self, sources):
        pass

    def log_repositories(self, repositories):
        pass

    def log_dependencies(self, dependencies):
        pass

    def log_host_info(self, host_info):
        pass

    def add_agent(self, agent_id, agent_type):
        pass

    def add_activity(self, activity_id, activity_type, associated_with=None):
        pass

    def add_resource(self, path, used_by, used_at, label=None, sha256=None, size_bytes=None, archive_path=None):
        pass

    def add_artefact(self, path, generated_by, generated_at, label=None, sha256=None, size_bytes=None, archive_path=None):
        pass

    def log_scalar(self, metric_name, value, step=None):
        pass
