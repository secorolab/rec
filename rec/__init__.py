"""Where a run is and how it turned out: OSLC Automation state and verdict, by their rec.json terms."""

from enum import StrEnum


class State(StrEnum):
    NEW = "new"
    QUEUED = "queued"
    IN_PROGRESS = "in-progress"
    CANCELING = "canceling"
    CANCELED = "canceled"
    COMPLETE = "complete"


class Verdict(StrEnum):
    UNAVAILABLE = "unavailable"
    PASSED = "passed"
    WARNING = "warning"
    FAILED = "failed"
    ERROR = "error"
