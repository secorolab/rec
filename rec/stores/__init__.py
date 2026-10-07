"""Where run records are kept: any object with these methods is a store."""

from contextlib import AbstractContextManager
from typing import Protocol

from rec import State
from rec.record import RunRecord


class Store(Protocol):
    def load(self, run_id: str) -> RunRecord | None:
        """The run's record, None for a run the store does not hold."""

    def edit(self, run_id: str) -> AbstractContextManager[RunRecord]:
        """The run's record, new if not held, kept when the block ends; no other edit of the run interleaves and an error keeps nothing."""

    def run_ids(self, state: State | None = None) -> list[str]:
        """The runs the store holds, those in STATE only when given."""

    def close(self) -> None:
        """Release what the store holds open; whoever created it calls this once."""
