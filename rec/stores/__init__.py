"""Where run records are kept: any object with these methods is a store."""

from contextlib import AbstractContextManager
from typing import Protocol

from rec import State
from rec.record import RunRecord


class Store(Protocol):
    def load(self, run_id: str) -> RunRecord | None:
        """Return the stored record of a run, or None if the run is not stored."""
        ...

    def edit(self, run_id: str) -> AbstractContextManager[RunRecord]:
        """Lock a run for changing and keep the changes to its record when the with block exits.

        Yields a new record if the run is not stored. Other edits of the same run,
        also from other processes, wait until the block exits. If the block
        raises, nothing is kept.
        """
        ...

    def run_ids(self, state: State | None = None) -> list[str]:
        """Return the ids of all stored runs, or only of those in the given state."""
        ...

    def close(self) -> None:
        """Close the store's connections; the code that created the store calls it once."""
        ...
