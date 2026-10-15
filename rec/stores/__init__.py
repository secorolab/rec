"""Where run records are kept: any object with these methods is a store."""

from contextlib import AbstractContextManager
from datetime import datetime
from typing import Protocol

from rec import State
from rec.record import RunRecord


class RunHandle(Protocol):
    """A run held by ``Store.edit``: its state, and the changes made to it."""

    run_id: str
    state: State

    def update(self, **values) -> None:
        """Set fields of the run that it has one of, e.g. ``state``, ``start_time`` or ``host``."""
        ...

    def add(self, kind: str, row, replace: bool = False) -> None:
        """Add a row to one of the run's lists, e.g. ``artefacts``.

        Raises ValueError if the list already has a row with the same key
        (``rec.record.KEYS``), unless ``replace`` is set.
        """
        ...

    def add_metric(self, name: str, value: float, step: int | None = None, time: datetime | None = None) -> None:
        """Add a metric's value at ``step``, or after its last step, replacing a value at that step.

        Raises ValueError for a NaN or infinite value.
        """
        ...


class Store(Protocol):
    def load(self, run_id: str) -> RunRecord | None:
        """Return the stored record of a run, or None if the run is not stored."""
        ...

    def edit(self, run_id: str) -> AbstractContextManager[RunHandle]:
        """Lock a run for changing and keep the changes when the with block exits.

        Yields a new run if the run is not stored. Other edits of the same run,
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
