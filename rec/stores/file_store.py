"""A store keeping each run as one file in a directory."""

import fcntl
from contextlib import contextmanager
from pathlib import Path

from rdf_utils import DatasetFormat
from rdflib import Dataset

from rec import State, provenance
from rec.record import RUN_ID, RunRecord
from rec.stores import Store


class FileStore(Store):
    def __init__(self, directory, fmt: DatasetFormat = DatasetFormat.JSON_LD, base: str = provenance.RUN_BASE):
        """
        :param directory: Where the files go; created on first write
        :param fmt: The format of the run's provenance bundle, kept as ``<run_id>.<fmt.extension>``
        :param base: IRI base of the run nodes
        """
        self.directory = Path(directory)
        self.fmt = DatasetFormat(fmt)
        self.base = base

    def path(self, run_id: str) -> Path:
        """Return the run's file, ``<run_id>.<fmt.extension>`` in the directory.

        :raises ValueError: ``run_id`` is not a run id (``rec.record.RUN_ID``)
        """
        # A load comes before any record, and an id like '../x' must not read outside the directory.
        if not RUN_ID.fullmatch(run_id):
            raise ValueError(f"'{run_id}' is not a run id -- use letters, digits, '-', '.', '_' or '~', not '.' or '..'")
        return self.directory / f"{run_id}.{self.fmt.extension}"

    def load(self, run_id: str) -> RunRecord | None:
        path = self.path(run_id)
        if not path.exists():
            return None
        return provenance.record(Dataset().parse(path, format=self.fmt), run_id, self.base)

    @contextmanager
    def edit(self, run_id: str):
        """Yield the run's ``RunRecord`` under an ``flock`` on ``<run_id>.lock``, and save it when the block exits.

        The file is replaced whole, through a temporary file; if the block raises, it is left as it was.
        """
        path = self.path(run_id)
        self.directory.mkdir(parents=True, exist_ok=True)
        # The lock file stays: removing it would let a waiting process lock a file no longer in the directory.
        with open(path.with_name(f"{run_id}.lock"), "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            record = self.load(run_id) or RunRecord(run_id)
            yield record
            temporary = path.with_name(f"{path.name}.tmp")
            temporary.write_text(provenance.serialize(provenance.dataset(record, self.base), self.fmt))
            temporary.replace(path)

    def run_ids(self, state: State | None = None) -> list[str]:
        suffix = f".{self.fmt.extension}"
        run_ids = sorted(path.name.removesuffix(suffix) for path in self.directory.glob(f"*{suffix}"))
        return [run_id for run_id in run_ids if state is None or self.load(run_id).state is state]

    def close(self) -> None:
        """Nothing stays open between writes."""
