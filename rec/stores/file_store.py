"""A store keeping each run's provenance bundle as a JSON-LD file in a directory."""

import fcntl
from contextlib import contextmanager
from pathlib import Path

from rdf_utils import DatasetFormat
from rdflib import Dataset

from rec.provenance import CONTEXT


class FileStore:
    def __init__(self, directory):
        """
        :param directory: Where the files go, ``<run_id>.ld.json`` each; created on first write
        """
        self.directory = Path(directory)

    def path(self, run_id: str) -> Path:
        """Return the run's file, ``<run_id>.ld.json`` in the directory.

        :raises ValueError: ``run_id`` would name a file outside the directory, e.g. ``../x``
        """
        path = self.directory / f"{run_id}.{DatasetFormat.JSON_LD.extension}"
        if path.parent != self.directory:
            raise ValueError(f"'{run_id}' names a file outside '{self.directory}' -- a run's file is in the store's directory")
        return path

    def load(self, run_id: str) -> Dataset | None:
        """Return the run's bundle as stored, or None if the run is not stored."""
        path = self.path(run_id)
        if not path.exists():
            return None
        return Dataset().parse(path, format=DatasetFormat.JSON_LD)

    @contextmanager
    def edit(self, run_id: str):
        """Yield the run's dataset under an ``flock`` on ``<run_id>.lock``, empty if the run is new, and save it when the block exits.

        The file is replaced whole, through a temporary file; if the block raises, it is left as it was.
        """
        path = self.path(run_id)
        self.directory.mkdir(parents=True, exist_ok=True)
        # The lock file stays: removing it would let a waiting process lock a file no longer in the directory.
        with open(path.with_name(f"{run_id}.lock"), "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            ds = Dataset().parse(path, format=DatasetFormat.JSON_LD) if path.exists() else Dataset()
            yield ds
            temporary = path.with_name(f"{path.name}.tmp")
            temporary.write_text(ds.serialize(format=DatasetFormat.JSON_LD, context=CONTEXT, indent=2))
            temporary.replace(path)
