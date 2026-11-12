"""A store keeping runs in MariaDB: a row per run, holding its provenance bundle."""

import os
import threading
from contextlib import contextmanager
from importlib.resources import files

import mariadb
from dotenv import load_dotenv
from rdf_utils import DatasetFormat
from rdflib import Dataset

from rec import State, provenance
from rec.record import RunRecord
from rec.stores import Store


class MariaDBStore(Store):
    def __init__(self, db_name: str = "logbook", prefix: str = "logs"):
        """
        Store in MariaDB tables, connecting with ``MARIADB_USER``, ``MARIADB_PASSWORD``,
        ``MARIADB_HOST`` and ``MARIADB_PORT`` from the environment or a ``.env`` file

        :param db_name: Name of the database
        :param prefix: Prefix of the table name: ``<prefix>_runs`` has a row per run, ``id`` in the
            order runs were added and ``bundle`` its provenance bundle as JSON-LD
        """
        load_dotenv()
        self.db_name = db_name
        self.prefix = prefix
        self.conn = mariadb.connect(
            user=os.getenv("MARIADB_USER"),
            password=os.getenv("MARIADB_PASSWORD"),
            host=os.getenv("MARIADB_HOST"),
            port=int(os.getenv("MARIADB_PORT")),
            database=db_name,
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        # One connection for every thread using the store, so one statement or transaction at a time.
        self._lock = threading.RLock()
        schema = files("rec.stores").joinpath("mariadb.sql").read_text().format(prefix=prefix)
        for statement in schema.split(";"):
            if statement.strip():
                self.cursor.execute(statement)

    def load(self, run_id: str) -> RunRecord | None:
        with self._lock:
            self.cursor.execute(f"SELECT bundle FROM {self.prefix}_runs WHERE run_id = ?", (run_id,))
            row = self.cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return provenance.record(Dataset().parse(data=row[0], format=DatasetFormat.JSON_LD), run_id)

    @contextmanager
    def edit(self, run_id: str):
        """Yield the run's ``RunRecord`` in one transaction, and save it when the block exits; roll back if it raises.

        The run's row is locked with ``SELECT ... FOR UPDATE``, and inserted first if the run is new.
        """
        with self._lock:
            self.conn.begin()
            try:
                self.cursor.execute(f"SELECT id, bundle FROM {self.prefix}_runs WHERE run_id = ? FOR UPDATE", (run_id,))
                row = self.cursor.fetchone()
                if row is None:
                    record = RunRecord(run_id)
                    # Inserted before the block runs, so a concurrent edit of the same new run waits on its lock.
                    self.cursor.execute(f"INSERT INTO {self.prefix}_runs (run_id) VALUES (?) RETURNING id, bundle", (run_id,))
                    row = self.cursor.fetchone()
                else:
                    record = provenance.record(Dataset().parse(data=row[1], format=DatasetFormat.JSON_LD), run_id)
                yield record
                self.cursor.execute(
                    f"UPDATE {self.prefix}_runs SET bundle = ? WHERE id = ?",
                    (provenance.serialize(provenance.dataset(record), DatasetFormat.JSON_LD), row[0]),
                )
            except BaseException:
                self.conn.rollback()
                raise
            self.conn.commit()

    def run_ids(self, state: State | None = None) -> list[str]:
        with self._lock:
            self.cursor.execute(f"SELECT run_id FROM {self.prefix}_runs ORDER BY id")
            run_ids = [row[0] for row in self.cursor.fetchall()]
        return [run_id for run_id in run_ids if state is None or self.load(run_id).state is state]

    def close(self) -> None:
        self.cursor.close()
        self.conn.close()
