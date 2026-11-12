"""A store keeping runs in MariaDB: a row per run, holding its provenance bundle as JSON-LD."""

import os
import re
import threading
from contextlib import contextmanager

import mariadb
from dotenv import load_dotenv
from rdf_utils import DatasetFormat
from rdflib import Dataset

from rec.provenance import CONTEXT


class MariaDBStore:
    def __init__(self, db_name: str = "logbook", prefix: str = "logs"):
        """
        Store in MariaDB tables, connecting with ``MARIADB_USER``, ``MARIADB_PASSWORD``,
        ``MARIADB_HOST`` and ``MARIADB_PORT`` from the environment or a ``.env`` file

        :param db_name: Name of the database
        :param prefix: Prefix of the table name: ``<prefix>_runs`` has a row per run, ``id`` in the
            order runs were added and ``bundle`` its provenance bundle as JSON-LD
        """
        # The prefix is part of a table name, which SQL cannot take as a bound parameter.
        if not re.fullmatch(r"[A-Za-z0-9_]{1,59}", prefix):
            raise ValueError(f"'{prefix}' is not a table prefix -- use up to 59 letters, digits or '_'")
        load_dotenv()
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
        # Binary ASCII: a run id is case-sensitive, and the default collation would fold `Pick-1` into `pick-1`.
        self.cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {prefix}_runs ("
            "id INT AUTO_INCREMENT PRIMARY KEY, "
            "run_id VARCHAR(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE, "
            "bundle LONGTEXT)"
        )

    def load(self, run_id: str) -> Dataset | None:
        """Return the run's bundle as stored, or None if the run is not stored."""
        with self._lock:
            self.cursor.execute(f"SELECT bundle FROM {self.prefix}_runs WHERE run_id = ?", (run_id,))
            row = self.cursor.fetchone()
        if row is None or row[0] is None:
            return None
        return Dataset().parse(data=row[0], format=DatasetFormat.JSON_LD)

    @contextmanager
    def edit(self, run_id: str):
        """Yield the run's dataset in one transaction, empty if the run is new, and save it when the block exits; roll back if it raises."""
        with self._lock:
            self.conn.begin()
            try:
                # Locks the run's row, inserting it if new. A SELECT ... FOR UPDATE of a missing row takes
                # only a gap lock, which two edits of new runs both get, and then deadlock on their inserts.
                self.cursor.execute(
                    f"INSERT INTO {self.prefix}_runs (run_id) VALUES (?) ON DUPLICATE KEY UPDATE run_id = run_id", (run_id,)
                )
                self.cursor.execute(f"SELECT bundle FROM {self.prefix}_runs WHERE run_id = ?", (run_id,))
                bundle = self.cursor.fetchone()[0]
                ds = Dataset().parse(data=bundle, format=DatasetFormat.JSON_LD) if bundle else Dataset()
                yield ds
                self.cursor.execute(
                    f"UPDATE {self.prefix}_runs SET bundle = ? WHERE run_id = ?",
                    (ds.serialize(format=DatasetFormat.JSON_LD, context=CONTEXT), run_id),
                )
            except BaseException:
                self.conn.rollback()
                raise
            self.conn.commit()

    def close(self) -> None:
        self.cursor.close()
        self.conn.close()
