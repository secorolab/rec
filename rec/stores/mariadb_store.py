"""A store keeping each run as one row of a MariaDB table."""

import os
import threading
from contextlib import contextmanager

import mariadb
from dotenv import load_dotenv

from rec import State
from rec.record import RunRecord, from_json, to_json


class MariaDBStore:
    def __init__(self, db_name: str = "logbook", table: str = "logs"):
        """
        Store in a MariaDB table, connecting with ``MARIADB_USER``, ``MARIADB_PASSWORD``,
        ``MARIADB_HOST`` and ``MARIADB_PORT`` from the environment or a ``.env`` file

        :param db_name: Name of the database
        :param table: Name of the table, one row per run: ``id`` in the order runs were added,
            ``run_id``, ``state`` and ``verdict`` to query by, and ``record``
        """
        load_dotenv()
        self.db_name = db_name
        self.table = table
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
        self.cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(255) NOT NULL UNIQUE,
                state VARCHAR(20) NOT NULL,
                verdict VARCHAR(20) NOT NULL,
                record LONGTEXT NOT NULL
            )
            """
        )

    def load(self, run_id: str) -> RunRecord | None:
        with self._lock:
            self.cursor.execute(f"SELECT record FROM {self.table} WHERE run_id = ?", (run_id,))
            row = self.cursor.fetchone()
        return from_json(row[0]) if row else None

    @contextmanager
    def edit(self, run_id: str):
        with self._lock:
            self.conn.begin()
            try:
                self.cursor.execute(f"SELECT record FROM {self.table} WHERE run_id = ? FOR UPDATE", (run_id,))
                row = self.cursor.fetchone()
                record = from_json(row[0]) if row else RunRecord(run_id)
                yield record
                values = (str(record.state), str(record.verdict), to_json(record), run_id)
                # Not INSERT ... ON DUPLICATE KEY UPDATE: InnoDB spends an `id` on every such update.
                if row:
                    self.cursor.execute(f"UPDATE {self.table} SET state = ?, verdict = ?, record = ? WHERE run_id = ?", values)
                else:
                    self.cursor.execute(f"INSERT INTO {self.table} (state, verdict, record, run_id) VALUES (?, ?, ?, ?)", values)
            except BaseException:
                self.conn.rollback()
                raise
            self.conn.commit()

    def run_ids(self, state: State | None = None) -> list[str]:
        with self._lock:
            if state is None:
                self.cursor.execute(f"SELECT run_id FROM {self.table} ORDER BY id")
            else:
                self.cursor.execute(f"SELECT run_id FROM {self.table} WHERE state = ? ORDER BY id", (str(state),))
            return [row[0] for row in self.cursor.fetchall()]

    def close(self) -> None:
        self.cursor.close()
        self.conn.close()
