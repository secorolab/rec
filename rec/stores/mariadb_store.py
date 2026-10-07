"""A store keeping each run as one row of a MariaDB table."""

import json
import logging
import os
from datetime import datetime

import mariadb
from dotenv import load_dotenv

from rec import State, Verdict
from rec.record import RunRecord, from_json, to_json

logger = logging.getLogger(__name__)

# The previous release's `status`, as an OSLC state and verdict.
STATUS_LIFECYCLES = {
    "QUEUED": (State.QUEUED, Verdict.UNAVAILABLE),
    "RUNNING": (State.IN_PROGRESS, Verdict.UNAVAILABLE),
    "CANCELLED": (State.CANCELED, Verdict.UNAVAILABLE),
    "COMPLETED": (State.COMPLETE, Verdict.PASSED),
    "FAILED": (State.COMPLETE, Verdict.FAILED),
    "INTERRUPTED": (State.COMPLETE, Verdict.ERROR),
    "TIMED_OUT": (State.COMPLETE, Verdict.ERROR),
}


class MariaDBStore:
    def __init__(self, db_name: str = "logbook", table: str = "logs"):
        """
        Store in a MariaDB table, connecting with ``MARIADB_USER``, ``MARIADB_PASSWORD``,
        ``MARIADB_HOST`` and ``MARIADB_PORT`` from the environment or a ``.env`` file

        :param db_name: Name of the database
        :param table: Name of the table, one row per run: ``id`` a short number to display,
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
        self.cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?",
            (db_name, table),
        )
        if "run_id" not in {row[0] for row in self.cursor}:
            self._migrate()

    def _migrate(self):
        """A table from the previous release gets the run's columns; its own columns stay as they are."""
        self.cursor.execute(
            f"ALTER TABLE {self.table} ADD COLUMN run_id VARCHAR(255), ADD COLUMN state VARCHAR(20), "
            "ADD COLUMN verdict VARCHAR(20), ADD COLUMN record LONGTEXT, MODIFY status VARCHAR(20) NULL"
        )
        self.cursor.execute(f"SELECT id, status, run_info FROM {self.table}")
        for number, status, run_info in self.cursor.fetchall():
            # All the previous release recorded besides a run's status: when it was queued or started.
            info = json.loads(run_info) if run_info else {}
            state, verdict = STATUS_LIFECYCLES.get(status, (State.COMPLETE, Verdict.UNAVAILABLE))
            record = RunRecord(
                str(number),
                state,
                verdict,
                queued_time=datetime.fromisoformat(info["queue_time"]) if info.get("queue_time") else None,
                start_time=datetime.fromisoformat(info["start_time"]) if info.get("start_time") else None,
            )
            self.cursor.execute(
                f"UPDATE {self.table} SET run_id = ?, state = ?, verdict = ?, record = ? WHERE id = ?",
                (record.run_id, str(state), str(verdict), to_json(record), number),
            )
        self.cursor.execute(
            f"ALTER TABLE {self.table} MODIFY run_id VARCHAR(255) NOT NULL, ADD UNIQUE (run_id), "
            "MODIFY state VARCHAR(20) NOT NULL, MODIFY verdict VARCHAR(20) NOT NULL, MODIFY record LONGTEXT NOT NULL"
        )
        logger.info("Migrated table %s: run_id, state, verdict and record added", self.table)

    def load(self, run_id: str) -> RunRecord | None:
        self.cursor.execute(f"SELECT record FROM {self.table} WHERE run_id = ?", (run_id,))
        row = self.cursor.fetchone()
        return from_json(row[0]) if row else None

    def save(self, record: RunRecord) -> None:
        values = (str(record.state), str(record.verdict), to_json(record), record.run_id)
        # Not INSERT ... ON DUPLICATE KEY UPDATE: InnoDB spends an `id` on every such update.
        if self.number(record.run_id) is None:
            self.cursor.execute(f"INSERT INTO {self.table} (state, verdict, record, run_id) VALUES (?, ?, ?, ?)", values)
        else:
            self.cursor.execute(f"UPDATE {self.table} SET state = ?, verdict = ?, record = ? WHERE run_id = ?", values)

    def run_ids(self, state: State | None = None) -> list[str]:
        if state is None:
            self.cursor.execute(f"SELECT run_id FROM {self.table} ORDER BY id")
        else:
            self.cursor.execute(f"SELECT run_id FROM {self.table} WHERE state = ? ORDER BY id", (str(state),))
        return [row[0] for row in self.cursor.fetchall()]

    def number(self, run_id: str) -> int | None:
        """The short number the table gave the run, None for a run it does not hold."""
        self.cursor.execute(f"SELECT id FROM {self.table} WHERE run_id = ?", (run_id,))
        row = self.cursor.fetchone()
        return row[0] if row else None

    def close(self) -> None:
        self.cursor.close()
        self.conn.close()
