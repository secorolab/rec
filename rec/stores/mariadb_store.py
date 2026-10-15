"""A store keeping runs in MariaDB tables: a row per run, and a row per item of each of its lists."""

import os
import threading
from contextlib import contextmanager
from dataclasses import asdict, fields, is_dataclass
from datetime import UTC, datetime
from enum import Enum
from importlib.resources import files
from types import UnionType
from typing import Any, get_args, get_origin, get_type_hints

import mariadb
from dotenv import load_dotenv

from rec import State, Verdict
from rec.record import KEYS, FileRef, Generation, Metric, RunRecord, Usage
from rec.stores import RunHandle, Store

# mariadb.sql follows RunRecord: a list field is a table, a dataclass field `<field>_<item>` run columns.
RECORD_FIELDS = get_type_hints(RunRecord)
TABLES = {name: get_args(hint)[0] for name, hint in RECORD_FIELDS.items() if get_origin(hint) is list}
PARTS = {
    name: get_args(hint)[0]
    for name, hint in RECORD_FIELDS.items()
    if get_origin(hint) is UnionType and is_dataclass(get_args(hint)[0])
}
# Optional columns of a unique key, kept as "" for None: MariaDB's UNIQUE treats NULLs as distinct.
KEY_TEXT = ("root", "activity")


class MariaDBStore(Store):
    def __init__(self, db_name: str = "logbook", prefix: str = "logs"):
        """
        Store in MariaDB tables, connecting with ``MARIADB_USER``, ``MARIADB_PASSWORD``,
        ``MARIADB_HOST`` and ``MARIADB_PORT`` from the environment or a ``.env`` file

        :param db_name: Name of the database
        :param prefix: Prefix of the table names: ``<prefix>_runs`` has a row per run, ``id`` in the
            order runs were added; ``<prefix>_metrics``, ``<prefix>_artefacts`` and the other lists
            have a row per item, ``run`` naming its run's ``id``
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
        with self._lock, self.conn.cursor(dictionary=True) as cursor:
            cursor.execute(f"SELECT * FROM {self.prefix}_runs WHERE run_id = ?", (run_id,))
            columns = cursor.fetchone()
            if columns is None:
                return None
            record = _row(RunRecord, columns)
            for kind, cls in TABLES.items():
                cursor.execute(f"SELECT * FROM {self.prefix}_{kind} WHERE run = ?", (columns["id"],))
                setattr(record, kind, [_row(cls, row) for row in cursor.fetchall()])
        return record

    @contextmanager
    def edit(self, run_id: str):
        """Yield a ``MariaDBRun`` in one transaction, committed when the block exits, rolled back if it raises.

        The run's row is locked with ``SELECT ... FOR UPDATE``, and inserted first if the run is new.
        """
        with self._lock:
            self.conn.begin()
            try:
                self.cursor.execute(f"SELECT id, state FROM {self.prefix}_runs WHERE run_id = ? FOR UPDATE", (run_id,))
                row = self.cursor.fetchone()
                if row is None:
                    new = RunRecord(run_id)
                    self.cursor.execute(
                        f"INSERT INTO {self.prefix}_runs (run_id, state, verdict) VALUES (?, ?, ?) RETURNING id, state",
                        (new.run_id, str(new.state), str(new.verdict)),
                    )
                    row = self.cursor.fetchone()
                yield MariaDBRun(self, run_id, row[0], State(row[1]))
            except BaseException:
                self.conn.rollback()
                raise
            self.conn.commit()

    def run_ids(self, state: State | None = None) -> list[str]:
        with self._lock:
            if state is None:
                self.cursor.execute(f"SELECT run_id FROM {self.prefix}_runs ORDER BY id")
            else:
                self.cursor.execute(f"SELECT run_id FROM {self.prefix}_runs WHERE state = ? ORDER BY id", (str(state),))
            return [row[0] for row in self.cursor.fetchall()]

    def close(self) -> None:
        self.cursor.close()
        self.conn.close()


class MariaDBRun(RunHandle):
    """A run's rows, locked by the transaction of the edit that yields it; each change is one statement."""

    def __init__(self, store: MariaDBStore, run_id: str, id: int, state: State):
        """
        :param store: The store whose transaction holds the run
        :param run_id: The run's id
        :param id: The run's ``id`` in ``<prefix>_runs``, which its list rows name as ``run``
        :param state: The run's state when the edit began
        """
        self.store = store
        self.run_id = run_id
        self.id = id
        self.state = state

    def update(self, **values):
        columns = {}
        for name, value in values.items():
            if name in PARTS:
                columns |= {f"{name}_{item.name}": getattr(value, item.name) if value else None for item in fields(PARTS[name])}
            else:
                columns[name] = value
        columns = _columns(columns)
        self.store.cursor.execute(
            f"UPDATE {self.store.prefix}_runs SET {', '.join(f'`{name}` = ?' for name in columns)} WHERE id = ?",
            (*columns.values(), self.id),
        )
        self.state = values.get("state", self.state)

    def add(self, kind: str, row, replace: bool = False):
        activity = row.activity if isinstance(row, (Usage, Generation)) else None
        if activity is not None:
            self.store.cursor.execute(f"SELECT 1 FROM {self.store.prefix}_activities WHERE run = ? AND id = ?", (self.id, activity))
            if self.store.cursor.fetchone() is None:
                raise ValueError(f"run '{self.run_id}' has no activity '{activity}' -- add it to the run first")
        values = asdict(row)
        values = _columns({**values.pop("file", {}), **values})
        if kind == "metrics":
            values["is_integer"] = isinstance(values["value"], int)
        names = ", ".join(f"`{name}`" for name in values)
        statement = f"INSERT INTO {self.store.prefix}_{kind} (run, {names}) VALUES (?{', ?' * len(values)})"
        if replace:
            statement += " ON DUPLICATE KEY UPDATE " + ", ".join(f"`{name}` = VALUE(`{name}`)" for name in values)
        try:
            self.store.cursor.execute(statement, (self.id, *values.values()))
        except mariadb.IntegrityError as error:
            raise ValueError(f"run '{self.run_id}' already has {KEYS[kind](row)} among its {kind} -- each is logged once") from error

    def add_metric(self, name: str, value: float, step: int | None = None, time: datetime | None = None):
        if step is None:
            self.store.cursor.execute(
                f"SELECT COALESCE(MAX(step) + 1, 0) FROM {self.store.prefix}_metrics WHERE run = ? AND name = ?",
                (self.id, name),
            )
            step = int(self.store.cursor.fetchone()[0])
        self.add("metrics", Metric(name, step, value, time), replace=True)


def _columns(values: dict) -> dict:
    """Values as columns hold them: times in UTC without a zone, enums as text, no NULL in a unique key."""
    columns = {}
    for name, value in values.items():
        if isinstance(value, datetime):
            value = value.astimezone(UTC).replace(tzinfo=None)
        elif isinstance(value, Enum):
            value = str(value)
        elif value is None and name in KEY_TEXT:
            value = ""
        columns[name] = value
    return columns


def _row[T](cls: type[T], columns: dict) -> T:
    """A row of the record, from the columns ``_columns`` wrote."""
    values: dict[str, Any] = {}
    for name, value in columns.items():
        if isinstance(value, datetime):
            value = value.replace(tzinfo=UTC)
        elif value == "" and name in KEY_TEXT:
            value = None
        if name != "run":
            values[name] = value
    if cls is RunRecord:
        # The table key; a list row's ``id`` is data, e.g. an agent's IRI.
        values.pop("id")
        for name, part in PARTS.items():
            kept = {item.name: values.pop(f"{name}_{item.name}") for item in fields(part)}
            values[name] = part(**kept) if any(value is not None for value in kept.values()) else None
        values["state"], values["verdict"] = State(values["state"]), Verdict(values["verdict"])
    if cls in (Usage, Generation):
        values["file"] = FileRef(**{item.name: values.pop(item.name) for item in fields(FileRef)})
    if cls is Metric:
        is_integer = values.pop("is_integer")
        values["value"] = int(values["value"]) if is_integer else values["value"]
    return cls(**values)
