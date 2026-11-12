"""A store keeping runs in MariaDB tables: a row per run, and a row per package it used or was recorded by."""

import os
import threading
from contextlib import contextmanager
from dataclasses import fields
from datetime import UTC, datetime
from enum import Enum
from importlib.resources import files

import mariadb
from dotenv import load_dotenv
from rdf_utils.models.prov import PackageInfo
from rdflib import URIRef

from rec import State, Verdict
from rec.record import Host, RunRecord
from rec.stores import RunHandle, Store

# The columns of `<prefix>_packages` besides `run` and `recorder`: what `load_pkg_prov` records.
PACKAGE_COLUMNS = ("purl", "name", "version", "revision", "repository", "dirty")
TABLES = ("packages", "runs")


class MariaDBStore(Store):
    def __init__(self, db_name: str = "logbook", prefix: str = "logs"):
        """
        Store in MariaDB tables, connecting with ``MARIADB_USER``, ``MARIADB_PASSWORD``,
        ``MARIADB_HOST`` and ``MARIADB_PORT`` from the environment or a ``.env`` file

        :param db_name: Name of the database
        :param prefix: Prefix of the table names: ``<prefix>_runs`` has a row per run, ``id`` in the
            order runs were added; ``<prefix>_packages`` has a row per package, ``run`` naming its
            run's ``id`` and ``recorder`` set for the rec that recorded it
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
            cursor.execute(f"SELECT * FROM {self.prefix}_packages WHERE run = ?", (columns["id"],))
            packages = cursor.fetchall()
        host = {item.name: columns.pop(f"host_{item.name}") for item in fields(Host)}
        times = {name: columns[name].replace(tzinfo=UTC) for name in ("queued_time", "start_time", "end_time", "heartbeat_time") if columns[name]}
        by_role = {True: [], False: []}
        for row in packages:
            values = {name: row[name] for name in PACKAGE_COLUMNS}
            by_role[bool(row["recorder"])].append(PackageInfo(**values | {"purl": URIRef(values["purl"]), "dirty": bool(values["dirty"])}))
        return RunRecord(
            run_id=run_id,
            bundle=URIRef(columns["bundle"]) if columns["bundle"] else None,
            state=State(columns["state"]),
            verdict=Verdict(columns["verdict"]),
            fail_trace=columns["fail_trace"],
            recorder=next(iter(by_role[True]), None),
            host=Host(**host) if any(value is not None for value in host.values()) else None,
            packages=by_role[False],
            **times,
        )

    @contextmanager
    def edit(self, run_id: str):
        """Yield a ``MariaDBRun`` in one transaction, committed when the block exits, rolled back if it raises.

        The run's row is locked with ``SELECT ... FOR UPDATE``, and inserted first if the run is new.
        """
        with self._lock:
            self.conn.begin()
            try:
                self.cursor.execute(f"SELECT id, state, bundle FROM {self.prefix}_runs WHERE run_id = ? FOR UPDATE", (run_id,))
                row = self.cursor.fetchone()
                if row is None:
                    new = RunRecord(run_id)
                    self.cursor.execute(
                        f"INSERT INTO {self.prefix}_runs (run_id, state, verdict) VALUES (?, ?, ?) RETURNING id, state, bundle",
                        (new.run_id, str(new.state), str(new.verdict)),
                    )
                    row = self.cursor.fetchone()
                yield MariaDBRun(self, run_id, row[0], State(row[1]), URIRef(row[2]) if row[2] else None)
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

    def __init__(self, store: MariaDBStore, run_id: str, id: int, state: State, bundle: URIRef | None):
        """
        :param store: The store whose transaction holds the run
        :param run_id: The run's id
        :param id: The run's ``id`` in ``<prefix>_runs``, which its package rows name as ``run``
        :param state: The run's state when the edit began
        :param bundle: The run's bundle when the edit began
        """
        self.store = store
        self.run_id = run_id
        self.id = id
        self.state = state
        self.bundle = bundle

    def update(self, **values):
        if "recorder" in values:
            self._insert_package(values.pop("recorder"), recorder=True)
        columns = {}
        for name, value in values.items():
            if name == "host":
                columns |= {f"host_{item.name}": getattr(value, item.name) if value else None for item in fields(Host)}
            elif isinstance(value, datetime):
                columns[name] = value.astimezone(UTC).replace(tzinfo=None)
            elif isinstance(value, (Enum, URIRef)):
                columns[name] = str(value)
            else:
                columns[name] = value
        if columns:
            self.store.cursor.execute(
                f"UPDATE {self.store.prefix}_runs SET {', '.join(f'`{name}` = ?' for name in columns)} WHERE id = ?",
                (*columns.values(), self.id),
            )
        self.state = values.get("state", self.state)
        self.bundle = values.get("bundle", self.bundle)

    def add_package(self, package: PackageInfo):
        try:
            self._insert_package(package, recorder=False)
        except mariadb.IntegrityError as error:
            raise ValueError(f"run '{self.run_id}' already has package '{package.purl}' -- each is logged once") from error

    def _insert_package(self, package: PackageInfo, recorder: bool):
        values = [str(getattr(package, name)) if name == "purl" else getattr(package, name) for name in PACKAGE_COLUMNS]
        self.store.cursor.execute(
            f"INSERT INTO {self.store.prefix}_packages (run, {', '.join(f'`{name}`' for name in PACKAGE_COLUMNS)}, recorder)"
            f" VALUES (?{', ?' * len(PACKAGE_COLUMNS)}, ?)",
            (self.id, *values, recorder),
        )
