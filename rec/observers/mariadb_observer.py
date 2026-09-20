import sys
import os
import json
import datetime as dt
from datetime import datetime

import mariadb

from dotenv import load_dotenv

from rec.observers.base import BaseObserver

JSON_COLUMNS = (
    "host_info",
    "sources",
    "repositories",
    "dependencies",
    "metrics",
    "agents",
    "resources",
    "artefacts",
    "run_info",
    "data",
)


class MariaDBObserver(BaseObserver):
    def __init__(self, db_name="logbook", table="logs", base=None, **kwargs):
        """
        Observer in a run that writes to a MariaDB database

        :param db_name: Name of the database
        :param table: Name of the table, one row per run
        :param base: IRI base of the run nodes, ``BaseObserver.base`` by default
        :param kwargs:
        """

        super().__init__()
        self.db_name = db_name
        self.table = table
        if base is not None:
            self.base = base

        load_dotenv()

        # Connect to MariaDB Platform
        try:
            self.conn = mariadb.connect(
                user=os.getenv("MARIADB_USER"),
                password=os.getenv("MARIADB_PASSWORD"),
                host=os.getenv("MARIADB_HOST"),
                port=int(os.getenv("MARIADB_PORT")),
                database=db_name,
            )
            self.conn.autocommit = True  # optional for simplicity
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

        # Get Cursor
        self.cursor = self.conn.cursor()
        print(f"Connected to MariaDB Platform: {self.db_name}")

        # Create the table if it doesn't exist. `id` is a short number to display and sort by;
        # `run_id` is the run's identity, the same in every store.
        try:
            self.cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    run_id VARCHAR(255) NOT NULL UNIQUE,
                    status VARCHAR(20) NOT NULL,
                    scenario_id VARCHAR(20),
                    host_info JSON,
                    sources JSON,
                    repositories JSON,
                    dependencies JSON,
                    metrics JSON,
                    agents JSON,
                    resources JSON,
                    artefacts JSON,
                    run_info JSON,
                    data JSON
                )
            """
            )
            print(f"Table '{self.table}' created or already exists.")
        except mariadb.Error as e:
            print(f"Error creating table: {e}")
            self.conn.rollback()  # Rollback in case of DDL error

    def add_run(self, run_id: str, status: str) -> int:
        """
        Saves a new run to the MariaDB database
        :param run_id: The run's identity
        :param status: The status of the run being added (RUNNING or QUEUED)
        :return: The short number the database assigned
        """
        template = f"INSERT INTO {self.table} (run_id, status) VALUES (?, ?);"
        try:
            self.cursor.execute(template, (run_id, status))
        except mariadb.Error as e:
            print(f"Error: {e}")

        return self.cursor.lastrowid

    def update_run_data(self, run_id: str, column: str, data):
        """
        Updates the data of a run in a particular column; a run not in the table yet is added
        :param run_id:
        :param column:
        :param data:
        :return:
        """
        if column in JSON_COLUMNS:
            d = json.dumps(data)
        else:
            d = data

        try:
            with self._lock:
                if self.db_id(run_id) is None:
                    self.add_run(run_id, data if column == "status" else "QUEUED")
                self.cursor.execute(
                    f"UPDATE {self.table} SET {column} = ? WHERE run_id = ?",
                    (d, run_id),
                )
        except mariadb.Error as e:
            print(f"An error occurred: {e}")
            self.conn.rollback()  # Rollback in case of DDL error
            sys.exit(1)

    def db_id(self, run_id: str):
        """The short number the database gave a run, or None when it holds no such run"""
        self.cursor.execute(f"SELECT id FROM {self.table} WHERE run_id=?;", (run_id,))
        row = self.cursor.fetchone()
        return row[0] if row else None

    def get_run(self, run_id: str):
        """
        Queries the MariaDB database for a run's data
        :param run_id:
        :return:
        """
        cols = ["id", "status", "scenario_id", *JSON_COLUMNS]
        try:
            self.cursor.execute(
                "SELECT {} FROM {} WHERE run_id=?;".format(", ".join(cols), self.table),
                (run_id,),
            )
        except mariadb.Error as e:
            print(f"An error occurred: {e}")
            sys.exit(1)

        d = {}
        for row in self.cursor:
            d = dict(id=row[0], status=row[1], scenario_id=row[2])
            for k, v in zip(cols[3:], row[3:]):
                if v is not None:
                    d[k] = json.loads(v)

        return d

    def query_active_run(self):
        try:
            self.cursor.execute(
                f"SELECT run_id, status FROM {self.table} WHERE status ='RUNNING'"
            )
        except mariadb.Error as e:
            print(f"An error occurred: {e}")
            sys.exit(1)

        for row in self.cursor:
            return row[0]

    def close(self):
        """
        This method must be called when the run is over to close the connection to the DB
        :return:
        """
        self.cursor.close()
        # self.conn.close()


if __name__ == "__main__":
    db = MariaDBObserver()
    print("Writing to the DB dummy data")
    run_id = "db-test-01"
    db.log_started_run(run_id, datetime.now(dt.UTC))
    info = db.get_run(run_id)
    print(info)

    db.update_run_data(run_id, "scenario_id", "db-test-01")
    db.add_agent(run_id, "https://example.org/agent/robot-01", "SoftwareAgent", name="robot-01")
    db.query_active_run()
    db.log_completed_run(run_id, datetime.now(dt.UTC))

    print("Getting run info")
    info = db.get_run(run_id)
    print(info)
    print(json.dumps(db.document(run_id), indent=2))

    db.close()
