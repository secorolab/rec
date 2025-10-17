import sys
import os
import json
import datetime as dt
from datetime import timedelta, datetime

import mariadb

from dotenv import load_dotenv

from rec.observers.base import BaseObserver


class MariaDBObserver(BaseObserver):
    def __init__(self, db_name="logbook", table="logs", **kwargs):
        """
        Observer in a run that writes to a MariaDB database

        :param db_name: Name of the table
        :param kwargs:
        """

        self.db_name = db_name
        self.table = table

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

        # Create the table if it doesn't exist
        try:
            self.cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
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

        # self.cursor.execute("DESCRIBE logs;")
        # for x in self.cursor:
        #     print(x)

    def add_run(
        self, status: str, start_t: datetime = None, queue_t: datetime = None
    ) -> int:
        """
        Saves a new run to the MariaDB database
        :param status: The status of the run being added (RUNNING or QUEUED)
        :param start_t: The start time of the run
        :param queue_t: The queue time of the run
        :return: The run_id
        """
        if start_t is not None:
            assert status == "RUNNING"
            run_info = {"start_time": start_t.isoformat()}
        elif queue_t is not None:
            assert status == "QUEUED"
            run_info = {"queue_time": queue_t.isoformat()}
        template = f"INSERT INTO {self.table} (status, run_info) VALUES (?, ?);"
        try:
            self.cursor.execute(template, (status, json.dumps(run_info)))
            print(f"Inserted {self.cursor.rowcount} rows.")
            print(f"Last inserted ID: {self.cursor.lastrowid}")
        except mariadb.Error as e:
            print(f"Error: {e}")

        return self.cursor.lastrowid

    def update_run_data(self, run_id: int, column: str, data):
        """
        Updates the data of a run in a particular column
        :param run_id:
        :param column:
        :param data:
        :return:
        """
        if column in ["status", "scenario_id"]:
            d = data
        else:
            d = json.dumps(data)

        try:
            self.cursor.execute(
                f"UPDATE {self.table} SET {column} = ? WHERE id = ?",
                (d, run_id),
            )
        except mariadb.Error as e:
            print(f"An error occurred: {e}")
            self.conn.rollback()  # Rollback in case of DDL error
            sys.exit(1)

    def get_run(self, run_id: int):
        """
        Queries the MariaDB database for a run's data
        :param run_id:
        :return:
        """
        cols = [
            "status",
            "scenario_id",
            "host_info",
            "sources",
            "repositories",
            "dependencies",
            "metrics",
            "agents",
            "resources",
            "artefacts",
            "run_info",
        ]
        try:
            self.cursor.execute(
                "SELECT {} FROM {} WHERE id=?;".format(", ".join(cols), self.table),
                (run_id,),
            )
        except mariadb.Error as e:
            print(f"An error occurred: {e}")
            sys.exit(1)

        for row in self.cursor:
            d = dict(status=row[0], scenario_id=row[1])
            for k, v in zip(cols[2:], row[2:]):
                if v is not None:
                    d[k] = json.loads(v)

        return d

    def log_run_heartbeat(self, run_id: int, beat_time: datetime, result):
        pass

    def log_cancelled_run(self, run_id: int, cancelled_time: datetime):
        pass

    def log_queued_run(self, run_id: int, queued_time: datetime):
        pass

    def log_started_run(
        self, run_id: int, started_time: datetime, trigger=None, starter=None
    ):
        pass

    def log_completed_run(self, run_id: int, completed_time: datetime):
        pass

    def log_interrupted_run(self, run_id: int, elapsed_time: timedelta):
        pass

    def log_failed_run(self, run_id: int, elapsed_time: timedelta):
        pass

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
    run_id = db.add_run("RUNNING", datetime.now(dt.UTC))
    db.update_run_data(run_id, "scenario_id", "db-test-01")
    db.update_run_data(
        run_id, "agents", {"agent_id": "robot-01", "agent_type": "SoftwareAgent"}
    )
    db.update_run_data(run_id, "status", "COMPLETED")
    print("Getting run info")
    info = db.get_run(run_id)
    print(info)
    db.close()
