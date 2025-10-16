import sys
import os
import json

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
                """
                CREATE TABLE IF NOT EXISTS {} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    data1 JSON NOT NULL
                )
            """.format(
                    self.table
                )
            )
            print("Table '{}' created or already exists.".format(self.table))
        except mariadb.Error as e:
            print(f"Error creating table: {e}")
            self.conn.rollback()  # Rollback in case of DDL error

        self.cursor.execute("DESCRIBE logs;")
        for x in self.cursor:
            print(x)

    def write(self, data: dict):
        """
        Saves data to the MariaDB database
        :param data: A python dictionary to be saved as JSON in the DB
        :return:
        """
        template = """
        INSERT INTO {} (data1) VALUES ('{}');
        """.format(
            self.table, json.dumps(data)
        )
        print(template)
        try:
            self.cursor.execute(template)
            print(f"Inserted {self.cursor.rowcount} rows.")
            print(f"Last inserted ID: {self.cursor.lastrowid}")
        except mariadb.Error as e:
            print(f"Error: {e}")

    def update_run_data(self, run_id: int, data: dict):
        try:
            self.cursor.execute(
                "UPDATE {} SET data1 = ? WHERE id = ?".format(self.table),
                (
                    json.dumps(data),
                    run_id,
                ),
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
        try:
            self.cursor.execute(
                "SELECT data1 FROM logs WHERE id=?;".format(self.table),
                (run_id,),
            )
        except mariadb.Error as e:
            print(f"An error occurred: {e}")
            sys.exit(1)

        for x in self.cursor:
            print(json.loads(x[0]))

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
    db.write({"start": 6, "end": 6})
    print("Getting run")
    db.get_run(4)
    db.close()
