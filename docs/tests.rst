Tests
=====

The default test suite verifies file-backed recording. It creates a completed
run and checks that the JSON-LD graph contains the expected PROV activity,
REC lifecycle type, stable file ID, qualified usage and generation, and no
execution-context, observation, or BDD vocabulary.

Install the test runner and run it:

.. code-block:: shell

   pip install pytest
   pytest

MariaDB integration tests require the optional backend, a running MariaDB
server, and a disposable database named by ``REC_TEST_MARIADB_DATABASE``:

.. code-block:: shell

   pip install -e ".[mariadb]" pytest
   export REC_TEST_MARIADB_DATABASE=rec_test
   pytest tests/test_mariadb_observer.py

They verify four backend cases:

* MariaDB-only recording stores a completed run without a file ID.
* File and MariaDB observers used together retain the same file ID and archive
  path in MariaDB.
* Synchronising one archive preserves its run and file identities.
* Bulk synchronisation orders archives by ``prov:startedAtTime`` and honours a
  start-time cursor.

The GitHub Actions workflow runs the full suite against MariaDB on Python 3.12
and Python 3.14.
