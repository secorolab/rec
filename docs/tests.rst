Tests
=====

Install the test runner and run the file-observer test:

.. code-block:: shell

   pip install pytest
   pytest

MariaDB integration tests require the optional backend, a running MariaDB
server, and a disposable database named by ``REC_TEST_MARIADB_DATABASE``:

.. code-block:: shell

   pip install -e ".[mariadb]" pytest
   export REC_TEST_MARIADB_DATABASE=rec_test
   pytest tests/test_mariadb_observer.py

The GitHub Actions workflow runs the full suite against MariaDB on Python 3.12
and Python 3.14.
