Tests
=====

Run the file-backed suite:

.. code-block:: shell

   pip install pytest
   pytest

MariaDB integration tests additionally need the optional driver, a running
MariaDB server, and a disposable database:

.. code-block:: shell

   pip install -e ".[mariadb]" pytest
   export REC_TEST_MARIADB_DATABASE=rec_test
   pytest tests/test_mariadb_observer.py

Coverage
--------

.. list-table::
   :header-rows: 1

   * - Area
     - Covered guarantee
     - Test module
   * - File observer
     - Writes REC/PROV JSON-LD, records lifecycle as an RDF type, and
       preserves ``rec:file-id`` when reopening the archive.
     - ``tests/test_file_observer.py``
   * - MariaDB observer
     - Stores a completed database-only run without inventing a file identity.
     - ``tests/test_mariadb_observer.py``
   * - Dual backend
     - File and MariaDB observers retain the same file ID and archive path.
     - ``tests/test_mariadb_observer.py``
   * - Archive synchronisation
     - Preserves run and file identities; imports in ``prov:startedAtTime``
       order and filters with a start-time cursor.
     - ``tests/test_mariadb_observer.py``

Not covered
-----------

The suite does not currently cover MariaDB connection failures or retries,
concurrent writers, SHACL validation, or recovery from partial or corrupt
archives.

Continuous integration
----------------------

GitHub Actions runs the full suite against MariaDB 11 on Python 3.12 and
Python 3.14.
