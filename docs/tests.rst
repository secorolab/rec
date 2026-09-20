Tests
=====

Run the suite:

.. code-block:: shell

   pip install -e ".[dev]"
   pytest

The conformance test reads the shapes from a metamodels checkout, found beside
this repository or at ``REC_METAMODELS_DIR``. MariaDB
integration tests additionally need the optional driver, a running MariaDB
server, and a disposable database; without the driver they are skipped:

.. code-block:: shell

   pip install -e ".[mariadb,dev]"
   export REC_TEST_MARIADB_DATABASE=rec_test
   pytest tests/test_mariadb_observer.py

Coverage
--------

.. list-table::
   :header-rows: 1

   * - Area
     - Covered guarantee
     - Test module
   * - Run lifecycle
     - Queue, cancel, complete, fail and interrupt map to the OSLC
       (state, verdict) pair; a running run is cancelled in process and
       through its archive; the heartbeat does not race the run; metrics,
       host, trigger and starter are recorded.
     - ``tests/test_run_lifecycle.py``
   * - File observer
     - Writes PROV/rec JSON-LD that conforms to the metamodel shapes, mints
       run-scoped instance IRIs, keeps an injected run IRI on reopening, and
       hands back the file entities it mints.
     - ``tests/test_file_observer.py``
   * - MariaDB observer
     - Stores database-only and dual-backend runs under one run node with
       the archive location, numbers runs, and imports archives in
       ``prov:startedAtTime`` order with a cursor.
     - ``tests/test_mariadb_observer.py``

Not covered
-----------

The suite does not currently cover MariaDB connection failures or retries,
concurrent writers, or recovery from partial or corrupt archives.

Continuous integration
----------------------

GitHub Actions runs the full suite against MariaDB 11 on Python 3.12 and
Python 3.14, with the metamodels checked out at ``REC_METAMODELS_DIR``.
