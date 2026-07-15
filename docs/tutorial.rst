Tutorial
========

Install REC
-----------

Install the file observer:

.. code-block:: shell

   pip install -e .

Install MariaDB support when required:

.. code-block:: shell

   pip install -e ".[mariadb]"

Record to a file
----------------

``Run`` owns the canonical run ID. ``FileObserver`` creates and stores a
separate stable file ID in the graph.

.. code-block:: python

   from rec.observers import FileObserver
   from rec.run import Run


   class CalibrationRun(Run):
       def main(self):
           return "ok"


   run = CalibrationRun(
       observers=[FileObserver("runs/calibration/rec.jsonld")],
       run_id="calibration",
   )
   run.run()

REC represents lifecycle state with RDF types: ``rec:QueuedRun``,
``rec:RunningRun``, ``rec:CompletedRun``, ``rec:FailedRun``,
``rec:InterruptedRun``, and ``rec:CancelledRun``.

Record to a file and MariaDB
----------------------------

Configure the connection in ``.env``:

.. code-block:: ini

   MARIADB_USER=rec
   MARIADB_PASSWORD=secret
   MARIADB_HOST=localhost
   MARIADB_PORT=3306

Attach both observers. Do not pass the run ID to either observer; the file ID
is carried into MariaDB automatically.

.. code-block:: python

   from rec.observers import FileObserver, MariaDBObserver
   from rec.run import Run

   run = Run(
       observers=[
           FileObserver("runs/run-1/rec.jsonld"),
           MariaDBObserver(),
       ],
       run_id="run-1",
   )

Synchronise archived runs
-------------------------

Import file-only archives later without changing either ID:

.. code-block:: python

   from rec.observers import MariaDBObserver

   database = MariaDBObserver()
   database.sync_file("runs/run-1/rec.jsonld")
   database.sync_files("runs")

Bulk synchronisation uses ``prov:startedAtTime`` to order files and to apply
an optional start-time cursor.
