Tutorial
========

This tutorial records a robot calibration run, first as a JSON-LD archive and
then with MariaDB as a second backend. Install REC before starting; see
:doc:`install`.

Record a run to JSON-LD
-----------------------

Create a ``Run`` subclass and give it a ``FileObserver``. ``Run`` owns the
canonical run ID; do not pass that ID to the observer. The file observer
creates a separate stable file ID and writes it into the archive.

.. code-block:: python

   from rec.observers import FileObserver
   from rec.run import Run


   class CalibrationRun(Run):
       def main(self):
           return "calibrated"


   run = CalibrationRun(
       observers=[FileObserver("runs/calibration/rec.jsonld")],
       run_id="calibration",
   )
   result = run.run()

``run()`` marks the run as running, calls ``main()``, and records completion.
An exception records ``rec:FailedRun``; a keyboard interrupt records
``rec:InterruptedRun``. The resulting ``runs/calibration/rec.jsonld`` is a
portable JSON-LD provenance graph.

Add provenance and measurements
--------------------------------

Call REC's recording methods from ``main()``. Use compact ``rec:`` and
``prov:`` identifiers for the agent and activity. The resource and artefact
timestamps default to the current UTC time when omitted.

.. code-block:: python

   class CalibrationRun(Run):
       def main(self):
           self.add_agent("rec:agent/calibrator", "prov:SoftwareAgent")
           self.add_activity(
               "rec:activity/calibration",
               "rec:Calibration",
               associated_with="rec:agent/calibrator",
           )
           self.add_resource(
               "config/robot.yaml",
               usage_activity="rec:activity/calibration",
           )
           self.add_artefact(
               "results/calibration.json",
               gen_activity="rec:activity/calibration",
           )
           self.log_scalar("position-error", 0.02, step=1)
           return "calibrated"

The graph records a PROV agent, activity, qualified resource usage, artefact
generation, and a QUDT dimensionless metric. REC uses exactly one lifecycle
RDF type at a time: ``rec:QueuedRun``, ``rec:RunningRun``,
``rec:CompletedRun``, ``rec:FailedRun``, ``rec:InterruptedRun``, or
``rec:CancelledRun``.

Write the same run to MariaDB
-----------------------------

After configuring :doc:`MariaDB <install>`, attach both observers to the same
run. MariaDB stores the same JSON-LD graph and retains the file observer's ID,
so the archive and database row remain linked without passing duplicate IDs.

.. code-block:: python

   from rec.observers import FileObserver, MariaDBObserver


   run = CalibrationRun(
       observers=[
           FileObserver("runs/calibration/rec.jsonld"),
           MariaDBObserver(),
       ],
       run_id="calibration",
   )
   run.run()

Synchronise existing archives
-----------------------------

File-only archives can be imported later. ``sync_file()`` imports one archive;
``sync_files()`` recursively imports ``*.jsonld`` files, ordered by
``prov:startedAtTime``. Pass ``started_after`` to import only newer runs.

.. code-block:: python

   from datetime import UTC, datetime

   from rec.observers import MariaDBObserver


   database = MariaDBObserver()
   try:
       database.sync_file("runs/calibration/rec.jsonld")
       imported = database.sync_files(
           "runs",
           started_after=datetime(2026, 1, 1, tzinfo=UTC),
       )
   finally:
       database.close()

Synchronisation preserves the archive's ``rec:run-id`` and ``rec:file-id``.
The start time only determines import order and cursor filtering; it does not
replace either identity.
