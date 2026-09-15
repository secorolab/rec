Tutorial
========

This tutorial records a robot calibration run as JSON-LD, then shows two
independent alternatives: recording the next run in both backends and
synchronising an existing archive. Install REC first; see :doc:`install`.

Record a run to JSON-LD
-----------------------

``Run`` owns the canonical run ID. ``FileObserver`` creates a separate stable
file ID and writes it into the archive; do not pass the run ID to the observer.

.. code-block:: python

   from rec import Run
   from rec.observers import FileObserver


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


   run = CalibrationRun(
       observers=[FileObserver("runs/calibration/rec.jsonld")],
       run_id="calibration",
   )
   result = run.run()

``run()`` marks the run as running, calls ``main()``, and records completion.
An exception records ``rec:FailedRun``; a keyboard interrupt records
``rec:InterruptedRun``. The archive contains a PROV agent, activity,
qualified resource usage, artefact generation, and a QUDT dimensionless
metric. Resource and artefact timestamps default to the current UTC time.

REC stores exactly one lifecycle RDF type at a time:
``rec:QueuedRun``, ``rec:RunningRun``, ``rec:CompletedRun``,
``rec:FailedRun``, ``rec:InterruptedRun``, or ``rec:CancelledRun``.

Record the next run to a file and MariaDB
-----------------------------------------

After configuring :doc:`MariaDB <install>`, create the next run with both
observers. This is an alternative to the file-only construction above, not a
second write of the completed ``calibration`` run. MariaDB stores the same
graph and retains the file observer's ID.

.. code-block:: python

   from rec.observers import FileObserver, MariaDBObserver


   run = CalibrationRun(
       observers=[
           FileObserver("runs/calibration-db/rec.jsonld"),
           MariaDBObserver(),
       ],
       run_id="calibration-db",
   )
   run.run()

Synchronise existing archives
-----------------------------

Use one of the following approaches for file-only archives. Both preserve the
archive's ``rec:run-id`` and ``rec:file-id``.

Import one archive:

.. code-block:: python

   from rec.observers import MariaDBObserver


   database = MariaDBObserver()
   try:
       database.sync_file("runs/calibration/rec.jsonld")
   finally:
       database.close()

Import a directory in ``prov:startedAtTime`` order. ``started_after`` is an
optional cursor for importing only newer runs:

.. code-block:: python

   from datetime import UTC, datetime

   from rec.observers import MariaDBObserver


   database = MariaDBObserver()
   try:
       imported = database.sync_files(
           "runs",
           started_after=datetime(2026, 1, 1, tzinfo=UTC),
       )
   finally:
       database.close()
