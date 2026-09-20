Tutorial
========

This tutorial records a robot calibration run as JSON-LD, then shows two
independent alternatives: recording the next run in both backends and
synchronising an existing archive. Install REC first; see :doc:`install`.

Record a run to JSON-LD
-----------------------

``Run`` owns the run ID. The run's IRI ends in it, and reopening the archive
continues that run rather than starting a second one.

.. code-block:: python

   from rec import Run
   from rec.observers import FileObserver

   CALIBRATOR = "https://example.org/agent/calibrator"
   CALIBRATION = "https://example.org/activity/calibration"


   class CalibrationRun(Run):
       def main(self):
           self.add_agent(CALIBRATOR, "prov:SoftwareAgent", name="calibrator")
           self.add_software("rdflib", version="7.7.0")
           self.add_resource("config/robot.yaml")
           self.add_activity(CALIBRATION, "prov:Activity", associated_with=CALIBRATOR)
           self.add_resource("config/robot.yaml", used_by=CALIBRATION)
           self.add_artefact("results/calibration.json", generated_by=CALIBRATION)
           self.log_scalar("position-error", 0.02, step=1)
           return "calibrated"


   run = CalibrationRun(
       observers=[FileObserver("runs/calibration/rec.ld.json")],
       run_id="calibration",
   )
   result = run.run()

``run()`` marks the run as in progress, calls ``main()``, and records completion.
An exception records the verdict ``failed``; a keyboard interrupt records
``error``. The archive contains a PROV agent, a software package the run ran
with, an activity, qualified resource usage, artefact generation, and a QUDT
dimensionless metric. A resource or artefact with no activity named belongs to
the run itself; timestamps default to the current UTC time.

The run is a ``prov-ext:Execution`` whose lifecycle is an OSLC Automation
``oslc_auto:state`` (``queued``, ``inProgress``, ``complete``, ``canceled``)
and, once complete, an ``oslc_auto:verdict`` (``passed``, ``failed``,
``error``). Both are single-valued and replaced as the run moves on.

The metamodel asks every execution to name the agent that ran it
(``add_agent`` with a ``name`` for a software agent, or ``add_software``) and
what it used (``add_resource``); a run that records neither still writes its
archive but does not validate. Agents, activities and trigger entities are full IRIs of the
caller's own; ``prov:`` is the one shorthand, for PROV types.

Record the next run to a file and MariaDB
-----------------------------------------

After configuring :doc:`MariaDB <install>`, create the next run with both
observers. This is an alternative to the file-only construction above, not a
second write of the completed ``calibration`` run. MariaDB stores the same
graph under the same run node.

.. code-block:: python

   from rec.observers import FileObserver, MariaDBObserver


   run = CalibrationRun(
       observers=[
           FileObserver("runs/calibration-db/rec.ld.json"),
           MariaDBObserver(),
       ],
       run_id="calibration-db",
   )
   run.run()

Synchronise existing archives
-----------------------------

Use one of the following approaches for file-only archives. Both preserve the
archive's run IRI, whose last segment is the run id.

Import one archive:

.. code-block:: python

   from rec.observers import MariaDBObserver


   database = MariaDBObserver()
   try:
       database.sync_file("runs/calibration/rec.ld.json")
   finally:
       database.close()

Import every ``*.ld.json`` below a directory in ``prov:startedAtTime`` order.
``started_after`` is an optional cursor for importing only newer runs:

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
