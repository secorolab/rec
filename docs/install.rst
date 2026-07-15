Installation
============

REC requires Python 3.12 or newer. Install it from a checkout:

.. code-block:: shell

   python -m venv .venv
   . .venv/bin/activate
   pip install -e .

This installs file-backed recording. Install the optional MariaDB backend when
the run will be stored in MariaDB or archives will be synchronised there:

.. code-block:: shell

   pip install -e ".[mariadb]"

Quick run
---------

Save this as ``quick_run.py`` and run ``python quick_run.py``. It creates a
JSON-LD archive at ``runs/quick/rec.jsonld``:

.. code-block:: python

   from rec import Run
   from rec.observers import FileObserver


   class QuickRun(Run):
       def main(self):
           return "ok"


   QuickRun(
       observers=[FileObserver("runs/quick/rec.jsonld")],
       run_id="quick",
   ).run()

MariaDB connection
------------------

``MariaDBObserver`` reads connection settings from a ``.env`` file in the
working directory:

.. code-block:: ini

   MARIADB_USER=rec
   MARIADB_PASSWORD=secret
   MARIADB_HOST=localhost
   MARIADB_PORT=3306

The configured database must already exist. REC creates its own ``runs`` and
``runs_file_sources`` tables when the observer is first used.
