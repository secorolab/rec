# Robot Execution Catalog (REC)

[![Tests (Python 3.12 and 3.14)](https://github.com/secorolab/rec/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/secorolab/rec/actions/workflows/tests.yml)
[![Documentation](https://github.com/secorolab/rec/actions/workflows/docs.yml/badge.svg?branch=main)](https://secoro.uni-bremen.de/rec/)

REC records robot runs as JSON-LD provenance graphs on the
[prov-extension and rec vocabularies](https://secorolab.github.io/metamodels/).
A run can be stored in a file, MariaDB, or both.

## Install

```shell
pip install -e .
```

Install the MariaDB backend only when needed:

```shell
pip install -e ".[mariadb]"
```

## Record a run

`Run` owns the run ID; the run's IRI ends in it, and reopening an archive
continues that run.

```python
from rec import Run
from rec.observers import FileObserver


class CalibrationRun(Run):
    def main(self):
        self.add_agent("https://example.org/agent/calibrator", "prov:SoftwareAgent", name="calibrator")
        self.add_resource("config/robot.yaml")
        return "ok"


run = CalibrationRun(
    observers=[FileObserver("runs/calibration/rec.ld.json")],
    run_id="calibration",
)
run.run()
```

The run is a `prov-ext:Execution`. Its lifecycle is an OSLC Automation
`oslc_auto:state` (`queued`, `inProgress`, `complete`, `canceled`) and, once
complete, an `oslc_auto:verdict` (`passed`, `failed`, `error`). The metamodel
asks every execution to name the agent that ran it and what it used, as above.

A running run is cancelled from anywhere that reaches its store, by run id:
`FileObserver(path).request_cancel()` or `MariaDBObserver(run_id).request_cancel()`.
The run adopts the request at its next write or heartbeat, sets
`run.cancel_requested` for a cooperative `main()`, and records `canceled`.

## MariaDB

Set the database connection in `.env`:

```dotenv
MARIADB_USER=user
MARIADB_PASSWORD=pass12345
MARIADB_HOST=localhost
MARIADB_PORT=3306
```

Use both observers to write a file archive and MariaDB at the same time; both
records describe the same run node.

```python
from rec.observers import FileObserver, MariaDBObserver
from rec.run import Run

run = Run(
    observers=[
        FileObserver("runs/run-1/rec.ld.json"),
        MariaDBObserver(),
    ],
    run_id="run-1",
)
```

Import archived file-only runs later:

```python
database = MariaDBObserver()
database.sync_file("runs/run-1/rec.ld.json")
database.sync_files("runs")
```

`sync_files()` imports every `*.ld.json` below the directory in
`prov:startedAtTime` order, keeping each archive's run IRI.

## Tests

```shell
pytest
```

The conformance test needs a metamodels checkout beside this repository or
at `REC_METAMODELS_DIR`. MariaDB integration tests require the optional driver
and a disposable database selected by `REC_TEST_MARIADB_DATABASE`:

```shell
REC_TEST_MARIADB_DATABASE=rec_test pytest tests/test_mariadb_observer.py
```

## Acknowledgments

REC is inspired by [Sacred](https://github.com/IDSIA/sacred), an experiment
management tool for machine learning. Small MIT-licensed pieces informed the
original project; REC has been rewritten and simplified for robotics.
