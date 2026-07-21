# Robot Execution Catalog (REC)

[![Tests (Python 3.12 and 3.14)](https://github.com/secorolab/rec/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/secorolab/rec/actions/workflows/tests.yml)
[![Documentation](https://github.com/secorolab/rec/actions/workflows/docs.yml/badge.svg?branch=main)](https://secoro.uni-bremen.de/rec/)

REC records robot runs as JSON-LD provenance graphs. A run can be stored in a
file, MariaDB, or both.

## Install

```shell
pip install -e .
```

Install the MariaDB backend only when needed:

```shell
pip install -e ".[mariadb]"
```

## Record a run

`Run` owns the canonical run ID. `FileObserver` creates its own stable file ID.

```python
from rec.observers import FileObserver
from rec.run import Run


class CalibrationRun(Run):
    def main(self):
        # Run the robot task here.
        return "ok"


run = CalibrationRun(
    observers=[FileObserver("runs/calibration/rec.jsonld")],
    run_id="calibration",
)
run.run()
```

The graph records the lifecycle as RDF types: `rec:QueuedRun`,
`rec:RunningRun`, `rec:CompletedRun`, `rec:FailedRun`,
`rec:InterruptedRun`, or `rec:CancelledRun`.

## MariaDB

Set the database connection in `.env`:

```dotenv
MARIADB_USER=user
MARIADB_PASSWORD=pass12345
MARIADB_HOST=localhost
MARIADB_PORT=3306
```

Use both observers to write a file archive and MariaDB at the same time. The
database retains the file observer's ID, so both records refer to the same run.

```python
from rec.observers import FileObserver, MariaDBObserver
from rec.run import Run

run = Run(
    observers=[
        FileObserver("runs/run-1/rec.jsonld"),
        MariaDBObserver(),
    ],
    run_id="run-1",
)
```

Import archived file-only runs later:

```python
database = MariaDBObserver()
database.sync_file("runs/run-1/rec.jsonld")
database.sync_files("runs")
```

`sync_files()` orders and filters imports by `prov:startedAtTime`; it does not
replace the file observer's ID.

## Tests

```shell
pytest
```

MariaDB integration tests require a disposable database selected by
`REC_TEST_MARIADB_DATABASE`:

```shell
REC_TEST_MARIADB_DATABASE=rec_test pytest tests/test_mariadb_observer.py
```

## Acknowledgments

REC is inspired by [Sacred](https://github.com/IDSIA/sacred), an experiment
management tool for machine learning. Small MIT-licensed pieces informed the
original project; REC has been rewritten and simplified for robotics.
