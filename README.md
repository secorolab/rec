# Robot Execution Catalog (REC)

REC makes the data of robot executions [FAIR](https://www.go-fair.org/fair-principles/): it records
each run's provenance in [PROV](https://www.w3.org/TR/prov-o/), on the
[rec and prov-extension vocabularies](https://secorolab.github.io/metamodels/).

## Installation

```shell
pip install -e .              # file stores
pip install -e ".[mariadb]"   # and the MariaDB store
```

## Recording a run

A run is a `Run` subclass; `main` does the work and logs what it uses and generates.

```python
from rec.observer import Observer
from rec.run import Run
from rec.stores.file_store import FileStore


class PickPlaceRun(Run):
    def main(self):
        self.add_agent("https://example.org/agent/arm", "Agent", name="arm")
        self.log_repositories([{"name": "controller", "url": "https://example.org/lab/controller", "commit": "0123abcd"}])
        self.add_resource("config/robot.yaml")
        self.add_artefact("results/trajectory.csv")
        self.log_scalar("final-position-error", 0.004)
        return "placed"


observer = Observer(FileStore("runs/prov"))
PickPlaceRun(observers=[observer], run_id="pick-place-1").run()
observer.close()
```

`run()` records the start, runs `main`, and ends the run as `passed`, `failed` (`main` raised) or
`error` (interrupted). What `main` returns is kept as the run's result.

### Steps of a run

A step is an activity of a prov-extension kind (`Execution`, `Assurance`, `Transformation`,
`Generalization`, `Specification`) that is `dcterms:isPartOf` the run. Files name the step that
used or generated them:

```python
calibration = "https://example.org/calibration/1"
self.add_activity(calibration, "Execution", "https://example.org/agent/arm", start, end)
self.add_resource("config/robot.yaml", usage_activity=calibration)
self.add_artefact("results/calibration.json", gen_activity=calibration)
```

### Queueing and cancelling

State lives in the store, so any `Run` with the same id sees and changes the same run:

```python
DrawerRun(observers=[observer], run_id="drawer-2").queue()

# elsewhere, e.g. a scheduler with its own connection
Run(observers=[Observer(MariaDBStore())], run_id="drawer-2").cancel()
```

| State when cancelled | Result |
|---|---|
| `queued` | `canceled` |
| `in-progress` | `canceling`; `main` sees `self.state is State.CANCELING`, returns, and the run ends `canceled` |
| anything else | `RuntimeError` |

The store decides every step under a lock (`flock` for files, `SELECT ... FOR UPDATE` for
MariaDB): only a new run is queued, only a new or queued run starts.

## Stores

| Store | Keeps a run as |
|---|---|
| `FileStore(dir)` | its PROV document, `<run_id>.ld.json` |
| `FileStore(dir, fmt="json")` | the `RunRecord` as `<run_id>.json` |
| `MariaDBStore(db_name, prefix)` | a row of `<prefix>_runs` and a row per list item, see [mariadb.sql](rec/stores/mariadb.sql) |

All keep the same record. A run may have several observers, one store each:

```python
stores = [FileStore("runs/prov"), FileStore("runs/json", fmt="json"), MariaDBStore()]
run = ScrewingRun(observers=[Observer(store) for store in stores], run_id="screw-1")
run.run()

graphs = [provenance.graph(store.load("screw-1")) for store in stores]
assert all(isomorphic(graphs[0], graph) for graph in graphs)
```

### One artefact in each store

```python
self.add_artefact("results/trajectory.csv", sha256="9f86d0...0a08", size_bytes=2048)  # logged from /home/lab
```

`FileStore(dir)`, `<run_id>.ld.json` (shown as Turtle; `run:` is `https://secoro.uni-bremen.de/rec/run/`):

```turtle
<run:pick-place-1/file/%2Fhome%2Flab/results%2Ftrajectory.csv>
    a prov:Entity ;
    prov:atLocation [ a prov-ext:RelativeLocation ;
                      prov-ext:rel-path "results/trajectory.csv"^^xsd:anyURI ;
                      prov:atLocation <file:///home/lab/> ] ;
    prov:wasGeneratedBy run:pick-place-1 ;
    prov:qualifiedGeneration [ a prov:Generation ;
                               prov:activity run:pick-place-1 ;
                               prov:atTime "2026-10-15T09:30:00+00:00"^^xsd:dateTime ] ;
    spdx:checksum [ a spdx:Checksum ;
                    spdx:algorithm spdx:checksumAlgorithm_sha256 ;
                    spdx:checksumValue "9f86d0...0a08"^^xsd:hexBinary ] ;
    dcat:byteSize "2048"^^xsd:nonNegativeInteger .
```

`FileStore(dir, fmt="json")`, `<run_id>.json`:

```json
"artefacts": [
  {
    "file": {"path": "results/trajectory.csv", "root": "/home/lab", "title": null,
             "sha256": "9f86d0...0a08", "size_bytes": 2048},
    "activity": null,
    "time": "2026-10-15T09:30:00+00:00"
  }
]
```

`MariaDBStore()`, a row of `logs_artefacts` (`run` is the run's `id` in `logs_runs`; `activity` is
`''` for the run itself):

| run | path | root | title | sha256 | size_bytes | activity | time |
|---|---|---|---|---|---|---|---|
| 1 | results/trajectory.csv | /home/lab | NULL | 9f86d0...0a08 | 2048 | | 2026-10-15 09:30:00.000000 |

A value one store could not give back is refused when logged: a NaN or infinite metric, or an
item logged twice (`rec.record.KEYS`). Any object with `load`, `edit`, `run_ids` and `close` is a
store (`rec.stores.Store`).

## The PROV graph

```python
from rec import provenance

g = provenance.graph(store.load("pick-place-1"))      # RunRecord -> rdflib Graph
record = provenance.record(g, "pick-place-1")         # and back
```

```turtle
<https://secoro.uni-bremen.de/rec/run/pick-place-1>
    a prov-ext:Execution, oslc_auto:AutomationResult ;
    oslc_auto:state oslc_auto:complete ;
    oslc_auto:verdict oslc_auto:passed ;
    prov:used <.../repository/controller>, ... ;
    prov:wasAssociatedWith <https://example.org/agent/arm> ;
    rec:result "placed" .
```

A run id is RFC 3986 unreserved characters (letters, digits, `-`, `.`, `_`, `~`); a run given none
gets a UUID.

## Examples

```shell
python examples/file_store_run.py      # PROV and JSON files
python examples/mariadb_store_run.py   # queue, cancel by id, run
python examples/all_stores_run.py      # one run in all three stores
```

The MariaDB examples need the `mariadb` extra and an `.env` at the repository root:

```dotenv
MARIADB_USER="user"
MARIADB_PASSWORD="pass12345"
MARIADB_HOST="localhost"
MARIADB_PORT=3306
```

## Tests

```shell
pip install -e ".[dev]"
pytest
```

The conformance tests validate runs against the shapes of a
[metamodels](https://github.com/secorolab/metamodels) checkout beside this repository or at
`REC_METAMODELS_DIR`. MariaDB tests need the `mariadb` extra and a disposable database named by
`REC_TEST_MARIADB_DATABASE`.

## Acknowledgments

Inspired by [sacred](https://github.com/IDSIA/sacred/tree/master); small pieces of its code are
used under the [MIT License](https://opensource.org/license/MIT).
