# Robot Execution Catalog (REC)

REC makes the data of robot executions [FAIR](https://www.go-fair.org/fair-principles/): it records
each run's lifecycle in [PROV](https://www.w3.org/TR/prov-o/), on the
[rec and prov-extension vocabularies](https://secorolab.github.io/metamodels/), as a provenance
bundle written with [rdf-utils](https://github.com/minhnh/rdf-utils).

## Installation

```shell
pip install -e .              # file stores
pip install -e ".[mariadb]"   # and the MariaDB store
```

## Recording a run

A run is a `Run` subclass; `main` does the work and logs the software packages it uses.

```python
from rdf_utils.models.prov import get_py_pkg_info

from rec.observer import Observer
from rec.run import Run
from rec.stores.file_store import FileStore


class PickPlaceRun(Run):
    def main(self):
        self.log_packages([get_py_pkg_info("controller")])
        return "placed"


observer = Observer(FileStore("runs/prov"))
PickPlaceRun(observers=[observer], run_id="pick-place-1").run()
observer.close()
```

`run()` records the start and the host, runs `main`, and ends the run as `passed`, `failed`
(`main` raised) or `error` (interrupted). It returns what `main` returned; rec does not record it.

Files a run uses or generates, and its steps, are recorded by the tool that knows them, with
rdf-utils' `add_file_entity`, `add_activity` and the `load_*_prov` helpers, in that tool's own
bundle.

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
MariaDB): only a new run is queued, only a new or queued run starts. A started run beats every
`beat_interval` seconds, recorded as `rec:heartbeat-time`.

## Stores

| Store | Keeps a run as |
|---|---|
| `FileStore(dir)` | its bundle, `<run_id>.ld.json`; `fmt=DatasetFormat.TRIG` or `NQUADS` for `.trig`, `.nq` |
| `MariaDBStore(db_name, prefix)` | a row of `<prefix>_runs` and a row per package in `<prefix>_packages`, see [mariadb.sql](rec/stores/mariadb.sql) |

A run may have several observers, one store each. Each store names the run's bundle on its first
edit, so two stores hold the same run in differently named bundles. Any object with `load`,
`edit`, `run_ids` and `close` is a store (`rec.stores.Store`).

## The PROV bundle

```python
from rec import provenance

ds = provenance.dataset(store.load("pick-place-1"))   # RunRecord -> rdflib Dataset
record = provenance.record(ds, "pick-place-1")        # and back
```

```trig
<urn:uuid:7f1c…> {
    <urn:uuid:7f1c…> a prov:Bundle ;
        prov:wasAttributedTo <pkg:git/github.com/secorolab/rec@816d61f> .

    <https://secoro.uni-bremen.de/rec/run/pick-place-1>
        a prov-ext:Execution, oslc_auto:AutomationResult ;
        oslc_auto:state oslc_auto:complete ;
        oslc_auto:verdict oslc_auto:passed ;
        prov:wasAssociatedWith <pkg:git/github.com/secorolab/rec@816d61f> ;
        prov:used <pkg:git/example.org/lab/controller@0123abcd> ;
        time:hasBeginning <https://secoro.uni-bremen.de/rec/run/pick-place-1/beginning> ;
        time:hasEnd <https://secoro.uni-bremen.de/rec/run/pick-place-1/end> ;
        prov:atLocation <https://secoro.uni-bremen.de/rec/run/pick-place-1/host> .
}
```

A package is recorded by rdf-utils' `load_pkg_prov`, named by its purl. A run id is RFC 3986
unreserved characters (letters, digits, `-`, `.`, `_`, `~`); a run given none gets a UUID.

## Examples

```shell
python examples/file_store_run.py      # JSON-LD and TriG files
python examples/mariadb_store_run.py   # queue, cancel by id, run
python examples/all_stores_run.py      # one run in a file and in MariaDB
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
