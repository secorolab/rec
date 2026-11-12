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

A run is a `Run` subclass; `main` does the work.

```python
from rec.observer import Observer
from rec.run import Run
from rec.stores.file_store import FileStore


class PickPlaceRun(Run):
    def main(self):
        return "placed"


PickPlaceRun(Observer(FileStore("runs/prov")), run_id="pick-place-1").run()
```

`run()` records the start and the host, runs `main`, and ends the run as `passed`, `failed`
(`main` raised) or `error` (interrupted, e.g. `KeyboardInterrupt` or `SystemExit`, which `run()`
raises again once recorded). It returns what `main` returned; rec does not record it.

Each event changes only its own triples in the run's bundle: the store parses the bundle under its
lock, the `Observer` checks the run's state and sets what the event changes, and the store writes
it back. rdf-utils writes the bundle (`create_bundle`), rec as its software agent (`load_pkg_prov`),
the execution (`load_execution_prov`) and its instants (`add_wall_instant`), each once.

What a run executes, the software and files it uses or generates, and its steps, are recorded by
the tool that knows them, with rdf-utils' `add_file_entity`, `add_activity` and the `load_*_prov`
helpers, in that tool's own bundle.

### Queueing and cancelling

State lives in the store, so any `Run` with the same id sees and changes the same run:

```python
DrawerRun(Observer(MariaDBStore()), run_id="drawer-2").queue()

# elsewhere, e.g. a scheduler with its own connection
Run(Observer(MariaDBStore()), run_id="drawer-2").cancel()
```

| State when cancelled | Result |
|---|---|
| `queued` | `canceled`; it never started, so it has no beginning or end |
| `in-progress` | `canceling`; `main` sees `self.state is State.CANCELING`, returns, and the run ends `canceled`; a `main` that raises instead ends `complete` with its verdict |
| anything else | `RuntimeError` |

The store decides every step under a lock (`flock` for files, the run's row for MariaDB): only a
new run is queued, only a new or queued run starts. A started run beats every `beat_interval`
seconds, recorded as `rec:heartbeat-time`; a beat that fails is logged and the next one tried.

## Stores

| Store | Keeps a run as |
|---|---|
| `FileStore(dir)` | its bundle, `<run_id>.ld.json` |
| `MariaDBStore(db_name, prefix)` | its bundle as JSON-LD in a row of `<prefix>_runs` (`id`, `run_id`, `bundle`) |

`load(run_id)` returns the run's bundle as an rdflib `Dataset`; `Observer.lifecycle(run_id)` gives
its state and verdict. A run is recorded in one store.

## The PROV bundle

```trig
<urn:uuid:7f1c…> {
    <urn:uuid:7f1c…> a prov:Bundle ;
        prov:wasAttributedTo <pkg:git/github.com/secorolab/rec@816d61f> .

    <https://secoro.uni-bremen.de/rec/run/pick-place-1>
        a prov-ext:Execution, oslc_auto:AutomationResult ;
        oslc_auto:state oslc_auto:complete ;
        oslc_auto:verdict oslc_auto:passed ;
        prov:wasAssociatedWith <pkg:git/github.com/secorolab/rec@816d61f> ;
        time:hasBeginning <https://secoro.uni-bremen.de/rec/run/pick-place-1/beginning> ;
        time:hasEnd <https://secoro.uni-bremen.de/rec/run/pick-place-1/end> ;
        prov:atLocation <https://secoro.uni-bremen.de/rec/run/pick-place-1/host> .
}
```

The bundle is attributed to the rec that first recorded the run (e.g. a scheduler that queued it);
the run is associated with the rec that started it. The run takes its state and verdict terms from
OSLC Automation 2.1; it is not a conformant `oslc_auto:AutomationResult`, which also needs an
automation plan, a title and an identifier.

A run id is RFC 3986 unreserved characters (letters, digits, `-`, `.`, `_`, `~`); a run given none
gets a UUID. The run's node is `<base><run_id>`, `base` given to the `Observer`.

## Examples

```shell
python examples/robot_runs.py   # passed, failed, cancelled while queued, stopped while running
```

It records the runs as bundle files under `runs/prov`, then in MariaDB, so it needs the `mariadb` extra and an `.env` at the repository root:

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
