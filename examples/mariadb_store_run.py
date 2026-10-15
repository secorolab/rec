"""Queue runs in MariaDB, cancel one by id as a scheduler would, run the other, and read it back.

Needs the `mariadb` extra and the connection settings in `.env` (see the README).
"""

from rec import State
from rec.observer import Observer
from rec.run import Run
from rec.stores.mariadb_store import MariaDBStore


class DrawerOpeningRun(Run):
    def main(self):
        self.add_agent("https://example.org/agent/dual-arm", "Agent", name="dual-arm")
        self.add_resource("config/cabinet.yaml")
        if self.state is State.CANCELING:
            return "stopped"
        self.add_artefact("results/drawer-opening.bag")
        self.log_scalar("loop-overruns", 0)
        self.log_scalar("retries", 1)
        return "opened"


if __name__ == "__main__":
    observer = Observer(MariaDBStore(db_name="logbook", prefix="logs"))
    kept = DrawerOpeningRun(observers=[observer], run_id="drawer-1")
    dropped = DrawerOpeningRun(observers=[observer], run_id="drawer-2")
    kept.queue()
    dropped.queue()
    print("queued:", observer.store.run_ids(State.QUEUED))

    # A scheduler holds only the id, and its own connection to the same tables.
    scheduler = Observer(MariaDBStore(db_name="logbook", prefix="logs"))
    Run(observers=[scheduler], run_id="drawer-2").cancel()
    scheduler.close()
    print("drawer-2:", dropped.state)

    kept.run()
    record = observer.store.load(kept.id)
    assert record is not None
    print(record.state, record.verdict, [(metric.name, metric.step, metric.value) for metric in record.metrics])
    print("canceled:", observer.store.run_ids(State.CANCELED))
    observer.close()
