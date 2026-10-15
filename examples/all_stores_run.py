"""Record one run in all three stores at once -- PROV document, JSON record, MariaDB tables -- and check they give back the same graph.

Needs the `mariadb` extra and the connection settings in `.env` (see the README).
"""

from rdflib.compare import isomorphic

from rec import provenance
from rec.observer import Observer
from rec.run import Run
from rec.stores.file_store import FileStore
from rec.stores.mariadb_store import MariaDBStore


class ScrewingRun(Run):
    def main(self):
        self.add_agent("https://example.org/agent/arm", "Agent", name="arm")
        self.log_dependencies([{"name": "rdflib", "version": "7.6.0"}])
        self.add_resource("config/screw.yaml")
        self.add_artefact("results/torque.csv")
        # One count per screw: a step per attempt, not per sample.
        for screw, attempts in enumerate([1, 2, 1]):
            self.log_scalar("attempts", attempts, screw)
        self.log_scalar("safety-stops", 0)
        return 3


if __name__ == "__main__":
    stores = [FileStore("runs/prov"), FileStore("runs/json", fmt="json"), MariaDBStore(db_name="logbook", prefix="logs")]
    observers = [Observer(store) for store in stores]
    run = ScrewingRun(observers=observers, run_id="screw-1")
    run.run()

    prov, records, tables = (provenance.graph(store.load(run.id)) for store in stores)
    assert isomorphic(prov, records) and isomorphic(prov, tables)
    print(prov.serialize(format="turtle"))
    for observer in observers:
        observer.close()
