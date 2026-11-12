"""Record one run in a PROV bundle file and in MariaDB tables at once, and check they give back the same run.

Needs the `mariadb` extra and the connection settings in `.env` (see the README).
"""

from rdf_utils.models.prov import get_py_pkg_info
from rdflib.compare import isomorphic

from rec import provenance
from rec.observer import Observer
from rec.run import Run
from rec.stores.file_store import FileStore
from rec.stores.mariadb_store import MariaDBStore


class ScrewingRun(Run):
    def main(self):
        self.log_packages([get_py_pkg_info("rdflib")])
        return 3


if __name__ == "__main__":
    stores = [FileStore("runs/prov"), MariaDBStore(db_name="logbook", prefix="logs")]
    observers = [Observer(store) for store in stores]
    run = ScrewingRun(observers=observers, run_id="screw-1")
    run.run()

    from_file, from_tables = (store.load(run.id) for store in stores)
    # Each store minted its own bundle on the run's first edit.
    from_tables.bundle = from_file.bundle
    bundle, tables = (provenance.dataset(record).graph(record.bundle) for record in (from_file, from_tables))
    assert isomorphic(bundle, tables)
    print(bundle.serialize(format="turtle"))
    for observer in observers:
        observer.close()
