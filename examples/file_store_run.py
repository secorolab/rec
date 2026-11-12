"""Record a run as a PROV bundle in JSON-LD and in TriG, and read it back."""

from rdf_utils import DatasetFormat
from rdf_utils.models.prov import get_py_pkg_info

from rec.observer import Observer
from rec.run import Run
from rec.stores.file_store import FileStore


class PickPlaceRun(Run):
    def main(self):
        self.log_packages([get_py_pkg_info("rdflib")])
        return "placed"


if __name__ == "__main__":
    as_json_ld = FileStore("runs/prov")
    as_trig = FileStore("runs/prov", fmt=DatasetFormat.TRIG)
    observers = [Observer(as_json_ld), Observer(as_trig)]
    run = PickPlaceRun(observers=observers, run_id="pick-place-1")
    run.run()
    for observer in observers:
        observer.close()

    print(f"PROV bundle: {as_json_ld.path(run.id)}, {as_trig.path(run.id)}")
    record = as_trig.load(run.id)
    assert record is not None
    print(record.state, record.verdict, [package.purl for package in record.packages])
