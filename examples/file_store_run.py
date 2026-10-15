"""Record a run as a PROV document and as a JSON record in directories, and read it back."""

from rec.observer import Observer
from rec.run import Run
from rec.stores.file_store import FileStore


class PickPlaceRun(Run):
    def main(self):
        self.add_agent("https://example.org/agent/arm", "Agent", name="arm")
        self.log_repositories([{"name": "controller", "url": "https://example.org/lab/controller", "commit": "0123abcd"}])
        self.log_dependencies([{"name": "rdflib", "version": "7.6.0"}])
        self.add_resource("config/robot.yaml")
        self.add_artefact("results/trajectory.csv")
        # The value the verdict is judged on; the trajectory itself is the artefact.
        self.log_scalar("final-position-error", 0.004)
        return "placed"


if __name__ == "__main__":
    prov = FileStore("runs/prov")
    records = FileStore("runs/json", fmt="json")
    observers = [Observer(prov), Observer(records)]
    run = PickPlaceRun(observers=observers, run_id="pick-place-1")
    run.run()
    for observer in observers:
        observer.close()

    print(f"PROV document: {prov.path(run.id)}")
    record = records.load(run.id)
    assert record is not None
    print(record.state, record.verdict, [(metric.name, metric.step, metric.value) for metric in record.metrics])
