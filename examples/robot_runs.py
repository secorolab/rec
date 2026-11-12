"""Record robot runs, first as bundle files and then in MariaDB: one passes, one fails, one is
cancelled while queued and one is stopped while it runs.

Needs the `mariadb` extra and the connection settings in `.env` (see the README).
"""

import logging
import threading
import time

from rec import State
from rec.observer import Observer
from rec.run import Run
from rec.stores.file_store import FileStore
from rec.stores.mariadb_store import MariaDBStore

logger = logging.getLogger("robot_runs")


class PickPlaceRun(Run):
    """Picks an object and places it, checking between motions whether it was asked to stop."""

    motions = ("approach", "grasp", "lift", "transport", "place", "retreat")

    def main(self):
        for done, motion in enumerate(self.motions):
            if self.state is State.CANCELING:
                logger.info("%s: stopped before %s", self.id, motion)
                return f"stopped after {done} of {len(self.motions)} motions"
            logger.info("%s: %s", self.id, motion)
            time.sleep(0.5)
        return "placed"


class DrawerOpeningRun(Run):
    """Opens a drawer, and fails when the handle is further from its modelled pose than the gripper reaches."""

    handle_offset = 0.04
    reach = 0.02

    def main(self):
        logger.info("%s: looking for the handle", self.id)
        time.sleep(0.5)
        if self.handle_offset > self.reach:
            raise RuntimeError(f"handle is {self.handle_offset} m from its modelled pose -- the gripper reaches {self.reach} m")
        return "opened"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    # A scheduler holds only the run ids, and its own store on the same files or tables.
    for store, scheduler_store in (
        (FileStore("runs/prov"), FileStore("runs/prov")),
        (MariaDBStore(db_name="logbook", prefix="logs"), MariaDBStore(db_name="logbook", prefix="logs")),
    ):
        observer, scheduler = Observer(store), Observer(scheduler_store)

        passed = PickPlaceRun(observer, run_id="pick-place-1")
        # A short beat, so the run records a few heartbeats while it moves.
        passed.beat_interval = 1
        passed.run()

        DrawerOpeningRun(observer, run_id="drawer-1").run()

        PickPlaceRun(observer, run_id="pick-place-2").queue()
        Run(scheduler, run_id="pick-place-2").cancel()

        stopped = PickPlaceRun(observer, run_id="pick-place-3")
        running = threading.Thread(target=stopped.run)
        running.start()
        time.sleep(1.2)
        Run(scheduler, run_id="pick-place-3").cancel()
        running.join()

        for run_id in ("pick-place-1", "drawer-1", "pick-place-2", "pick-place-3"):
            print(type(store).__name__, run_id, *observer.lifecycle(run_id))
    store.close()
    scheduler_store.close()
