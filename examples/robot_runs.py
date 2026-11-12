"""Record robot runs, first as bundle files and then in MariaDB: one passes, one fails, one is
cancelled while queued and one is stopped while it runs. Every invocation records new runs.

Needs the `mariadb` extra and the connection settings in `.env` (see the README).
"""

import logging
import threading
import time
from uuid import uuid4

from rec import State
from rec.run import Run
from rec.stores.file_store import FileStore
from rec.stores.mariadb_store import MariaDBStore

logger = logging.getLogger("robot_runs")
MOTIONS = ("approach", "grasp", "lift", "transport", "place", "retreat")
# How far the drawer handle is from its modelled pose, and how far the gripper reaches, in metres.
HANDLE_OFFSET, REACH = 0.04, 0.02


def pick_and_place(run):
    """Pick an object and place it, checking between motions whether the run was asked to stop."""
    for done, motion in enumerate(MOTIONS):
        if run.lifecycle()[0] is State.CANCELING:
            logger.info("%s: stopped before %s", run.id, motion)
            return f"stopped after {done} of {len(MOTIONS)} motions"
        logger.info("%s: %s", run.id, motion)
        time.sleep(0.5)
    return "placed"


def open_drawer(run):
    """Open a drawer, failing when the handle is further from its modelled pose than the gripper reaches."""
    logger.info("%s: looking for the handle", run.id)
    time.sleep(0.5)
    if HANDLE_OFFSET > REACH:
        raise RuntimeError(f"handle is {HANDLE_OFFSET} m from its modelled pose -- the gripper reaches {REACH} m")
    return "opened"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    session = uuid4().hex[:8]
    for make_store in (lambda: FileStore("runs/prov"), lambda: MariaDBStore(db_name="logbook", prefix="logs")):
        # A scheduler holds only the run ids, and its own store on the same files or tables.
        store, scheduler = make_store(), make_store()

        passed = Run(store, f"pick-place-{session}-1")
        # A short beat, so the run records a few heartbeats while it moves.
        passed.beat_interval = 1
        passed.run(pick_and_place)

        failed = Run(store, f"drawer-{session}-1")
        failed.run(open_drawer)

        queued = Run(store, f"pick-place-{session}-2")
        queued.queue()
        Run(scheduler, queued.id).cancel()

        stopped = Run(store, f"pick-place-{session}-3")
        running = threading.Thread(target=stopped.run, args=(pick_and_place,))
        running.start()
        time.sleep(1.2)
        Run(scheduler, stopped.id).cancel()
        running.join()

        for run in (passed, failed, queued, stopped):
            print(type(store).__name__, run.id, *run.lifecycle())
        if isinstance(store, MariaDBStore):
            store.close()
            scheduler.close()
