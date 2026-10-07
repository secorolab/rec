from rec.observer import Observer
from rec.run import Run
from rec.stores.mariadb_store import MariaDBStore


def main():
    import time

    print("\tThis the main of the run")
    time.sleep(15)


if __name__ == "__main__":
    observer = Observer(MariaDBStore())
    run = Run(observers=[observer])
    run.main = main

    run.run()
    observer.close()
