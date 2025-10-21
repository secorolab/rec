from rec.run import Run
from rec.observers.mariadb_observer import MariaDBObserver


def main():
    import time

    print("\tThis the main of the run")
    time.sleep(15)


if __name__ == "__main__":
    run = Run()
    run.main = main
    run.observers.append(MariaDBObserver())

    run.run()
