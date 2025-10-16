from rec.run import Run
from rec.observers.mariadb import MariaDBObserver

if __name__ == "__main__":
    run = Run()
    # run.observers.append(MariaDBObserver())

    run.run()
