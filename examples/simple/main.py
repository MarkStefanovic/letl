import json
import logging
import multiprocessing as mp
import pathlib
import sys
import time
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler

import sqlalchemy as sa

import letl


class Printer:
    def __init__(self, name: str):
        self.name = name

    def close(self) -> None:
        print(f"closed {self.name}.")

    def print(self, /, message: str) -> None:
        print(f"{self.name}: {message}")


class PrinterResource(letl.Resource[Printer]):
    def __init__(self, key: str):
        super().__init__(key=key)

    def open(self) -> Printer:
        return Printer(self.key)

    def close(self, /, handle: Printer) -> None:
        handle.close()


def job1(
    config: letl.Config, logger: letl.Logger, resources: letl.ResourceManager
) -> None:
    logger.info("Job1 will always succeed.")
    printer = resources.get("printer_1", Printer)
    printer.print("Hello")
    time.sleep(12)
    logger.info(config.get("payload", str))


def job2(_: letl.Config, logger: letl.Logger, resources: letl.ResourceManager) -> None:
    printer = resources.get("printer_2", Printer)
    printer.print("Hello")
    logger.info("Job2 running (and is going to timeout)...")
    time.sleep(19)


def job3(_: letl.Config, logger: letl.Logger, __: letl.ResourceManager) -> None:
    logger.info("Job3 will always fail...")
    time.sleep(2)
    raise Exception("I'm a bad job.")


def job4(config: letl.Config, logger: letl.Logger, _: letl.ResourceManager) -> None:
    logger.info("Job4 depends on Job1, so it should always run after it.")
    time.sleep(2)
    logger.info(config.get("payload", str))


def main() -> None:
    config_fp = pathlib.Path(sys.argv[0]).parent / "config.json"
    with config_fp.open("r") as fh:
        json_config = json.load(fh)

    engine = sa.create_engine(json_config["db_uri"])
    letl.db.create_tables(engine=engine, recreate=True)

    jobs = [
        letl.Job(
            job_name=f"Job4",
            timeout_seconds=20,
            dependencies=frozenset({"Job1"}),
            retries=1,
            run=job4,
            config=letl.config(payload="job4_payload"),
            schedule=frozenset({letl.Schedule.every_x_seconds(seconds=30)}),
        ),
        letl.Job(
            job_name=f"Job1",
            timeout_seconds=20,
            dependencies=frozenset(),
            retries=1,
            run=job1,
            config=letl.config(payload="job1_payload"),
            schedule=frozenset({letl.Schedule.every_x_seconds(seconds=30)}),
        ),
        letl.Job(
            job_name=f"Job2",
            timeout_seconds=5,
            dependencies=frozenset(),
            retries=1,
            run=job2,
            config=letl.config(payload="job2_payload"),
            schedule=frozenset({letl.Schedule.every_x_seconds(seconds=30)}),
        ),
        letl.Job(
            job_name=f"Job3",
            timeout_seconds=20,
            dependencies=frozenset(),
            retries=1,
            run=job3,
            config=letl.config(payload="job3_payload"),
            schedule=frozenset({letl.Schedule.every_x_seconds(seconds=30)}),
        ),
    ]

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = RotatingFileHandler("error.log", maxBytes=2000, backupCount=0)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.ERROR)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    log_queue = mp.Queue(-1)  # type: ignore
    queue_handler = QueueHandler(log_queue)
    log_listener = QueueListener(log_queue, console_handler, file_handler)

    letl.root_logger.setLevel(logging.INFO)
    letl.root_logger.addHandler(queue_handler)

    log_listener.start()

    try:
        letl.start(
            jobs=jobs,
            etl_db_uri=json_config["db_uri"],
            max_job_runners=3,
            log_to_console=True,
            log_sql_to_console=False,
            log_level=letl.LogLevel.Debug,
            resources=[
                PrinterResource(key="printer_1"),
                PrinterResource(key="printer_2"),
            ],
        )
    finally:
        log_listener.stop()


if __name__ == "__main__":
    main()
