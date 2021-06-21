import dataclasses
import json
import logging
import multiprocessing as mp
import pathlib
import sys
import time
import typing
from logging.handlers import QueueHandler, QueueListener, RotatingFileHandler

import sqlalchemy as sa

import letl


@dataclasses.dataclass(frozen=True, eq=True)
class Config:
    payload: str


def job1(config: typing.Hashable, logger: letl.Logger) -> None:
    logger.info("Job1 will always succeed.")
    assert isinstance(config, Config)
    time.sleep(12)
    logger.info(config.payload)


def job2(_: typing.Hashable, logger: letl.Logger) -> None:
    logger.info("Job2 running (and is going to timeout)...")
    time.sleep(19)


def job3(config: typing.Hashable, logger: letl.Logger) -> None:
    logger.info("Job3 will always fail...")
    time.sleep(2)
    raise Exception("I'm a bad job.")


def job4(config: typing.Hashable, logger: letl.Logger) -> None:
    logger.info("Job4 depends on Job1, so it should always run after it.")
    assert isinstance(config, Config)
    time.sleep(2)
    logger.info(config.payload)


def main() -> None:

    config_fp = pathlib.Path(sys.argv[0]).parent / "config.json"
    with config_fp.open("r") as fh:
        config = json.load(fh)

    engine = sa.create_engine(config["db_uri"])
    letl.db.create_tables(engine=engine, recreate=True)

    jobs = [
        letl.Job(
            job_name=f"Job4",
            timeout_seconds=20,
            dependencies=frozenset({"Job1"}),
            retries=1,
            run=job4,
            config=Config("job4_payload"),
            schedule=frozenset({letl.Schedule.every_x_seconds(seconds=30)}),
        ),
        letl.Job(
            job_name=f"Job1",
            timeout_seconds=20,
            dependencies=frozenset(),
            retries=1,
            run=job1,
            config=Config("job1_payload"),
            schedule=frozenset({letl.Schedule.every_x_seconds(seconds=30)}),
        ),
        letl.Job(
            job_name=f"Job2",
            timeout_seconds=5,
            dependencies=frozenset(),
            retries=1,
            run=job2,
            config=Config("job2_payload"),
            schedule=frozenset({letl.Schedule.every_x_seconds(seconds=30)}),
        ),
        letl.Job(
            job_name=f"Job3",
            timeout_seconds=20,
            dependencies=frozenset(),
            retries=1,
            run=job3,
            config=Config("job3_payload"),
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
            etl_db_uri=config["db_uri"],
            max_job_runners=3,
            log_sql_to_console=False,
            log_level=letl.LogLevel.Debug,
        )
    finally:
        log_listener.stop()


if __name__ == "__main__":
    main()
