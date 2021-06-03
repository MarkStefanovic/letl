import json
import logging
import pathlib
import sys
import time
import typing

import sqlalchemy as sa

import letl


def job1(config: typing.Dict[str, typing.Any], logger: letl.Logger) -> None:
    logger.info("Job1 is a happy, simple job...")
    time.sleep(12)


def job2(config: typing.Dict[str, typing.Any], logger: letl.Logger) -> None:
    logger.info("Job2 running (and is going to timeout)...")
    time.sleep(19)


def job3(config: typing.Dict[str, typing.Any], logger: letl.Logger) -> None:
    logger.info("Job3 will always fail...")
    time.sleep(2)
    raise Exception("I'm a bad job.")


def job4(config: typing.Dict[str, typing.Any], logger: letl.Logger) -> None:
    logger.info("Job4 depends on Job1, so it should always run after it.")
    time.sleep(2)


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
            config=None,
            schedule=frozenset({letl.Schedule.every_x_seconds(seconds=30)}),
        ),
        letl.Job(
            job_name=f"Job1",
            timeout_seconds=20,
            dependencies=frozenset(),
            retries=1,
            run=job1,
            config=None,
            schedule=frozenset({letl.Schedule.every_x_seconds(seconds=30)}),
        ),
        letl.Job(
            job_name=f"Job2",
            timeout_seconds=5,
            dependencies=frozenset(),
            retries=1,
            run=job2,
            config=None,
            schedule=frozenset({letl.Schedule.every_x_seconds(seconds=30)}),
        ),
        letl.Job(
            job_name=f"Job3",
            timeout_seconds=20,
            dependencies=frozenset(),
            retries=1,
            run=job3,
            config=None,
            schedule=frozenset({letl.Schedule.every_x_seconds(seconds=30)}),
        ),
    ]
    letl.start(
        jobs=jobs,
        etl_db_uri=config["db_uri"],
        max_job_runners=3,
        log_to_console=True,
        log_sql_to_console=False,
        min_log_level=letl.LogLevel.Info,
    )


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger("letl").setLevel(logging.DEBUG)

    main()
