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
    logger.info("Job1 finished.")


def job2(config: typing.Dict[str, typing.Any], logger: letl.Logger) -> None:
    logger.info("Job2 running (and is going to timeout)...")
    time.sleep(19)
    logger.info("Job2 finished.")


def job3(config: typing.Dict[str, typing.Any], logger: letl.Logger) -> None:
    logger.info("Job3 will always fail...")
    time.sleep(2)
    raise Exception("I'm a bad job.")


def job4(config: typing.Dict[str, typing.Any], logger: letl.Logger) -> None:
    logger.info("Job4 depends on Job1, so it should always run after it.")
    time.sleep(2)
    raise Exception("I'm a bad job.")


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
            dependencies={"Job1"},
            retries=1,
            run=job4,
            config={},
            schedule={letl.EveryXSeconds(seconds=30)},
        ),
        letl.Job(
            job_name=f"Job1",
            timeout_seconds=20,
            dependencies=set(),
            retries=1,
            run=job1,
            config={},
            schedule={letl.EveryXSeconds(seconds=30)},
        ),
        letl.Job(
            job_name=f"Job2",
            timeout_seconds=5,
            dependencies=set(),
            retries=1,
            run=job2,
            config={},
            schedule={letl.EveryXSeconds(seconds=30)},
        ),
        letl.Job(
            job_name=f"Job3",
            timeout_seconds=20,
            dependencies=set(),
            retries=1,
            run=job3,
            config={},
            schedule={letl.EveryXSeconds(seconds=30)},
        ),
    ]
    letl.start(
        jobs=jobs,
        etl_db_uri=config["db_uri"],
        # etl_db_uri="sqlite://",
        max_job_runners=3,
        log_to_console=True,
        log_sql_to_console=False,
        min_log_level=letl.LogLevel.Debug,
    )


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger("letl").setLevel(logging.DEBUG)

    main()
