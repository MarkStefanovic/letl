import datetime
import time
import typing

import letl


def job1(config: typing.Mapping[str, typing.Any], logger: letl.Logger) -> None:
    logger.info("Job1 running...")
    time.sleep(10)
    logger.info("Job1 finished.")


def job2(config: typing.Mapping[str, typing.Any], logger: letl.Logger) -> None:
    logger.info("Job2 running...")
    time.sleep(10)
    logger.info("Job2 finished.")


def main() -> None:
    jobs = [
        letl.Job(
            job_name=f"Job1",
            timeout_seconds=5,
            dependencies=set(),
            retries=1,
            run=job1,
            config={},
            schedule=[letl.EveryXSeconds(seconds=30)],
        ),
        letl.Job(
            job_name=f"Job2",
            timeout_seconds=5,
            dependencies=set(),
            retries=1,
            run=job2,
            config={},
            schedule=[letl.EveryXSeconds(seconds=30)],
        ),
    ]
    letl.start(
        jobs=jobs,
        etl_db_uri="sqlite://",
        max_processes=3,
        log_to_console=True,
        tick_seconds=1,
    )


if __name__ == "__main__":
    main()
