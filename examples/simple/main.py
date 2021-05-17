import time
import typing

import letl


def job1(config: typing.Dict[str, typing.Any], logger: letl.Logger) -> None:
    logger.info("Job1 running...")
    time.sleep(10)
    logger.info("Job1 finished.")


def job2(config: typing.Dict[str, typing.Any], logger: letl.Logger) -> None:
    logger.info("Job2 running...")
    time.sleep(10)
    logger.info("Job2 finished.")


def job3(config: typing.Dict[str, typing.Any], logger: letl.Logger) -> None:
    logger.info("Job2 running...")
    time.sleep(2)
    raise Exception("I'm a bad job.")


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
        letl.Job(
            job_name=f"Job3",
            timeout_seconds=5,
            dependencies=set(),
            retries=1,
            run=job3,
            config={},
            schedule=[letl.EveryXSeconds(seconds=30)],
        ),
    ]
    letl.start(
        jobs=jobs,
        etl_db_uri="sqlite:///temp.db",
        # etl_db_uri="sqlite://",
        max_processes=3,
        log_to_console=True,
    )


if __name__ == "__main__":
    main()
