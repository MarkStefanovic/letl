import time
import typing

from letl import domain
from letl.service import sa_job_queue, sa_job_runner, sa_logger, sa_scheduler

__all__ = ("start",)


def start(
    *,
    jobs: typing.List[domain.Job],
    etl_db_uri: str,
    max_processes: int = 5,
    log_to_console: bool = False,
    tick_seconds: int = 1,
) -> None:
    check_job_names_are_unique(jobs=jobs)
    log_actor = sa_logger.SALogger.start(db_uri=etl_db_uri)
    logger = sa_logger.NamedLogger(
        name="root",
        sql_logger=log_actor,
        log_to_console=log_to_console,
    )
    job_queue_actor = sa_job_queue.SAJobQueue.start(
        db_uri=etl_db_uri,
        logger=logger.new(name="JobQueue"),
    ).proxy()
    scheduler_actor = sa_scheduler.SAScheduler.start(
        db_uri=etl_db_uri,
        jobs=jobs,
        job_queue=job_queue_actor,
        logger=logger.new(name="Scheduler"),
        echo_sql=log_to_console,
    )
    job_runners = {
        i: sa_job_runner.SAJobRunner.start(
            db_uri=etl_db_uri,
            scheduler=scheduler_actor,
            logger=logger.new(name=f"JobRunner{i}"),
            echo_sql=log_to_console,
        )
        for i in range(max_processes)
    }
    while True:
        # print("tick")
        # TODO report back status of job runners if log_to_console is True
        time.sleep(tick_seconds)

    # pykka.ActorRegistry.stop_all()


def check_job_names_are_unique(*, jobs: typing.List[domain.Job]) -> None:
    jobs_seen = []
    duplicate_job_names = []
    for job in jobs:
        if job.job_name in jobs_seen:
            duplicate_job_names.append(job.job_name)
        else:
            jobs_seen.append(job.job_name)
    if duplicate_job_names:
        raise domain.error.DuplicateJobNames(set(duplicate_job_names))
