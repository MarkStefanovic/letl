import multiprocessing as mp
import time
import typing

import sqlalchemy as sa

from letl import adapter, domain
from letl.service import admin
from letl.service.job_queue import JobQueue
from letl.service.job_runner import run_job
from letl.service.logger import NamedLogger
from letl.service.scheduler import update_queue

__all__ = ("start",)


def start(
    *,
    jobs: typing.List[domain.Job],
    etl_db_uri: str,
    max_job_runners: int = 5,
    min_log_level: domain.LogLevel = domain.LogLevel.Info,
    log_sql_to_console: bool = False,
    log_to_console: bool = False,
    days_logs_to_keep: int = 3,
) -> None:
    admin_jobs = [
        admin.delete_old_log_entries(
            etl_db_uri=etl_db_uri, days_to_keep=days_logs_to_keep
        ),
    ]
    all_jobs = jobs + admin_jobs
    check_job_names_are_unique(jobs=all_jobs)

    handles = []
    try:
        engine = sa.create_engine(
            etl_db_uri,
            echo=log_sql_to_console,
            echo_pool=log_sql_to_console,
            future=True,
        )
        print("Engine created.")

        log_message_queue: mp.Queue[domain.LogMessage] = mp.Queue(
            -1
        )  # -1 = infinite size
        logger = NamedLogger(
            name="root",
            message_queue=log_message_queue,
            log_to_console=log_to_console,
            min_log_level=min_log_level,
        )
        logger.info("Logger started.")

        adapter.db.create_tables(engine=engine)

        admin.delete_orphan_jobs(
            admin_engine=engine,
            current_jobs=jobs,
            logger=logger,
        )

        job_queue = JobQueue(
            jobs=jobs,
            logger=logger.new(name="JobQueue"),
        )

        scheduler_handle = domain.repeat(
            seconds=10,
            fn=update_queue,
            engine=engine,
            jobs=jobs,
            job_queue=job_queue,
            logger=logger.new(name="Scheduler"),
        )
        handles.append(scheduler_handle)
        logger.info("Scheduler started.")

        for i in range(max_job_runners):
            job_handle = domain.repeat(
                seconds=10,
                fn=run_job,
                engine=engine,
                job_queue=job_queue,
                logger=logger.new(name=f"JobRunner{i}"),
            )
            handles.append(job_handle)

        logger.info("JobRunners started.")

        while True:
            time.sleep(10)
    finally:
        # stop all repeating function
        for handle in handles:
            handle()


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
