import logging
import multiprocessing as mp
import queue
import threading
import typing
from logging.handlers import QueueHandler, QueueListener

import sqlalchemy as sa

from letl import adapter, domain
from letl.service import admin
from letl.service.job_runner import *
from letl.service.logger import LoggerThread, NamedLogger
from letl.service.scheduler import Scheduler

__all__ = ("start",)

std_logger = domain.root_logger.getChild("run")


def start(
    *,
    jobs: typing.List[domain.Job],
    etl_db_uri: str,
    max_job_runners: int = 5,
    days_logs_to_keep: int = 3,
    log_level: domain.LogLevel = domain.LogLevel.Info,
    log_sql_to_console: bool = False,
    std_logging_handlers: typing.Optional[
        typing.List[typing.Union[logging.StreamHandler, logging.FileHandler]]
    ] = None,
) -> None:
    log_queue = mp.Queue(-1)  # infinite size
    queue_handler = QueueHandler(log_queue)
    domain.root_logger.addHandler(queue_handler)

    if std_logging_handlers:
        std_log_listener: typing.Optional[QueueListener] = QueueListener(
            log_queue, *std_logging_handlers
        )
    else:
        std_log_listener = None

    try:
        if std_log_listener:
            std_log_listener.start()

        std_logger.info("Started.")
        threads: typing.List[threading.Thread] = []

        std_logger.info("Loading jobs...")
        admin_jobs = [
            admin.delete_old_log_entries(
                etl_db_uri=etl_db_uri, days_to_keep=days_logs_to_keep
            ),
        ]
        all_jobs = jobs + admin_jobs
        check_job_names_are_unique(jobs=all_jobs)
        std_logger.info("Jobs have been loaded.")

        engine = sa.create_engine(
            etl_db_uri,
            echo=log_sql_to_console,
            echo_pool=log_sql_to_console,
            future=True,
        )
        std_logger.info("Engine created.")
        # fmt: off
        log_message_queue: "mp.Queue[domain.LogMessage]" = mp.Queue(-1)  # -1 = infinite size
        # fmt: on
        logger_thread = LoggerThread(
            message_queue=log_message_queue,
            engine=engine,
        )
        threads.append(logger_thread)
        logger_thread.start()

        logger = NamedLogger(
            name="root",
            message_queue=log_message_queue,
            min_log_level=log_level,
        )
        logger.info("Logger started.")

        adapter.db.create_tables(engine=engine)

        admin.delete_orphan_jobs(
            admin_engine=engine,
            current_jobs=jobs,
            logger=logger,
        )

        job_queue: "queue.Queue[domain.Job]" = adapter.SetQueue(max_job_runners)

        scheduler = Scheduler(
            engine=engine,
            job_queue=job_queue,
            jobs=jobs,
            logger=logger,
            seconds_between_scans=10,
        )
        threads.append(scheduler)
        scheduler.start()
        logger.info("Scheduler started.")

        for i in range(max_job_runners):
            job_runner = JobRunner(
                engine=engine,
                job_queue=job_queue,
                logger=logger.new(name=f"JobRunner{i}"),
                seconds_between_jobs=1,
            )
            threads.append(job_runner)
            job_runner.start()

        logger.info("JobRunners started.")

        for thread in threads:
            thread.join()
    except Exception as e:
        std_logger.exception(e)
        raise
    finally:
        if std_log_listener:
            std_log_listener.stop()


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
