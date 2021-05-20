import itertools
from multiprocessing import Queue  # noqa
import time
import typing

import pykka
import sqlalchemy as sa

from letl import adapter, domain
from letl.service import admin, sa_job_queue, sa_job_runner, sa_logger, sa_scheduler

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

    reg = pykka.ActorRegistry()
    handles = []
    try:
        engine = sa.create_engine(
            etl_db_uri,
            echo=log_sql_to_console,
            echo_pool=log_sql_to_console,
            # pool_size=max_job_runners + 2,
            future=True,
        )
        print("Engine created.")

        log_actor = sa_logger.SALogger.start(engine=engine)
        logger = sa_logger.NamedLogger(
            name="root",
            sql_logger=log_actor,
            log_to_console=log_to_console,
            min_log_level=min_log_level,
        )
        logger.info("Logger started.")

        adapter.db.create_tables(engine=engine)

        # TODO clear etl status rows flagged as running at startup

        job_queue = sa_job_queue.SAJobQueue.start(
            engine=engine,
            jobs=jobs,
            logger=logger.new(name="JobQueue"),
        ).proxy()

        scheduler_handle = domain.repeat(
            seconds=10,
            fn=sa_scheduler.update_queue,
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
                fn=sa_job_runner.run_job,
                engine=engine,
                job_queue=job_queue,
                logger=logger.new(name=f"JobRunner{i}"),
                log_to_console=log_to_console,
            )
            handles.append(job_handle)

        logger.info("JobRunners started.")

        while True:
            actors = reg.get_all()
            status = {
                ("Alive" if is_alive else "Dead"): sorted(
                    a.actor_class.__name__ for a in actors_by_status
                )
                for is_alive, actors_by_status in itertools.groupby(
                    actors, key=lambda a: a.is_alive()
                )
            }
            if dead_actors := status.get("Dead"):
                logger.debug(f"Dead Actors: {', '.join(dead_actors)}.")
            time.sleep(10)
            print("Tick")
    finally:
        # stop all repeating function
        for handle in handles:
            handle()
        reg.stop_all(block=True, timeout=10)


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
