import itertools
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
    max_processes: int = 5,
    min_log_level: domain.LogLevel = domain.LogLevel.Info,
    log_to_console: bool = False,
    days_logs_to_keep: int = 3,
) -> None:
    admin_jobs = [
        admin.delete_old_log_entries(days_to_keep=days_logs_to_keep),
    ]
    all_jobs = jobs + admin_jobs
    check_job_names_are_unique(jobs=all_jobs)

    reg = pykka.ActorRegistry()
    try:
        engine = sa.create_engine(
            etl_db_uri,
            echo=log_to_console,
            echo_pool=log_to_console,
            pool_size=max_processes + 3,
            future=True,
            # connect_args={"check_same_thread": False},  # needed for sqlite
        )

        adapter.db.create_tables(engine=engine)
        print("Engine created.")
        log_actor = sa_logger.SALogger.start(engine=engine)
        logger = sa_logger.NamedLogger(
            name="root",
            sql_logger=log_actor,
            log_to_console=log_to_console,
            min_log_level=min_log_level,
        )
        print("Logger started.")
        job_queue_actor = sa_job_queue.SAJobQueue.start(
            engine=engine,
            logger=logger.new(name="JobQueue"),
        ).proxy()
        scheduler_actor = sa_scheduler.SAScheduler.start(
            engine=engine,
            jobs=all_jobs,
            job_queue=job_queue_actor,
            logger=logger.new(name="Scheduler"),
        )
        print("Scheduler started.")
        for i in range(max_processes):
            sa_job_runner.SAJobRunner.start(
                engine=engine,
                scheduler=scheduler_actor,
                logger=logger.new(name=f"JobRunner{i}"),
            )
        print("JobRunners started.")

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
                print(f"Dead Actors: {', '.join(dead_actors)}.")
            time.sleep(10)
            print("Tick")
    finally:
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
