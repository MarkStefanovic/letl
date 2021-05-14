import dataclasses
import datetime
import multiprocessing
import time
import typing

from letl import adapter, domain
from letl.service import sa_logger

import sqlalchemy as sa

__all__ = ("run_jobs",)


def run(
    *,
    jobs: typing.Set[domain.Job],
    etl_db_uri: str,
    max_processes: int = 3,
    timeout_seconds: int = 3600,
    log_to_console: bool = False,
    tick_seconds: int = 5,
) -> typing.Set[domain.JobStatus]:
    check_job_names_are_unique(jobs=jobs)
    batch_id = domain.unique_id.generate()
    results: typing.List[typing.List[domain.JobStatus]] = []
    start = datetime.datetime.now()
    elapsed_seconds = 0
    while jobs:
        if elapsed_seconds > timeout_seconds:
            break
        ready_jobs: typing.List[domain.Job] = []
        for job in jobs:
            if job_is_ready(con=con, job=job):
                jobs.remove(job)
                ready_jobs.append(job)
        if ready_jobs:
            seconds_remaining = int(
                timeout_seconds - (datetime.datetime.now() - start).total_seconds()
            )
            result = run_group(
                batch_id=batch_id,
                group=ready_jobs,
                etl_db_uri=etl_db_uri,
                max_processes=max_processes,
                timeout_seconds=seconds_remaining,
                log_to_console=log_to_console,
            )
            results.append(result)
        else:
            time.sleep(tick_seconds)
        elapsed_seconds = (datetime.datetime.now() - start).total_seconds()
    pykka.ActorRegistry.stop_all()



def start_planner(*, etl_db_uri: str) -> None:


def start_job_runner(*, etl_db_uri: str) -> None:



def create_plans(*, con: sa.engine.Connection, jobs: typing.List[domain.Job]) -> Plan:
    repo = adapter.SAStatusRepo(con=con)
    plans: typing.List[Plan] = []
    for job in jobs:
        status: typing.Optional[domain.JobStatus] = repo.latest_status(
            job_name=job.job_name
        )
        if status.is_running:
            plan = Plan.skip("The job is already running.")
            plans.append(plan)
        elif status.is_error:
            plan = Plan(
                run=True,
                skip=False,
                skip_reason=None
            )
        elif status.is_skipped:

        else:
            raise NotImplementedError()


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


def run_group(
    *,
    batch_id: str,
    group: typing.List[domain.Job],
    etl_db_uri: str,
    max_processes: int,
    timeout_seconds: int,
    log_to_console: bool,
) -> typing.List[domain.JobStatus]:
    params = [(batch_id, job, etl_db_uri, log_to_console) for job in group]
    with multiprocessing.Pool(max_processes, maxtasksperchild=1) as pool:
        future = pool.starmap_async(run_job, params)
        return future.get(timeout_seconds)


def run_job(
    batch_id: str,
    job: domain.Job,
    etl_db_uri: str,
    log_to_console: bool,
) -> domain.JobStatus:
    engine = sa.create_engine(url=etl_db_uri, echo=log_to_console)
    with engine.connect() as con:
        logger = sa_logger.SALogger(
            batch_id=batch_id,
            job_name=job.config.job_name,
            con=con,
            log_to_console=log_to_console,
        )
        status_repo = adapter.SAStatusRepo(con=con)
        last_run = status_repo.latest_status(job_name=job.job_name)
        job_status_id = status_repo.start(batch_id=batch_id, job_name=job.job_name)
        if last_run:
            seconds_since_last_run = (
                datetime.datetime.now() - last_run
            ).total_seconds()
            if seconds_since_last_run <= job.min_seconds_between_refreshes:
                skipped_reason = (
                    f"The job was run {seconds_since_last_run} seconds ago, and it is set "
                    f"to run every {job.min_seconds_between_refreshes} seconds."
                )
                status_repo.skipped(job_status_id=job_status_id, reason=skipped_reason)
                return domain.JobStatus.skipped(
                    batch_id=batch_id,
                    job_name=job.job_name,
                    reason=skipped_reason,
                )

        try:
            start = datetime.datetime.now()
            run_job_with_retry(batch_id=batch_id, job=job, logger=logger)
            execution_millis = int(
                (datetime.datetime.now() - start).total_seconds() * 1000
            )
            status_repo.done(job_status_id=job_status_id)
            return domain.JobStatus.success(
                batch_id=batch_id,
                job_name=job.config.job_name,
                execution_millis=execution_millis,
            )
        except Exception as e:
            error_message = domain.error.parse_exception(e).text()
            status_repo.error(job_status_id=job_status_id, error=error_message)
            logger.exception(e)
            return domain.JobStatus.error(
                batch_id=batch_id,
                job_name=job.config.job_name,
                error=error_message,
            )


# noinspection PyBroadException
def run_job_with_retry(
    batch_id: str,
    job: domain.Job,
    logger: domain.Logger,
    retries_so_far: int = 0,
) -> domain.JobStatus:
    try:
        job.run(job.config, logger)
    except:
        if job.retries > retries_so_far:
            return run_job_with_retry(
                batch_id=batch_id,
                job=job,
                logger=logger,
                retries_so_far=retries_so_far + 1,
            )
        else:
            raise
