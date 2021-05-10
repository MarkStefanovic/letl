import datetime
import multiprocessing
import typing

from letl import adapter, domain
from letl.service import sa_logger

import sqlalchemy as sa

__all__ = ("run_batch",)


def run_batch(
    *,
    batch: typing.List[typing.List[domain.Job]],
    etl_db_uri: str,
    max_processes: int = 3,
    timeout_seconds: int = 3600,
    log_to_console: bool = False,
) -> typing.Set[domain.JobStatus]:
    check_job_names_are_unique(batch=batch)
    batch_id = domain.unique_id.generate()
    results: typing.List[typing.List[domain.JobStatus]] = []
    for group in batch:
        result = run_group(
            batch_id=batch_id,
            group=group,
            etl_db_uri=etl_db_uri,
            max_processes=max_processes,
            timeout_seconds=timeout_seconds,
            log_to_console=log_to_console,
        )
        results.append(result)
    return {r for grp in results for r in grp}


def check_job_names_are_unique(*, batch: typing.List[typing.List[domain.Job]]) -> None:
    jobs_seen = []
    duplicate_job_names = []
    for group in batch:
        for job in group:
            if job.config.job_name in jobs_seen:
                duplicate_job_names.append(job.config.job_name)
            else:
                jobs_seen.append(job.config.job_name)
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
        last_run = status_repo.last_run(job_name=job.job_name)
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
