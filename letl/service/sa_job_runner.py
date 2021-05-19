import typing

import pykka
import sqlalchemy as sa

from letl import adapter, domain

__all__ = ("run_job",)


def run_job(
    *,
    engine: sa.engine.Engine,
    job_queue: pykka.ActorProxy,
    # jobs: typing.List[domain.Job],
    logger: domain.Logger,
) -> None:
    status_repo = adapter.SAStatusRepo(engine=engine)
    # job_queue_repo = adapter.SAJobQueueRepo(engine=engine)
    # job_map = {job.job_name: job for job in jobs}
    # pending_jobs = job_queue.pop(1)
    job = job_queue.pop().get()
    if job:
        logger.info(f"Running {job.job_name}")
        # TODO run run_job_with_retry in separate process and wait to complete, timeout = job.timeout
        status_repo.start(job_name=job.job_name)
        result = run_job_with_retry(
            job=job,
            logger=logger.new(name=job.job_name),
            retries_so_far=0,
        )
        logger.debug(f"Saving results of {job.job_name} to database")
        if result.is_error:
            err_msg = result.error_message or "No error message was provided."
            status_repo.error(job_name=job.job_name, error=err_msg)
            logger.error(err_msg)
        else:
            status_repo.done(job_name=job.job_name)
            logger.info(f"{job.job_name} finished.")


# noinspection PyBroadException
def run_job_with_retry(
    job: domain.Job,
    logger: domain.Logger,
    retries_so_far: int = 0,
) -> domain.JobResult:
    try:
        result = job.run(job.config, logger)
        if result is None:
            return domain.JobResult.success()
        else:
            return result
    except Exception as e:
        if job.retries > retries_so_far:
            return run_job_with_retry(
                job=job,
                logger=logger,
                retries_so_far=retries_so_far + 1,
            )
        else:
            return domain.JobResult.error(e)
