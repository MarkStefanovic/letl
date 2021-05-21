import typing

import sqlalchemy as sa
from letl import adapter, domain

__all__ = ("delete_orphan_jobs",)


def delete_orphan_jobs(
    *,
    admin_engine: sa.engine.Engine,
    current_jobs: typing.List[domain.Job],
    logger: domain.Logger,
) -> None:
    """Delete job entries that are no longer active or died during a previous run.

    Parameters
    ----------
    admin_engine
        SqlAlchemy engine instance associated with database that stores the ETL logs
    current_jobs
        Jobs that will be sent to job runners to execute
    logger
        domain.Logger that logs messages to the ETL database

    Returns
    -------
    None
    """
    logger.debug("Deleting jobs that are no longer active.")
    active_jobs = {job.job_name for job in current_jobs}
    job_queue_repo = adapter.SAJobQueueRepo(engine=admin_engine)
    jobs_in_queue = job_queue_repo.all()
    inactive_jobs = jobs_in_queue - active_jobs
    for job_name in inactive_jobs:
        logger.debug(f"Removing [{job_name}] from the queue as it is no longer active.")
        job_queue_repo.delete(job_name=job_name)

    logger.debug(
        "Deleting jobs from the queue that are currently running at the same time."
    )
    status_repo = adapter.SAStatusRepo(engine=admin_engine)
    statuses = status_repo.all()
    running_job_names = {s.job_name for s in statuses if s.is_running}
    jobs_in_queue = job_queue_repo.all()
    for job_name in jobs_in_queue & running_job_names:
        job_queue_repo.delete(job_name=job_name)

    logger.debug("Deleting jobs that are past their expiration date.")
    schedules: typing.Dict[str, typing.Set[domain.Schedule]] = {
        job.job_name: job.schedule for job in current_jobs
    }
    for status in statuses:
        job_schedule = schedules[status.job_name]
        if not any(s.is_due(last_completed=status.ended) for s in job_schedule):
            logger.debug(
                f"Deleting [{status.job_name}] from that job_queue and status tables, as it is not "
                f"currently supposed to be running."
            )
            job_queue_repo.delete(job_name=status.job_name)
            status_repo.delete(job_name=status.job_name)
