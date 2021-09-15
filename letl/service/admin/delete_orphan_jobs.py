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
    status_repo = adapter.DbStatusRepo(engine=admin_engine)
    statuses = status_repo.all()
    job_names = {s.job_name for s in statuses}
    orphan_jobs = job_names - active_jobs
    for job_name in orphan_jobs:
        logger.debug(f"Removing [{job_name}] from the queue as it is no longer active.")
        status_repo.delete(job_name=job_name)

    logger.debug(
        "Deleting jobs from the queue that are currently running at the same time."
    )
    running_job_names = {s.job_name for s in statuses if s.is_running}
    for job_name in running_job_names:
        status_repo.delete(job_name=job_name)

    logger.debug("Deleting jobs that are past their expiration date.")
    schedules: typing.Dict[str, typing.FrozenSet[domain.Schedule]] = {
        job.job_name: job.schedule for job in current_jobs
    }
    for status in statuses:
        job_schedule = schedules.get(status.job_name)
        if job_schedule is None:
            logger.info(
                f"The job, [{status.job_name}], has no schedule associated with it."
            )
            status_repo.delete(job_name=status.job_name)
