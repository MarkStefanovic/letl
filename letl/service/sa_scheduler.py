import datetime
import typing

import pykka
import sqlalchemy as sa

from letl import adapter, domain

__all__ = ("update_queue",)


def update_queue(
    *,
    engine: sa.engine.Engine,
    job_queue: pykka.ActorProxy,
    jobs: typing.List[domain.Job],
    logger: domain.Logger,
) -> None:
    status_repo = adapter.SAStatusRepo(engine=engine)
    job_map = {job.job_name: job for job in jobs}
    for job_name, job in job_map.items():
        logger.debug(f"Checking if [{job_name}] is ready...")
        if job_is_ready_to_run(job=job, status_repo=status_repo):
            logger.debug(f"Adding [{job_name}] to queue.")
            job_queue.add(job_name=job_name)
        else:
            logger.debug(f"[{job_name}] was skipped.")


def job_is_ready_to_run(
    *,
    job: domain.Job,
    status_repo: domain.StatusRepo,
) -> bool:
    status = status_repo.status(job_name=job.job_name)
    if status:
        last_started: typing.Optional[datetime.datetime] = status.started
        last_completed: typing.Optional[datetime.datetime] = status.ended
    else:
        last_started = None
        last_completed = None

    if last_started and status and status.is_running:
        seconds_since_started = (datetime.datetime.now() - last_started).total_seconds()
        if seconds_since_started < job.timeout_seconds:
            return False

    if dependencies_have_run(
        status_repo=status_repo,
        job_last_run=last_completed,
        dependencies=job.dependencies,
    ):
        if any(s.is_due(last_completed=last_completed) for s in job.schedule):
            return True

    return False


def dependencies_have_run(
    *,
    status_repo: domain.StatusRepo,
    job_last_run: typing.Optional[datetime.datetime],
    dependencies: typing.Set[str],
) -> bool:
    for dep in dependencies:
        dep_status = status_repo.status(job_name=dep)
        if dep_status:
            if dep_status.is_running:
                return False

            if job_last_run and dep_status.ended and dep_status.ended < job_last_run:
                return False
        else:
            return False

    return True
