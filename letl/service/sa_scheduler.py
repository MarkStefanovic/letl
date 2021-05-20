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
        logger.debug(f"Checking if {job_name} is ready...")
        status = status_repo.status(job_name=job.job_name)
        if status:
            if status.is_running:
                logger.debug(f"Skipping {job_name}...")
                continue
            else:
                last_completed = status.ended
        else:
            last_completed = None
        if last_completed:
            logger.debug(
                f"{job_name} was last attempted at {last_completed.strftime('%Y-%m-%d %H:%M:%S')}."
            )
        else:
            logger.debug(f"{job_name} has not been run before.")
        if any(s.is_due(last_completed=last_completed) for s in job.schedule):
            # TODO if the job has dependencies, check if any have been completed since the job was last
            #  completed, if not, then skip
            logger.debug(f"Adding {job_name} to queue.")
            job_queue.add(job_name=job_name)
        else:
            logger.debug(f"{job_name} was skipped.")
