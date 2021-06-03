import datetime
import queue
import threading
import time
import typing

import sqlalchemy as sa

from letl import adapter, domain

__all__ = ("Scheduler",)


class Scheduler(threading.Thread):
    def __init__(
        self,
        *,
        engine: sa.engine.Engine,
        job_queue: "queue.Queue[domain.Job]",
        jobs: typing.List[domain.Job],
        logger: domain.Logger,
        seconds_between_scans: int,
    ):
        super().__init__()

        self._engine = engine
        self._job_queue = job_queue
        self._jobs = jobs
        self._logger = logger
        self._seconds_between_scans = seconds_between_scans

    def run(self) -> None:
        while True:
            try:
                update_queue(
                    engine=self._engine,
                    job_queue=self._job_queue,
                    jobs=self._jobs,
                    logger=self._logger,
                )
            except Exception as e:
                self._logger.exception(e)

            time.sleep(self._seconds_between_scans)


def update_queue(
    *,
    engine: sa.engine.Engine,
    job_queue: "queue.Queue[domain.Job]",
    jobs: typing.List[domain.Job],
    logger: domain.Logger,
) -> None:
    print(f"{datetime.datetime.now()}: running update_queue")
    status_repo = adapter.SAStatusRepo(engine=engine)
    job_map = {job.job_name: job for job in jobs}
    for job_name, job in job_map.items():
        logger.debug(f"Checking if [{job_name}] is ready...")
        if job_is_ready_to_run(job=job, status_repo=status_repo):
            logger.debug(f"Adding [{job_name}] to queue.")
            job_queue.put(job_map[job_name])
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
        if seconds_since_started < (job.timeout_seconds + 10):
            return False

    if job.dependencies:
        if not dependencies_have_run(
            status_repo=status_repo,
            job_last_run=last_completed,
            dependencies=job.dependencies,
        ):
            return False

    return any(s.is_due(last_completed=last_completed) for s in job.schedule)


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
