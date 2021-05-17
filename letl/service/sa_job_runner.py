import time
import typing
from types import TracebackType

import pykka
import sqlalchemy as sa

from letl import adapter, domain

__all__ = ("SAJobRunner",)


class SAJobRunner(pykka.ThreadingActor):
    def __init__(
        self,
        *,
        engine: sa.engine.Engine,
        scheduler: pykka.ActorRef,
        logger: domain.Logger,
    ):
        super().__init__()

        self._engine = engine
        self._scheduler = scheduler
        self._logger = logger

        self._status_repo = adapter.SAStatusRepo(engine=self._engine)
        self._status = "Initializing"

    def on_start(self) -> None:
        while True:
            self._status = "Asking scheduler for a job to run"
            self._logger.debug(self._status)
            jobs: typing.List[domain.Job] = self._scheduler.ask(1, block=True)
            if jobs:
                job = jobs[0]
                config = job.config
                config["etl_engine"] = self._engine
                self._status = f"Running {job.job_name}"
                self._logger.info(self._status)
                job_status_id: int = self._status_repo.start(job_name=job.job_name)
                result: domain.JobResult = run_job_with_retry(
                    job=job,
                    config=config,
                    logger=self._logger.new(name=job.job_name),
                    retries_so_far=0,
                )
                self._status = f"Saving results of {job.job_name} to database"
                self._logger.debug(self._status)
                if result.is_error:
                    err_msg = result.error_message or "No error message was provided."
                    self._status_repo.error(job_status_id=job_status_id, error=err_msg)
                    self._logger.error(err_msg)
                else:
                    self._status_repo.done(job_status_id=job_status_id)
                    self._logger.info(f"{job.job_name} finished.")
            else:
                time.sleep(1)
            self._status = "Waiting for a job to appear in the queue"
            self._logger.debug(self._status)

    def on_failure(
        self,
        exception_type: typing.Type[BaseException],
        exception_value: BaseException,
        traceback: TracebackType,
    ) -> None:
        self._logger.exception(exception_value)


# noinspection PyBroadException
def run_job_with_retry(
    job: domain.Job,
    config: typing.Dict[str, typing.Any],
    logger: domain.Logger,
    retries_so_far: int = 0,
) -> domain.JobResult:
    try:
        job.run(config, logger)
        return domain.JobResult.success()
    except Exception as e:
        if job.retries > retries_so_far:
            return run_job_with_retry(
                job=job,
                config=config,
                logger=logger,
                retries_so_far=retries_so_far + 1,
            )
        else:
            return domain.JobResult.error(e)
