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
        db_uri: str,
        scheduler: pykka.ActorRef,
        logger: domain.Logger,
        echo_sql: bool = False,
    ):
        super().__init__()

        self._db_uri = db_uri
        self._scheduler = scheduler
        self._logger = logger
        self._echo_sql = echo_sql

        self._engine: typing.Optional[sa.engine.Engine] = None
        self._con: typing.Optional[sa.engine.Connection] = None
        self._status_repo: typing.Optional[domain.StatusRepo] = None

    def on_start(self) -> None:
        self._engine = sa.engine.create_engine(self._db_uri, echo=self._echo_sql)
        self._con = self._engine.connect()

        if self._db_uri == "sqlite://":
            self._con.execute("ATTACH ':memory:' as etl")
            adapter.db.create_tables(engine=self._engine)

        self._status_repo = adapter.SAStatusRepo(con=self._con)
        while True:
            jobs: domain.Job = self._scheduler.ask(1, block=True)
            if jobs:
                job = jobs[0]
                self._logger.debug(f"running job: {job}")
                job_status_id: int = self._status_repo.start(job_name=job.job_name)
                result: domain.JobResult = run_job_with_retry(
                    job=job,
                    logger=self._logger.new(name=job.job_name),
                    retries_so_far=0,
                )
                if result.is_error:
                    self._status_repo.error(
                        job_status_id=job_status_id,
                        error=result.error_message,
                    )
                else:
                    self._status_repo.done(job_status_id=job_status_id)
            else:
                time.sleep(1)

    def on_stop(self) -> None:
        self._cleanup()

    def on_failure(
        self,
        exception_type: typing.Type[BaseException],
        exception_value: BaseException,
        traceback: TracebackType,
    ) -> None:
        print(exception_value)
        self._cleanup()

    # def on_receive(self, message: domain.Job) -> domain.JobStatus:
    #     job = self._scheduler.ask(1, block=True)[0]
    #     return run_job(job=job)

    def _cleanup(self) -> None:
        self._logger.debug("Cleaning up...")
        if self._con:
            self._con.close()


# noinspection PyBroadException
def run_job_with_retry(
    job: domain.Job,
    logger: domain.Logger,
    retries_so_far: int = 0,
) -> domain.JobResult:
    try:
        job.run(job.config, logger)
        return domain.JobResult.success()
    except Exception as e:
        if job.retries > retries_so_far:
            return run_job_with_retry(
                job=job,
                logger=logger,
                retries_so_far=retries_so_far + 1,
            )
        else:
            return domain.JobResult.error(e)
