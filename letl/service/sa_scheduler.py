import collections
import datetime
import typing
from types import TracebackType

import pykka
import sqlalchemy as sa

from letl import domain, adapter

__all__ = ("SAScheduler",)


class SAScheduler(pykka.ThreadingActor):
    def __init__(
        self,
        *,
        db_uri: str,
        jobs: typing.List[domain.Job],
        job_queue: pykka.ActorProxy,
        logger: domain.Logger,
        echo_sql: bool = False,
    ):
        super().__init__()

        self._db_uri = db_uri
        self._job_registry = {job.job_name: job for job in jobs}
        self._job_queue = job_queue
        self._logger = logger
        self._echo_sql = echo_sql

        self._last_update: typing.Optional[datetime.datetime] = None

        self._engine: typing.Optional[sa.engine.Engine] = None
        self._con: typing.Optional[sa.engine.Connection] = None
        self._schedule_repo: typing.Optional[domain.ScheduleRepo] = None
        self._status_repo: typing.Optional[domain.StatusRepo] = None

    def on_start(self) -> None:
        self._engine = sa.engine.create_engine(self._db_uri, echo=self._echo_sql)
        self._con = self._engine.connect()
        # self._job_queue = adapter.SAJobQueueRepo(con=self._con)
        self._schedule_repo = adapter.SAScheduleRepo(con=self._con)
        self._status_repo = adapter.SAStatusRepo(con=self._con)

    def on_stop(self) -> None:
        self._cleanup()

    def on_failure(
        self,
        exception_type: typing.Type[BaseException],
        exception_value: BaseException,
        traceback: TracebackType,
    ) -> None:
        self._cleanup()

    def _update_queue(self) -> None:
        if (
            self._last_update
            and (datetime.datetime.now() - self._last_update).total_seconds() > 10
        ):
            ready_jobs = self._schedule_repo.ready_jobs()
            ready_job_names = {job.job_name for job in ready_jobs}
            for job_name in ready_job_names:
                self._job_queue.add(job_name=job_name)
            self._last_update = datetime.datetime.now()

    def on_receive(self, n: int) -> typing.List[domain.Job]:
        jobs_to_run = self._job_queue.pop(n)
        return [self._job_registry[job_name] for job_name in jobs_to_run]

    def _cleanup(self) -> None:
        if self._con:
            self._con.close()
        self._job_registry = None
        self._status_repo = None
        self._job_queue = None
        self._con = None
        self._engine = None
