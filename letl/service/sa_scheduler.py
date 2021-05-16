import datetime
import typing
from types import TracebackType

import pykka
import sqlalchemy as sa

from letl import adapter, domain

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
        self._status_repo: typing.Optional[domain.StatusRepo] = None

    def on_start(self) -> None:
        self._engine = sa.engine.create_engine(self._db_uri, echo=self._echo_sql)
        self._con = self._engine.connect()

        if self._db_uri == "sqlite://":
            self._con.execute("ATTACH ':memory:' as etl")
            adapter.db.create_tables(engine=self._engine)

        self._status_repo = adapter.SAStatusRepo(con=self._con)

    def on_stop(self) -> None:
        self._cleanup()

    def on_failure(
        self,
        exception_type: typing.Type[BaseException],
        exception_value: BaseException,
        traceback: TracebackType,
    ) -> None:
        self._logger.exception(exception_value)
        self._cleanup()

    def _update_queue(self) -> None:
        if (
            self._last_update
            and (datetime.datetime.now() - self._last_update).total_seconds() > 10
        ) or self._last_update is None:
            for job_name, job in self._job_registry.items():
                last_completed = self._status_repo.latest_completed_time(
                    job_name=job.job_name
                )
                if any(s.is_due(last_completed=last_completed) for s in job.schedule):
                    self._logger.debug(f"Adding {job_name} to queue.")
                    self._job_queue.add(job_name=job_name)
            self._last_update = datetime.datetime.now()

    def on_receive(self, n: int) -> typing.List[domain.Job]:
        # self._logger.debug(f"SAScheduler received a request for {n} items.")
        self._update_queue()
        jobs_to_run: typing.List[str] = self._job_queue.pop(n).get()
        try:
            return [self._job_registry[job_name] for job_name in jobs_to_run]
        except KeyError as ke:
            self._logger.exception(ke)

    def _cleanup(self) -> None:
        if self._con:
            self._con.close()
