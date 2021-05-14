import typing
from types import TracebackType

import pykka
import sqlalchemy as sa

from letl import domain, adapter

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
        self._repo: typing.Optional[domain.JobQueueRepo] = None

    def on_start(self) -> None:
        self._engine = sa.engine.create_engine(self._db_uri, echo=self._echo_sql)
        self._con = self._engine.connect()
        self._repo = adapter.SAJobQueueRepo(con=self._con)

    def on_stop(self) -> None:
        self._cleanup()

    def on_failure(
        self,
        exception_type: typing.Type[BaseException],
        exception_value: BaseException,
        traceback: TracebackType,
    ) -> None:
        self._cleanup()

    def on_receive(self, message: domain.Job) -> domain.JobStatus:
        job = self._scheduler.ask(1, block=True)[0]
        return run_job(job=job)

    def _cleanup(self) -> None:
        if self._con:
            self._con.close()
        self._job_registry = None
        self._con = None
        self._engine = None
        self._repo = None
