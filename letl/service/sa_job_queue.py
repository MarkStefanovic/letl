import typing
from types import TracebackType

import pykka
import sqlalchemy as sa

from letl import domain, adapter

__all__ = ("SAJobQueue",)


class SAJobQueue(pykka.ThreadingActor):
    def __init__(
        self,
        *,
        db_uri: str,
        logger: domain.Logger,
        echo_sql: bool = False,
    ):
        super().__init__()

        self._db_uri = db_uri
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

    def add(self, *, job_name: str) -> None:
        queue = self._repo.all()
        if job_name in queue:
            return
        else:
            self._repo.add(job_name=job_name)

    def pop(self, n: int) -> typing.List[str]:
        return self._repo.pop(n)

    def _cleanup(self) -> None:
        if self._con:
            self._con.close()
        self._job_registry = None
        self._con = None
        self._engine = None
        self._repo = None
