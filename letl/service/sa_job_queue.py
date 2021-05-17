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
        engine: sa.engine.Engine,
        logger: domain.Logger,
        echo_sql: bool = False,
    ):
        super().__init__()

        self._logger = logger
        self._echo_sql = echo_sql

        self._repo = adapter.SAJobQueueRepo(engine=engine)

    def on_failure(
        self,
        exception_type: typing.Type[BaseException],
        exception_value: BaseException,
        traceback: TracebackType,
    ) -> None:
        self._logger.exception(exception_value)

    def add(self, *, job_name: str) -> None:
        self._logger.debug(f"{job_name} added to queue.")
        self._repo.add(job_name=job_name)

    def pop(self, n: int) -> typing.List[str]:
        return self._repo.pop(n)
