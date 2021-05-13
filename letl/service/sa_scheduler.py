import typing
from types import TracebackType

import pykka
import sqlalchemy as sa

from letl import domain, adapter

__all__ = ("SAScheduler",)

mod_logger = domain.root_logger.getChild("sa_scheduler")


class SAScheduler(domain.Scheduler, pykka.ThreadingActor):
    def __init__(self):
        super().__init__()

        self._logger = mod_logger.getChild(self.__class__.__name__)

        self._engine: typing.Optional[sa.engine.Engine] = None
        self._con: typing.Optional[sa.engine.Connection] = None
        self._job_queue: typing.Optional[domain.JobQueue] = None
        self._status_repo: typing.Optional[domain.StatusRepo] = None

    def on_start(self) -> None:
        self._logger.debug("on_start()")
        self._engine = adapter

    def on_stop(self) -> None:
        self._logger.debug("on_stop()")
        self._cleanup()

    def on_failure(
        self,
        exception_type: typing.Type[BaseException],
        exception_value: BaseException,
        traceback: TracebackType,
    ) -> None:
        self._logger.error(f"on_failure(...): {exception_value}")
        self._cleanup()

    def on_receive(self, message: None) -> typing.Set[domain.Job]:
        pass

    def _cleanup(self) -> None:
        if self._con:
            self._con.close()
        self._status_repo = None
        self._job_queue = None
        self._con = None
        self._engine = None
