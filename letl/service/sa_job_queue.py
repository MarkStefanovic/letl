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
        jobs: typing.List[domain.Job],
        logger: domain.Logger,
    ):
        super().__init__()

        self._engine = engine
        self._jobs = {job.job_name: job for job in jobs}
        self._logger = logger

        self._repo = adapter.SAJobQueueRepo(engine=self._engine)

    def on_failure(
        self,
        exception_type: typing.Type[BaseException],
        exception_value: BaseException,
        traceback: TracebackType,
    ) -> None:
        self._logger.exception(exception_value)

    def add(self, *, job_name: str) -> None:
        self._logger.debug(f"{job_name} added to queue.")
        repo = adapter.SAJobQueueRepo(engine=self._engine)
        repo.add(job_name=job_name)

    def pop(self) -> typing.Optional[domain.Job]:
        result = self._repo.pop(1)
        if result:
            job_name = result[0]
            return self._jobs[job_name]
        return None


# class JobQueue:
#     def __init__(self, *, actor: pykka.ActorRef):
#         self._actor = actor
#
#     def add(self, /, name: str) -> None:
#         self._actor.ask(message=msg, block=True)
