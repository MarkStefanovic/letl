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
        engine: sa.engine.Engine,
        jobs: typing.List[domain.Job],
        job_queue: pykka.ActorProxy,
        logger: domain.Logger,
    ):
        super().__init__()

        self._job_registry = {job.job_name: job for job in jobs}
        self._job_queue = job_queue
        self._logger = logger

        self._last_update: typing.Optional[datetime.datetime] = None

        self._status_repo = adapter.SAStatusRepo(engine=engine)

    def on_failure(
        self,
        exception_type: typing.Type[BaseException],
        exception_value: BaseException,
        traceback: TracebackType,
    ) -> None:
        self._logger.exception(exception_value)

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

                    # TODO if the job has dependencies, check if any have been completed since the job was last completed, if not, then skip

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
