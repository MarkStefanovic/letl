import multiprocessing as mp
import typing

from letl import domain

__all__ = ("JobQueue",)


class JobQueue(domain.JobQueue):
    def __init__(
        self,
        *,
        jobs: typing.List[domain.Job],
        logger: domain.Logger,
    ):
        super().__init__()

        self._jobs = {job.job_name: job for job in jobs}
        self._logger = logger

        self._lock = mp.Lock()
        self._queue: typing.List[str] = []

    def add(self, *, job_name: str) -> None:
        with self._lock:
            if job_name not in self._queue:
                self._queue.append(job_name)
        return None

    def pop(self) -> typing.Optional[domain.Job]:
        with self._lock:
            if self._queue:
                job_name = self._queue.pop(0)
                return self._jobs[job_name]
        return None
