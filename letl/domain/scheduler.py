import abc
import typing

from letl.domain import job

__all__ = ("Scheduler",)


class Scheduler(abc.ABC):
    @abc.abstractmethod
    def get_ready_jobs(self) -> typing.Set[job.Job]:
        raise NotImplementedError
