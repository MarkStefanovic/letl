import abc
import typing

from letl.domain.job import Job

__all__ = ("Scheduler",)


class Scheduler(abc.ABC):
    @abc.abstractmethod
    def get_ready_jobs(self) -> typing.Set[Job]:
        raise NotImplementedError
