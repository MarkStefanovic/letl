import abc
import typing

from letl.domain.job import Job
from letl.domain.schedule import Schedule

__all__ = ("ScheduleRepo",)


class ScheduleRepo(abc.ABC):
    @abc.abstractmethod
    def add(self, *, job_name: str, schedule: Schedule) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def clear(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, *, job_name: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def ready_jobs(self) -> typing.Set[Job]:
        raise NotImplementedError
