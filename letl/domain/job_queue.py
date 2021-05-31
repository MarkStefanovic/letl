import abc
import typing

from letl.domain.job import Job

__all__ = ("JobQueue",)


class JobQueue(abc.ABC):
    @abc.abstractmethod
    def add(self, *, job_name: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def pop(self) -> typing.Optional[Job]:
        raise NotImplementedError
