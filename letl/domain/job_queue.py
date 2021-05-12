import abc
import typing

from letl import Job

__all__ = ("JobQueue",)


class JobQueue(abc.ABC):
    @abc.abstractmethod
    def add(self, *, job_name: str) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def clear(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def pop(self, n: int) -> typing.Set[Job]:
        raise NotImplementedError
