import abc
import typing

from letl.domain.job import Job

__all__ = ("JobQueueRepo",)


class JobQueueRepo(abc.ABC):
    @abc.abstractmethod
    def add(self, *, job_name: str) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def all(self) -> typing.List[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def clear(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, *, job_name: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def pop(self, n: int) -> typing.List[str]:
        raise NotImplementedError
