import abc
import datetime
import typing

__all__ = ("StatusRepo",)


class StatusRepo(abc.ABC):
    @abc.abstractmethod
    def done(self, *, job_name: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def error(self, *, job_name: str, error: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def skipped(self, *, job_name: str, reason: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def start(self, *, job_name: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, *, job_name: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_before(self, /, ts: datetime.datetime) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def status(self, *, job_name: str) -> datetime.datetime:
        raise NotImplementedError
