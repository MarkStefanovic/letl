import abc
import datetime
import typing

from letl.domain import job_status

__all__ = ("StatusRepo",)


class StatusRepo(abc.ABC):
    @abc.abstractmethod
    def all(self) -> typing.Set[job_status.JobStatus]:
        raise NotImplementedError

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
    def status(self, *, job_name: str) -> typing.Optional[job_status.JobStatus]:
        raise NotImplementedError
