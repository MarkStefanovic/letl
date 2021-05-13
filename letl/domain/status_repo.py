import abc
import datetime
import typing

__all__ = ("StatusRepo",)


class StatusRepo(abc.ABC):
    @abc.abstractmethod
    def done(self, *, job_status_id: int) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def error(self, *, job_status_id: int, error: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def skipped(self, *, job_status_id: int, reason: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def start(self, *, job_name: str) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_before(self, /, ts: datetime.datetime) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def latest_completed_time(
        self, *, job_name: str
    ) -> typing.Optional[datetime.datetime]:
        raise NotImplementedError

    @abc.abstractmethod
    def latest_status(self, *, job_name: str) -> datetime.datetime:
        raise NotImplementedError
