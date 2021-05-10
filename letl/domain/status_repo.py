import abc
import datetime

__all__ = ("JobStatusRepo",)


class JobStatusRepo(abc.ABC):
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
    def start(self, *, batch_id: str, job_name: str) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_before(self, /, ts: datetime.datetime) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def last_run(self, *, job_name: str) -> datetime.datetime:
        raise NotImplementedError
