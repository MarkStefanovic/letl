import abc
import datetime

from letl.domain import log_level

__all__ = ("LogRepo",)


class LogRepo(abc.ABC):
    @abc.abstractmethod
    def add(
        self, *, batch_id: str, job_name: str, level: log_level.LogLevel, message: str
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_before(self, /, ts: datetime.datetime) -> None:
        raise NotImplementedError
