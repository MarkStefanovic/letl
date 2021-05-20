import abc
import datetime
import typing

from letl.domain import log_level

__all__ = ("LogRepo",)


class LogRepo(abc.ABC):
    @abc.abstractmethod
    def add(
        self,
        *,
        name: str,
        level: log_level.LogLevel,
        message: str,
        ts: typing.Optional[datetime.datetime] = None,
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_before(self, /, ts: datetime.datetime) -> None:
        raise NotImplementedError
