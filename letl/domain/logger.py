from __future__ import annotations

import abc
import typing

from letl.domain.log_level import LogLevel

__all__ = ("Logger",)


class Logger(abc.ABC):
    @abc.abstractmethod
    def debug(self, /, message: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def error(self, /, message: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def exception(self, /, e: BaseException) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def info(self, /, message: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def new(
        self,
        *,
        name: str,
        log_to_console: typing.Optional[bool] = None,
        min_log_level: typing.Optional[LogLevel] = None,
    ) -> Logger:
        raise NotImplementedError
