from __future__ import annotations

import abc
import datetime
import typing

from letl.domain.log_level import LogLevel

__all__ = ("Logger",)


class Logger(abc.ABC):
    @abc.abstractmethod
    def debug(
        self, /, message: str, *, ts: typing.Optional[datetime.datetime] = None
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def error(
        self, /, message: str, *, ts: typing.Optional[datetime.datetime] = None
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def exception(
        self, /, e: Exception, *, ts: typing.Optional[datetime.datetime] = None
    ) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def info(
        self, /, message: str, *, ts: typing.Optional[datetime.datetime] = None
    ) -> None:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def name(self) -> str:
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
