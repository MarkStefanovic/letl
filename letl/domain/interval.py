from __future__ import annotations

import abc
import datetime

__all__ = ("Interval",)


class Interval(abc.ABC):
    @staticmethod
    def daily() -> Interval:
        return Daily()

    @staticmethod
    def every_x_seconds(*, seconds: int) -> Interval:
        return EveryXSeconds(seconds=seconds)

    @abc.abstractmethod
    def description(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def next(
        self, last: datetime.datetime, now: datetime.datetime
    ) -> datetime.datetime:
        raise NotImplementedError


class Daily(Interval):
    def description(self) -> str:
        return "daily"

    def next(
        self, last: datetime.datetime, now: datetime.datetime
    ) -> datetime.datetime:
        if last.date() == now.date():
            return now + datetime.timedelta(days=1)
        else:
            return now

    def __repr__(self) -> str:
        return "Daily()"


class EveryXSeconds(Interval):
    def __init__(self, *, seconds: int):
        self._seconds = seconds

    def description(self) -> str:
        return f"every_x_seconds: {self._seconds}"

    def next(
        self, last: datetime.datetime, now: datetime.datetime
    ) -> datetime.datetime:
        seconds_since_last_run = (now - last).total_seconds()
        if seconds_since_last_run > self._seconds:
            return now
        else:
            return last + datetime.timedelta(seconds=self._seconds)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(seconds={self._seconds})"


if __name__ == "__main__":
    d = Daily()
    print(d.description)
