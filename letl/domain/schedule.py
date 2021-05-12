from __future__ import annotations

import abc
import datetime
import typing

__all__ = (
    "EveryXSeconds",
    "Schedule",
)


class Schedule(abc.ABC):
    @abc.abstractmethod
    def is_due(self, *, last_completed: typing.Optional[datetime.datetime]) -> bool:
        raise NotImplementedError


class EveryXSeconds(Schedule):
    def __init__(
        self,
        *,
        start: datetime.datetime,
        seconds: int,
        timeout: int,
        start_monthday: int = 1,
        end_monthday: int = 31,
        start_weekday: int = 1,
        end_weekday: int = 7,
        start_hour: int = 0,
        start_minute: int = 0,
        end_hour: int = 23,
        end_minute: int = 59,
    ):
        self._start = start
        self._seconds = seconds
        self._timeout = timeout
        self._start_monthday = start_monthday
        self._end_monthday = end_monthday
        self._start_weekday = start_weekday
        self._end_weekday = end_weekday
        self._start_hour = start_hour
        self._start_minute = start_minute
        self._end_hour = end_hour
        self._end_minute = end_minute

    def is_due(self, *, last_completed: typing.Optional[datetime.datetime]) -> bool:
        now = datetime.datetime.now()
        if now.day < self._start_monthday or now.day > self._end_monthday:
            return False
        elif now.weekday() < self._start_weekday or now.weekday() > self._end_weekday:
            return False
        else:
            if last_completed is None:
                return datetime.datetime.now() >= self._start
            else:
                seconds_since_last_run = (last_completed - self._start).total_seconds()
                if seconds_since_last_run > self._seconds:
                    next_due = datetime.datetime.now()
                else:
                    next_due = last_completed + datetime.timedelta(
                        seconds=self._seconds
                    )

                if datetime.datetime.now() >= next_due:
                    return True
                else:
                    return False
