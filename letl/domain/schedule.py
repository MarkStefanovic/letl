from __future__ import annotations

import abc
import dataclasses
import datetime
import typing

from letl.domain.weekday import Weekday

__all__ = (
    "EveryXSeconds",
    "Schedule",
)


class Schedule(abc.ABC):
    @abc.abstractmethod
    def is_due(self, *, last_completed: typing.Optional[datetime.datetime]) -> bool:
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class EveryXSeconds(Schedule):
    start: datetime.datetime
    seconds: int
    start_month: int = 1
    end_month: int = 12
    start_monthday: int = 1
    end_monthday: int = 31
    start_weekday: int = Weekday.Mon
    end_weekday: int = Weekday.Sun
    start_hour: int = 0
    start_minute: int = 0
    end_hour: int = 23
    end_minute: int = 59

    def is_due(self, *, last_completed: typing.Optional[datetime.datetime]) -> bool:
        now = datetime.datetime.now()
        if now.month < self.start_month or now.month > self.end_month:
            return False
        elif now.day < self.start_monthday or now.day > self.end_monthday:
            return False
        elif (
            now.isoweekday() < self.start_weekday or now.isoweekday() > self.end_weekday
        ):
            return False
        elif now.hour < self.start_hour or now.hour > self.end_hour:
            return False
        elif now.minute < self.start_minute or now.minute > self.end_minute:
            return False
        else:
            if last_completed is None:
                return now >= self.start
            else:
                seconds_since_last_run = (last_completed - self.start).total_seconds()
                if seconds_since_last_run > self.seconds:
                    next_due = now
                else:
                    next_due = last_completed + datetime.timedelta(seconds=self.seconds)

                if datetime.datetime.now() >= next_due:
                    return True
                else:
                    return False

    #
    # def __init__(
    #     self,
    #     *,
    #     start: datetime.datetime,
    #     seconds: int,
    #     start_month: int = 1,
    #     end_month: int = 12,
    #     start_monthday: int = 1,
    #     end_monthday: int = 31,
    #     start_weekday: int = 1,
    #     end_weekday: int = 7,
    #     start_hour: int = 0,
    #     start_minute: int = 0,
    #     end_hour: int = 23,
    #     end_minute: int = 59,
    # ):
    #     self._start = start
    #     self._seconds = seconds
    #     self._start_month = start_month
    #     self._end_month = end_month
    #     self._start_monthday = start_monthday
    #     self._end_monthday = end_monthday
    #     self._start_weekday = start_weekday
    #     self._end_weekday = end_weekday
    #     self._start_hour = start_hour
    #     self._start_minute = start_minute
    #     self._end_hour = end_hour
    #     self._end_minute = end_minute

    # def is_due(self, *, last_completed: typing.Optional[datetime.datetime]) -> bool:
    #     now = datetime.datetime.now()
    #     if now.month < self._start_month or now.month > self._end_month:
    #         return False
    #     elif now.day < self._start_monthday or now.day > self._end_monthday:
    #         return False
    #     elif (
    #         now.isoweekday() < self._start_weekday
    #         or now.isoweekday() > self._end_weekday
    #     ):
    #         return False
    #     elif now.hour < self._start_hour or now.hour > self._end_hour:
    #         return False
    #     elif now.minute < self._start_minute or now.minute > self._end_minute:
    #         return False
    #     else:
    #         if last_completed is None:
    #             return now >= self._start
    #         else:
    #             seconds_since_last_run = (last_completed - self._start).total_seconds()
    #             if seconds_since_last_run > self._seconds:
    #                 next_due = now
    #             else:
    #                 next_due = last_completed + datetime.timedelta(
    #                     seconds=self._seconds
    #                 )
    #
    #             if datetime.datetime.now() >= next_due:
    #                 return True
    #             else:
    #                 return False


if __name__ == "__main__":
    print(f"Fri: {datetime.date(2021, 5, 7).isoweekday()=}")
    print(f"Sat: {datetime.date(2021, 5, 8).isoweekday()=}")
    print(f"Sun: {datetime.date(2021, 5, 9).isoweekday()=}")
    print(f"Mon: {datetime.date(2021, 5, 10).isoweekday()=}")
    print(f"Tue: {datetime.date(2021, 5, 11).isoweekday()=}")
    print(f"Wed: {datetime.date(2021, 5, 12).isoweekday()=}")

    x = EveryXSeconds(start=datetime.datetime.now(), seconds=30)
    print(x)
    print(f"{x.is_due(last_completed=None)}")
