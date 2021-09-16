from __future__ import annotations

import dataclasses
import datetime
import typing

from letl.domain import interval, weekday

__all__ = ("Schedule",)


@dataclasses.dataclass(frozen=True, eq=True)
class Schedule:
    start: typing.Optional[datetime.datetime]
    start_month: int
    end_month: int
    start_monthday: int
    end_monthday: int
    start_weekday: int
    end_weekday: int
    start_hour: int
    start_minute: int
    end_hour: int
    end_minute: int
    interval: interval.Interval

    def between(
        self,
        *,
        start_month: typing.Optional[int] = None,
        end_month: typing.Optional[int] = None,
        start_monthday: typing.Optional[int] = None,
        end_monthday: typing.Optional[int] = None,
        start_weekday: typing.Optional[weekday.Weekday] = None,
        end_weekday: typing.Optional[weekday.Weekday] = None,
        start_hour: typing.Optional[int] = None,
        start_minute: typing.Optional[int] = None,
        end_hour: typing.Optional[int] = None,
        end_minute: typing.Optional[int] = None,
    ) -> Schedule:
        overrides = {
            "start_month": start_month,
            "end_month": end_month,
            "start_monthday": start_monthday,
            "end_monthday": end_monthday,
            "start_weekday": start_weekday,
            "end_weekday": end_weekday,
            "start_hour": start_hour,
            "start_minute": start_minute,
            "end_hour": end_hour,
            "end_minute": end_minute,
        }
        overrides_provided = {k: v for k, v in overrides.items() if v is not None}
        return dataclasses.replace(self, **overrides_provided)

    @staticmethod
    def daily(
        start: typing.Optional[datetime.datetime] = None,
        start_month: int = 1,
        end_month: int = 12,
        start_monthday: int = 1,
        end_monthday: int = 31,
        start_weekday: weekday.Weekday = weekday.Weekday.Mon,
        end_weekday: weekday.Weekday = weekday.Weekday.Sun,
        start_hour: int = 0,
        start_minute: int = 0,
        end_hour: int = 23,
        end_minute: int = 59,
    ) -> Schedule:
        return Schedule(
            interval=interval.Interval.daily(),
            start=start,
            start_month=start_month,
            end_month=end_month,
            start_monthday=start_monthday,
            end_monthday=end_monthday,
            start_weekday=start_weekday,
            end_weekday=end_weekday,
            start_hour=start_hour,
            start_minute=start_minute,
            end_hour=end_hour,
            end_minute=end_minute,
        )

    @staticmethod
    def every_x_seconds(
        seconds: int,
        start: typing.Optional[datetime.datetime] = None,
        start_month: int = 1,
        end_month: int = 12,
        start_monthday: int = 1,
        end_monthday: int = 31,
        start_weekday: weekday.Weekday = weekday.Weekday.Mon,
        end_weekday: weekday.Weekday = weekday.Weekday.Sun,
        start_hour: int = 0,
        start_minute: int = 0,
        end_hour: int = 23,
        end_minute: int = 59,
    ) -> Schedule:
        return Schedule(
            interval=interval.Interval.every_x_seconds(seconds=seconds),
            start=start,
            start_month=start_month,
            end_month=end_month,
            start_monthday=start_monthday,
            end_monthday=end_monthday,
            start_weekday=start_weekday,
            end_weekday=end_weekday,
            start_hour=start_hour,
            start_minute=start_minute,
            end_hour=end_hour,
            end_minute=end_minute,
        )

    def is_due(self, *, last_completed: typing.Optional[datetime.datetime]) -> bool:
        ts = datetime.datetime.now()
        if ts.month < self.start_month or ts.month > self.end_month:
            return False
        elif ts.day < self.start_monthday or ts.day > self.end_monthday:
            return False
        elif ts.isoweekday() < self.start_weekday or ts.isoweekday() > self.end_weekday:
            return False
        elif ts.hour < self.start_hour or ts.hour > self.end_hour:
            return False
        elif ts.minute < self.start_minute or ts.minute > self.end_minute:
            return False
        else:
            if last_completed is None:
                if self.start:
                    return ts >= self.start
                else:
                    return True
            else:
                next_due = self.interval.next(last=last_completed, now=ts)
                if ts >= next_due:
                    return True
                else:
                    return False


if __name__ == "__main__":
    print(f"Fri: {datetime.date(2021, 5, 7).isoweekday()=}")
    print(f"Sat: {datetime.date(2021, 5, 8).isoweekday()=}")
    print(f"Sun: {datetime.date(2021, 5, 9).isoweekday()=}")
    print(f"Mon: {datetime.date(2021, 5, 10).isoweekday()=}")
    print(f"Tue: {datetime.date(2021, 5, 11).isoweekday()=}")
    print(f"Wed: {datetime.date(2021, 5, 12).isoweekday()=}")

    x = Schedule.every_x_seconds(seconds=30)
    print(x)
    print(f"{x.is_due(last_completed=None)}")
