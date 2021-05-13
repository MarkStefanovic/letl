import collections
import typing

from letl import domain
from letl.adapter.sa_status_repo import SAStatusRepo

import sqlalchemy as sa

__all__ = ("SAScheduleRepo",)


class SAScheduleRepo(domain.ScheduleRepo):
    def __init__(self, *, con: sa.engine.Connection):
        self._con = con
        self._job_schedules: typing.Dict[
            str, typing.List[domain.Schedule]
        ] = collections.defaultdict(list)

    def add(self, *, job_name: str, schedule: domain.Schedule) -> None:
        self._job_schedules[job_name].append(schedule)

    def clear(self) -> None:
        self._job_schedules.clear()

    def delete(self, *, job_name: str) -> None:
        self._job_schedules[job_name] = []

    def ready_jobs(self) -> typing.Set[str]:
        ready_job_names: typing.List[str] = []
        status_repo = SAStatusRepo(con=self._con)
        for job_name, schedules in self._job_schedules.items():
            last_completed = status_repo.latest_completed_time(job_name=job_name)
            if any(
                schedule.is_due(last_completed=last_completed) for schedule in schedules
            ):
                ready_job_names.append(job_name)
        return set(ready_job_names)
