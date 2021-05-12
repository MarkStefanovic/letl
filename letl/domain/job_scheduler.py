from __future__ import annotations

import abc
import typing

from letl import Job
from letl.domain.job_registry import JobRegistry

__all__ = ("JobScheduler",)


class JobScheduler(abc.ABC):
    @abc.abstractmethod
    def start(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def get_ready_jobs(self) -> typing.Set[str]:
        raise NotImplementedError


class InMemoryJobScheduler(JobScheduler):
    def __init__(self, *, registry: JobRegistry, schedule_repo: ScheduleRepo):
        self._registry = registry
        self._schedule_repo = schedule_repo

        self._ready_jobs: typing.Set[str] = set()

    def start(self) -> None:
        pass

    def get_ready_jobs(self) -> typing.Set[str]:
        ready_jobs: typing.List[Job] = []
        for job in self._registry.all():
            job_is_ready(job)
