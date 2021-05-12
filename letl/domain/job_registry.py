import abc
import typing

from letl import Job
from letl.domain import error

__all__ = (
    "InMemoryJobRegistry",
    "JobRegistry",
)


class JobRegistry(abc.ABC):
    @abc.abstractmethod
    def all(self) -> typing.Set[Job]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_implementation(self, *, job_name: str) -> Job:
        raise NotImplementedError


class InMemoryJobRegistry(JobRegistry):
    def __init__(self, *, jobs: typing.List[Job]):
        self._jobs: typing.Dict[str, Job] = {job.job_name: job for job in jobs}

    def all(self) -> typing.Set[Job]:
        return set(self._jobs.values())

    def get_implementation(self, *, job_name: str) -> Job:
        if job_name in self._jobs.keys():
            return self._jobs[job_name]
        else:
            raise error.MissingJobImplementation(job_name=job_name)

    def __repr__(self) -> str:
        jobs = ", ".join(f"<Job: {job_name}>" for job_name in self._jobs.keys())
        return f"InMemoryJobRegistry(jobs={jobs})"
