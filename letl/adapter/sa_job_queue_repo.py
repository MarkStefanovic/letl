import datetime
import typing

import sqlalchemy as sa

from letl import domain, Job
from letl.adapter import db

__all__ = ("SAJobQueueRepo",)


class SAJobQueueRepo(domain.JobQueueRepo):
    def __init__(self, *, con: sa.engine.Connection):
        self._con = con

    def add(self, *, job_name: str) -> int:
        """
        First check if the job is already in the queue.  If it is, then return.  We only want a
        single instance of a job in the queue at once.

        :param job_name:
        :return:
        """
        result = self._con.execute(
            db.job_queue.insert().values(
                job_name=job_name,
                added=datetime.datetime.now(),
            )
        )
        return result.inserted_primary_key

    def all(self) -> typing.List[str]:
        jobs = self._con.execute(db.job_queue.select().order_by(db.job_queue.c.added))
        return [job.name for job in jobs]

    def clear(self) -> None:
        self._con.execute(db.job_queue.delete())

    def delete(self, *, job_name: str) -> None:
        self._con.execute(db.job_queue.delete().where(db.job_queue.c.name == job_name))

    def pop(self, n: int) -> typing.List[Job]:
        jobs = self._con.execute(
            db.job_queue.select().order_by(db.job_queue.c.added).limit(n)
        )
        job_names = [job.name for job in jobs]
        self._con.execute(
            db.job_queue.delete().where(db.job_queue.c.name.in_(job_names))
        )
        return job_names
