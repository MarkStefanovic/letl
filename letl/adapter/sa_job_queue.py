import datetime
import logging
import typing

import sqlalchemy as sa

from letl.adapter import db

from letl import domain, Job

__all__ = ("SAJobQueue",)

logger = domain.root_logger.getChild("sa_job_queue")


class SAJobQueue(domain.JobQueue):
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

    def clear(self) -> None:
        result = self._con.execute(db.job_queue.delete())
        logger.debug(f"Deleted {result.rowcount} rows from job_queue.")

    def pop(self, n: int) -> typing.Set[Job]:
        result = self._con.execute(
            db.job_queue.select().order_by(db.job_queue.c.added).limit(n)
        )
        job_names = [row.job_name for row in result]
        deleted = self._con.execute(
            db.job_queue.delete().where(db.job_queue.c.job_name.in_(job_names))
        )
        logger.debug(f"Deleted {deleted.rowcount} rows from job_queue.")
        return set(job_names)
