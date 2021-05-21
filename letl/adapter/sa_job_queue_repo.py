import datetime
import typing

import sqlalchemy as sa

from letl import domain
from letl.adapter import db

__all__ = ("SAJobQueueRepo",)


class SAJobQueueRepo(domain.JobQueueRepo):
    def __init__(self, *, engine: sa.engine.Engine):
        self._engine = engine

    def add(self, *, job_name: str) -> None:
        with self._engine.begin() as con:
            not_found = (
                con.execute(
                    db.job_queue.select().where(db.job_queue.c.job_name == job_name)
                ).first()
                is None
            )
            if not_found:
                con.execute(
                    db.job_queue.insert().values(
                        job_name=job_name,
                        added=datetime.datetime.now(),
                    )
                )
                # return result.inserted_primary_key

    def all(self) -> typing.Set[str]:
        with self._engine.begin() as con:
            jobs = con.execute(db.job_queue.select().order_by(db.job_queue.c.added))
            return {job.job_name for job in jobs}

    def clear(self) -> None:
        with self._engine.begin() as con:
            con.execute(db.job_queue.delete())

    def delete(self, *, job_name: str) -> None:
        with self._engine.begin() as con:
            con.execute(
                db.job_queue.delete().where(db.job_queue.c.job_name == job_name)
            )

    def pop(self, n: int) -> typing.List[str]:
        with self._engine.begin() as con:
            jobs = con.execute(
                db.job_queue.select().order_by(db.job_queue.c.added).limit(n)
            )
            job_names = [job.job_name for job in jobs]
            if job_names:
                con.execute(
                    db.job_queue.delete().where(db.job_queue.c.job_name.in_(job_names))
                )
                return job_names
            else:
                return []
