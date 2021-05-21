import datetime
import typing

import sqlalchemy as sa

from letl import domain
from letl.adapter import db

__all__ = ("SAStatusRepo",)


class SAStatusRepo(domain.StatusRepo):
    def __init__(self, *, engine: sa.engine.Engine):
        self._engine = engine

    def all(self) -> typing.Set[domain.JobStatus]:
        with self._engine.begin() as con:
            return {job_status for job_status in con.execute(db.status.select())}

    def done(self, *, job_name: str) -> None:
        with self._engine.begin() as con:
            con.execute(
                db.status.update()
                .where(db.status.c.job_name == job_name)
                .values(
                    status=domain.Status.Success.value,
                    ended=datetime.datetime.now(),
                )
            )
        append_to_history(engine=self._engine, job_name=job_name)

    def error(self, *, job_name: str, error: str) -> None:
        with self._engine.begin() as con:
            con.execute(
                db.status.update()
                .where(db.status.c.job_name == job_name)
                .values(
                    status=domain.Status.Error.value,
                    error_message=error,
                    ended=datetime.datetime.now(),
                )
            )
        append_to_history(engine=self._engine, job_name=job_name)

    def skipped(self, *, job_name: str, reason: str) -> None:
        with self._engine.begin() as con:
            con.execute(
                db.status.update()
                .where(db.status.c.job_name == job_name)
                .values(
                    status=domain.Status.Skipped.value,
                    skipped_reason=reason,
                    ended=datetime.datetime.now(),
                )
            )
        append_to_history(engine=self._engine, job_name=job_name)

    def start(self, *, job_name: str) -> None:
        with self._engine.begin() as con:
            con.execute(db.status.delete().where(db.status.c.job_name == job_name))
            stmt = db.status.insert().values(
                job_name=job_name,
                status=domain.Status.Running.value,
                started=datetime.datetime.now(),
                ended=None,
                error_message=None,
                skipped_reason=None,
            )
            result = con.execute(stmt)

    def delete(self, *, job_name: str) -> None:
        with self._engine.begin() as con:
            con.execute(db.status.delete().where(db.status.c.job_name == job_name))

    def delete_before(self, /, ts: datetime.datetime) -> None:
        with self._engine.begin() as con:
            stmt = db.job_history.delete().where(db.job_history.c.started <= ts)
            con.execute(stmt)
            stmt = db.status.delete().where(db.status.c.started <= ts)
            con.execute(stmt)

    def status(self, *, job_name: str) -> typing.Optional[domain.JobStatus]:
        with self._engine.begin() as con:
            # fmt: off
            stmt = (
                db.status
                .select()
                .where(db.status.c.job_name == job_name)
            )
            # fmt: on
            result = con.execute(stmt).first()
            if result:
                return domain.JobStatus(
                    job_name=result.job_name,
                    status=result.status,
                    started=result.started,
                    ended=result.ended,
                    skipped_reason=result.skipped_reason,
                    error_message=result.error_message,
                )
            else:
                return None


def append_to_history(*, engine: sa.engine.Engine, job_name: str) -> None:
    with engine.begin() as con:
        result = con.execute(
            db.status.select().where(db.status.c.job_name == job_name)
        ).first()
        if result:
            con.execute(
                db.job_history.insert().values(
                    job_name=job_name,
                    status=result.status,
                    started=result.started,
                    ended=result.ended,
                    skipped_reason=result.skipped_reason,
                    error_message=result.error_message,
                )
            )
