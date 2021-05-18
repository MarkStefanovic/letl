import datetime
import typing

import sqlalchemy as sa

from letl import domain
from letl.adapter import db

__all__ = ("SAStatusRepo",)


class SAStatusRepo(domain.StatusRepo):
    def __init__(self, *, engine: sa.engine.Engine):
        self._engine = engine

    def done(self, *, job_status_id: int) -> None:
        with self._engine.begin() as con:
            con.execute(
                db.status.update()
                .where(db.status.c.id == job_status_id)
                .values(
                    status=domain.Status.Success.value,
                    ended=datetime.datetime.now(),
                )
            )
        append_to_history(engine=self._engine, job_status_id=job_status_id)

    def error(self, *, job_status_id: int, error: str) -> None:
        with self._engine.begin() as con:
            con.execute(
                db.status.update()
                .where(db.status.c.id == job_status_id)
                .values(
                    status=domain.Status.Error.value,
                    error_message=error,
                    ended=datetime.datetime.now(),
                )
            )
        append_to_history(engine=self._engine, job_status_id=job_status_id)

    def skipped(self, *, job_status_id: int, reason: str) -> None:
        with self._engine.begin() as con:
            con.execute(
                db.status.update()
                .where(db.status.c.id == job_status_id)
                .values(
                    status=domain.Status.Skipped.value,
                    skipped_reason=reason,
                    ended=datetime.datetime.now(),
                )
            )
        append_to_history(engine=self._engine, job_status_id=job_status_id)

    def start(self, *, job_name: str) -> int:
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
            return result.inserted_primary_key[0]

    def delete_before(self, /, ts: datetime.datetime) -> None:
        with self._engine.begin() as con:
            stmt = db.job_history.delete().where(db.job_history.c.started <= ts)
            con.execute(stmt)
            stmt = db.status.delete().where(db.status.c.started <= ts)
            con.execute(stmt)

    def latest_status(self, *, job_name: str) -> typing.Optional[domain.JobStatus]:
        with self._engine.begin() as con:
            # fmt: off
            stmt = (
                db.status
                .select()
                .where(db.status.c.name == job_name)
                .order_by(sa.desc(db.status.c.started))
                .limit(1)
            )
            # fmt: on
            result = con.execute(stmt).first()
            return domain.JobStatus(
                job_name=result.name,
                status=result.status,
                started=result.started,
                ended=result.ended,
                skipped_reason=result.skipped_reason,
                error_message=result.error_message,
            )

    def latest_completed_time(
        self, *, job_name: str
    ) -> typing.Optional[datetime.datetime]:
        with self._engine.begin() as con:
            stmt = (
                sa.select(sa.func.max(db.status.c.ended).label("ts"))
                .where(db.status.c.status == "success")
                .where(db.status.c.job_name == job_name)
            )
            return con.execute(stmt).scalar()


def append_to_history(*, engine: sa.engine.Engine, job_status_id: int) -> None:
    with engine.begin() as con:
        result = con.execute(
            db.status.select().where(db.status.c.id == job_status_id)
        ).first()
        if result:
            con.execute(
                db.job_history.insert().values(
                    status_id=job_status_id,
                    job_name=result.name,
                    status=result.status,
                    started=result.started,
                    ended=result.ended,
                    skipped_reason=result.skipped_reason,
                    error_message=result.error_message,
                )
            )
