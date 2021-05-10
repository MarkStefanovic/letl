import datetime
import typing

import sqlalchemy as sa

from letl import domain
from letl.adapter import db

__all__ = ("SAStatusRepo",)


class SAStatusRepo(domain.JobStatusRepo):
    def __init__(self, *, con: sa.engine.Connection):
        self._con = con

    def done(self, *, job_status_id: int) -> None:
        stmt = (
            db.status.update()
            .where(db.status.c.id == job_status_id)
            .values(
                status=domain.Status.Success.value,
                ended=datetime.datetime.now(),
            )
        )
        self._con.execute(stmt)

    def error(self, *, job_status_id: int, error: str) -> None:
        stmt = (
            db.status.update()
            .where(db.status.c.id == job_status_id)
            .values(
                status=domain.Status.Error.value,
                error_message=error,
                ended=datetime.datetime.now(),
            )
        )
        self._con.execute(stmt)

    def skipped(self, *, job_status_id: int, reason: str) -> None:
        stmt = (
            db.status.update()
            .where(db.status.c.id == job_status_id)
            .values(
                status=domain.Status.Skipped.value,
                skipped_reason=reason,
                ended=datetime.datetime.now(),
            )
        )
        self._con.execute(stmt)

    def start(self, *, batch_id: str, job_name: str) -> int:
        stmt = db.status.insert().values(
            batch_id=batch_id,
            job_name=job_name,
            status=domain.Status.Running.value,
            started=datetime.datetime.now(),
            ended=None,
            error_message=None,
            skipped_reason=None,
        )
        result: sa.engine.CursorResult = self._con.execute(stmt)
        return result.inserted_primary_key[0]

    def delete_before(self, /, ts: datetime.datetime) -> None:
        stmt = db.status.delete().where(db.status.c.started <= ts)
        self._con.execute(stmt)

    def last_run(self, *, job_name: str) -> typing.Optional[datetime.datetime]:
        # fmt: off
        stmt = (
            sa.select(sa.func.max(db.status.c.ended).label("ts"))
            .where(db.status.c.status == "success")
            .where(db.status.c.job_name == job_name)
        )
        # fmt: on
        return self._con.execute(stmt).scalar()
