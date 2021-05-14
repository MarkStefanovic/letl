import datetime
import typing

import sqlalchemy as sa

from letl import domain
from letl.adapter import db

__all__ = ("SAStatusRepo",)

mod_logger = domain.root_logger.getChild("sa_status_repo")


class SAStatusRepo(domain.StatusRepo):
    def __init__(self, *, con: sa.engine.Connection):
        self._con = con

        self._logger = mod_logger.getChild(self.__class__.__name__)

    def done(self, *, job_status_id: int) -> None:
        self._logger.debug(f"done({job_status_id=})")
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
        self._logger.debug(f"error({job_status_id=}, {error=})")
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
        self._logger.debug(f"error({job_status_id=}, {reason=})")
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

    def start(self, *, job_name: str) -> int:
        self._logger.debug(f"start({job_name=})")
        stmt = db.status.insert().values(
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
        self._logger.debug(f"delete_before({ts})")
        stmt = db.status.delete().where(db.status.c.started <= ts)
        self._con.execute(stmt)

    def latest_status(self, *, job_name: str) -> typing.Optional[domain.JobStatus]:
        self._logger.debug(f"latest_status({job_name=})")
        # # fmt: off
        # stmt = (
        #     sa.select(sa.func.max(db.status.c.ended).label("ts"))
        #     .where(db.status.c.status == "success")
        #     .where(db.status.c.job_name == job_name)
        # )
        # # fmt: on
        # return self._con.execute(stmt).scalar()

        # fmt: off
        stmt = (
            db.status
            .select()
            .where(db.status.c.name == job_name)
            .order_by(sa.desc(db.status.c.started))
            .limit(1)
        )
        # fmt: on
        result = self._con.execute(stmt).first()
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
        self._logger.debug(f"latest_completed_time({job_name=})")
        stmt = (
            sa.select(sa.func.max(db.status.c.ended).label("ts"))
            .where(db.status.c.status == "success")
            .where(db.status.c.name == job_name)
        )
        return self._con.execute(stmt).scalar()
