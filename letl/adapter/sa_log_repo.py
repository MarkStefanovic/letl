import datetime

import sqlalchemy as sa

from letl import domain
from letl.adapter import db
from letl.domain import log_level

__all__ = ("SALogRepo",)


class SALogRepo(domain.LogRepo):
    def __init__(self, *, con: sa.engine.Connection):
        self._con = con

    def add(self, *, name: str, level: log_level.LogLevel, message: str) -> None:
        stmt = db.log.insert().values(
            name=name,
            level=str(level),
            message=message,
            ts=datetime.datetime.now(),
        )
        self._con.execute(stmt)

    def delete_before(self, /, ts: datetime.datetime) -> None:
        self._con.execute(db.log.delete().where(db.log.c.ts < ts))
