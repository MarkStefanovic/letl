import datetime
import typing

import sqlalchemy as sa

from letl import domain
from letl.adapter import db
from letl.domain import log_level

__all__ = ("DbLogRepo",)


class DbLogRepo(domain.LogRepo):
    def __init__(self, *, engine: sa.engine.Engine):
        self._engine = engine

    def add(
        self,
        *,
        name: str,
        level: log_level.LogLevel,
        message: str,
        ts: typing.Optional[datetime.datetime] = None,
    ) -> None:
        with self._engine.begin() as con:
            stmt = db.log.insert().values(
                name=name,
                level=str(level),
                message=message,
                ts=datetime.datetime.now(),
            )
            con.execute(stmt)

    def delete_before(self, /, ts: datetime.datetime) -> None:
        with self._engine.begin() as con:
            con.execute(db.log.delete().where(db.log.c.ts < ts))
