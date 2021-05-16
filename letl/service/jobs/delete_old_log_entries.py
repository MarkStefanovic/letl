from __future__ import annotations
import datetime
import typing

import sqlalchemy as sa

from letl import adapter, domain

__all__ = ("delete_old_log_entries",)


class Config(typing.TypedDict):
    etl_db_uri: str
    log_to_console: bool
    days_to_keep: int


def delete_old_log_entries(
    etl_db_uri: str, days_to_keep: int = 3, log_to_console: bool = False
) -> domain.Job:
    return domain.Job(
        job_name="delete_old_log_entries",
        timeout_seconds=900,
        retries=1,
        dependencies=set(),
        run=run,
        config=Config(
            etl_db_uri=etl_db_uri,
            log_to_console=log_to_console,
            days_to_keep=days_to_keep,
        ),
        schedule=[domain.EveryXSeconds(seconds=3600 * 24)],
    )


def run(config: Config, _: domain.Logger) -> None:
    engine = sa.create_engine(uri=config["etl_db_uri"], echo=config["log_to_console"])
    cutoff = datetime.datetime.now() - datetime.timedelta(days=config["days_to_keep"])
    with engine.connect() as con:
        log_repo = adapter.SALogRepo(con=con)
        log_repo.delete_before(cutoff)
        status_repo = adapter.SAStatusRepo(con=con)
        status_repo.delete_before(cutoff)
