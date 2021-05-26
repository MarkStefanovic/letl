from __future__ import annotations

import datetime
import typing

import sqlalchemy as sa

from letl import adapter, domain

__all__ = ("delete_old_log_entries",)


def delete_old_log_entries(etl_db_uri: str, days_to_keep: int = 3) -> domain.Job:
    return domain.Job(
        job_name="delete_old_log_entries",
        timeout_seconds=900,
        retries=1,
        dependencies=set(),
        run=run,
        config={"etl_db_uri": etl_db_uri, "days_to_keep": days_to_keep},
        schedule={domain.EveryXSeconds(seconds=3600 * 24)},
    )


def run(config: typing.Dict[str, typing.Any], _: domain.Logger) -> domain.JobResult:
    cutoff = datetime.datetime.now() - datetime.timedelta(days=config["days_to_keep"])
    engine = sa.create_engine(config["etl_db_uri"])
    log_repo = adapter.SALogRepo(engine=engine)
    log_repo.delete_before(cutoff)
    status_repo = adapter.SAStatusRepo(engine=engine)
    status_repo.delete_before(cutoff)
    return domain.JobResult.success()
