from __future__ import annotations
import datetime
import typing

from letl import adapter, domain

__all__ = ("delete_old_log_entries",)


def delete_old_log_entries(days_to_keep: int = 3) -> domain.Job:
    return domain.Job(
        job_name="delete_old_log_entries",
        timeout_seconds=900,
        retries=1,
        dependencies=set(),
        run=run,
        config={"days_to_keep": days_to_keep},
        schedule=[domain.EveryXSeconds(seconds=3600 * 24)],
    )


def run(config: typing.Dict[str, typing.Any], _: domain.Logger) -> None:
    cutoff = datetime.datetime.now() - datetime.timedelta(days=config["days_to_keep"])
    log_repo = adapter.SALogRepo(engine=config["etl_engine"])
    log_repo.delete_before(cutoff)
    status_repo = adapter.SAStatusRepo(engine=config["etl_engine"])
    status_repo.delete_before(cutoff)
