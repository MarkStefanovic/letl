import datetime

import sqlalchemy as sa

from letl import adapter, domain

__all__ = ("delete_old_log_entries",)


def delete_old_log_entries(etl_db_uri: str, days_to_keep: int = 3) -> domain.Job:
    return domain.Job(
        job_name="delete_old_log_entries",
        timeout_seconds=900,
        retries=1,
        dependencies=frozenset(),
        run=run,
        config=domain.config(
            etl_db_uri=etl_db_uri,
            days_to_keep=days_to_keep,
        ),
        schedule=frozenset({domain.Schedule.every_x_seconds(seconds=3600 * 24)}),
    )


def run(
    config: domain.Config, _: domain.Logger, __: domain.ResourceManager
) -> domain.JobResult:
    cutoff = datetime.datetime.now() - datetime.timedelta(
        days=config.get("days_to_keep", int)
    )
    engine = sa.create_engine(config.get("etl_db_uri", str))
    log_repo = adapter.DbLogRepo(engine=engine)
    log_repo.delete_before(cutoff)
    status_repo = adapter.DbStatusRepo(engine=engine)
    status_repo.delete_before(cutoff)
    return domain.JobResult.success()
