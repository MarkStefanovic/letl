import typing

import sqlalchemy as sa

__all__ = (
    "create_tables",
    "log",
    "job_queue",
    "status",
)

SCHEMA = "etl"

metadata = sa.MetaData(schema=SCHEMA)


log = sa.Table(
    "log",
    metadata,
    sa.Column(
        "id",
        sa.Integer,
        sa.Sequence(f"{SCHEMA}.log_id_seq"),
        primary_key=True,
    ),
    sa.Column("name", sa.String, nullable=False),
    sa.Column("level", sa.String, nullable=False),
    sa.Column("ts", sa.DateTime, nullable=False),
    sa.Column("message", sa.String, nullable=False),
)

job_queue = sa.Table(
    "job_queue",
    metadata,
    sa.Column("job_name", sa.String, primary_key=True),
    sa.Column("added", sa.DateTime, nullable=False),
)

schedule = sa.Table(
    "schedule",
    metadata,
    sa.Column(
        "id",
        sa.Integer,
        sa.Sequence(f"{SCHEMA}.schedule_id_seq"),
        primary_key=True,
    ),
    sa.Column("seconds", sa.Integer, nullable=True),
    sa.Column("start_monthday", sa.Integer, nullable=False),
    sa.Column("start_month", sa.Integer, nullable=False),
    sa.Column("end_month", sa.Integer, nullable=False),
    sa.Column("end_monthday", sa.Integer, nullable=False),
    sa.Column("start_weekday", sa.Integer, nullable=False),
    sa.Column("end_weekday", sa.Integer, nullable=False),
    sa.Column("start_hour", sa.Integer, nullable=False),
    sa.Column("start_minute", sa.Integer, nullable=False),
    sa.Column("end_hour", sa.Integer, nullable=False),
    sa.Column("end_minute", sa.Integer, nullable=False),
    sa.Column("added", sa.DateTime, nullable=False),
    sa.Column("updated", sa.DateTime, nullable=True),
)

status = sa.Table(
    "status",
    metadata,
    sa.Column(
        "id",
        sa.Integer,
        sa.Sequence(f"{SCHEMA}.status_id_seq"),
        primary_key=True,
    ),
    sa.Column("job_name", sa.String, nullable=True),
    sa.Column("status", sa.String, nullable=False),
    sa.Column("started", sa.DateTime, nullable=False),
    sa.Column("ended", sa.DateTime, nullable=True),
    sa.Column("error_message", sa.String, nullable=True),
    sa.Column("skipped_reason", sa.String, nullable=True),
)


def create_tables(*, engine: typing.Optional[sa.engine.Engine]) -> None:
    metadata.create_all(engine)
