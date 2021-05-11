import typing

import sqlalchemy as sa

__all__ = (
    "create_tables",
    "log",
    "plan",
    "status",
)


metadata = sa.MetaData(schema="etl")


log = sa.Table(
    "log",
    metadata,
    sa.Column("id", sa.String(32), primary_key=True),
    sa.Column("batch_id", sa.String(32), nullable=False),
    sa.Column("status", sa.String, nullable=False),
    sa.Column("level", sa.String, nullable=False),
    sa.Column("name", sa.String, nullable=False),
    sa.Column("ts", sa.DateTime, nullable=False),
    sa.Column("message", sa.String, nullable=False),
)

plan = sa.Table(
    "plan",
    metadata,
    sa.Column("id", sa.Integer, sa.Sequence("etl.status_id_seq"), primary_key=True),
    sa.Column("batch_id", sa.String(32), nullable=False),
    sa.Column("job_name", sa.String, nullable=True),
    sa.Column("status", sa.String, nullable=False),
    sa.Column("started", sa.DateTime, nullable=False),
    sa.Column("ended", sa.DateTime, nullable=True),
    sa.Column("error_message", sa.String, nullable=True),
    sa.Column("skipped_reason", sa.String, nullable=True),
)

status = sa.Table(
    "status",
    metadata,
    sa.Column("id", sa.Integer, sa.Sequence("etl.status_id_seq"), primary_key=True),
    sa.Column("batch_id", sa.String(32), nullable=False),
    sa.Column("job_name", sa.String, nullable=True),
    sa.Column("status", sa.String, nullable=False),
    sa.Column("started", sa.DateTime, nullable=False),
    sa.Column("ended", sa.DateTime, nullable=True),
    sa.Column("error_message", sa.String, nullable=True),
    sa.Column("skipped_reason", sa.String, nullable=True),
)


def create_tables(*, engine: typing.Optional[sa.engine.Engine]) -> None:
    metadata.create_all(engine)
