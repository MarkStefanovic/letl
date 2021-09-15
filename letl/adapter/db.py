import sqlalchemy as sa

__all__ = (
    "create_tables",
    "log",
    "status",
)

SCHEMA = "letl"

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

job_history = sa.Table(
    "job_history",
    metadata,
    sa.Column(
        "id",
        sa.Integer,
        sa.Sequence(f"{SCHEMA}.job_history_id_seq"),
        primary_key=True,
    ),
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
    sa.Column("job_name", sa.String, primary_key=True),
    sa.Column("status", sa.String, nullable=False),
    sa.Column("started", sa.DateTime, nullable=False),
    sa.Column("ended", sa.DateTime, nullable=True),
    sa.Column("error_message", sa.String, nullable=True),
    sa.Column("skipped_reason", sa.String, nullable=True),
)


def create_tables(*, engine: sa.engine.Engine, recreate: bool = False) -> None:
    with engine.begin() as con:
        if recreate:
            metadata.drop_all(con)
        metadata.create_all(con)
