import pytest

import letl

import sqlalchemy as sa


@pytest.fixture(scope="function")
def in_memory_db() -> sa.engine.Engine:
    engine = sa.create_engine("sqlite://", echo=True)
    with engine.connect() as con:
        con.execute("ATTACH ':memory:' as etl")
        letl.db.create_tables(engine=engine)
    return engine
