import pytest

import letl

import sqlalchemy as sa


@pytest.fixture(scope="function")
def in_memory_db() -> sa.engine.Engine:
    engine = sa.create_engine("sqlite://", echo=True)
    with engine.begin() as con:
        con.execute(sa.text("ATTACH ':memory:' as etl"))
        letl.db.create_tables(engine=engine)
    return engine
