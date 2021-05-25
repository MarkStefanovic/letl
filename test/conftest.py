import pytest
import sqlalchemy as sa

import letl


@pytest.fixture(scope="function")
def in_memory_db() -> sa.engine.Engine:
    engine = sa.create_engine("sqlite://", echo=True)
    with engine.begin() as con:
        con.execute(sa.text("ATTACH ':memory:' as letl"))
        letl.db.create_tables(engine=engine)
    return engine
