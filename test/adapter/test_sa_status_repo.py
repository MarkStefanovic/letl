import datetime

import sqlalchemy as sa

import letl


def check_row_count(*, con: sa.engine.Connection, expected_rows: int) -> None:
    sql = "SELECT COUNT(*) AS ct FROM letl.status"
    actual_rows = con.execute(sql).scalar()
    assert (
        actual_rows == expected_rows
    ), f"Expected {expected_rows} rows, but got {actual_rows}."


def test_latest_status_happy_path(in_memory_db: sa.engine.Engine):
    with in_memory_db.connect() as con:
        sql = """
            INSERT INTO letl.status 
                (id, job_name, status, started, ended, error_message, skipped_reason)
            VALUES 
                (1, 'test_job_1', 'success', '2010-01-01 03:00:00', '2010-01-01 04:00:00', NULL, NULL)
            ,   (2, 'test_job_2', 'skipped', '2010-01-01 03:01:00', '2010-01-01 03:30:00', NULL, 'skipped_reason')
            ,   (3, 'test_job_3', 'error', '2010-01-02 03:00:00', '2010-01-02 03:02:00', 'Whoops!', NULL)
            ,   (4, 'test_job_1', 'success', '2010-01-03 03:00:00', '2010-01-03 03:40:00', NULL, NULL)
            ,   (5, 'test_job_4', 'running', '2010-01-04 03:01:00', NULL, NULL, NULL)
        """
        con.execute(sql)
        check_row_count(con=con, expected_rows=5)

        repo = letl.SAStatusRepo(con=con)
        result = repo.status(job_name="test_job_1")
        assert result == letl.JobStatus(
            job_name="test_job_1",
            status=letl.Status.Success,
            skipped_reason=None,
            started=datetime.datetime(2010, 1, 3, 3, 0),
            ended=datetime.datetime(2010, 1, 3, 3, 40),
            error_message=None,
        )


def test_start_happy_path(in_memory_db: sa.engine.Engine):
    with in_memory_db.connect() as con:
        check_row_count(con=con, expected_rows=0)

        repo = letl.SAStatusRepo(con=con)
        result = repo.start(batch_id="batch_1", job_name="test_job_1")
        assert result > 0

        sql = "SELECT * FROM letl.status WHERE batch_id = 'batch_1' AND job_name = 'test_job_1'"
        result = con.execute(sql).first()
        assert result.name == "test_job_1"
        assert result.status == "running"
        assert result.started is not None
        assert result.ended is None
        assert result.error_message is None
        assert result.skipped_reason is None
