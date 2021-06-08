import multiprocessing as mp
import queue
import threading
import time

import sqlalchemy as sa

from letl import adapter, domain

__all__ = ("JobRunner",)

logger = domain.root_logger.getChild("job_runner")


class JobRunner(threading.Thread):
    def __init__(
        self,
        *,
        engine: sa.engine.Engine,
        job_queue: "queue.Queue[domain.Job]",
        logger: domain.Logger,
        seconds_between_jobs: int,
    ):
        super().__init__()

        self._engine = engine
        self._job_queue = job_queue
        self._logger = logger
        self._seconds_between_jobs = seconds_between_jobs

    def run(self) -> None:
        while True:
            try:
                job = self._job_queue.get()
                run_job(
                    job=job,
                    engine=self._engine,
                    logger=self._logger,
                )
                time.sleep(10)
            except Exception as e:
                try:
                    self._logger.exception(e)
                except Exception as e2:
                    print(e2)
                    logger.exception(e2)


def run_job(
    *,
    job: domain.Job,
    engine: sa.engine.Engine,
    logger: domain.Logger,
) -> None:
    status_repo = adapter.SAStatusRepo(engine=engine)
    logger.info(f"Starting [{job.job_name}]...")
    status_repo.start(job_name=job.job_name)
    result = run_job_in_process(
        logger=logger.new(name=f"{logger.name}.{job.job_name}"), job=job
    )
    logger.debug(f"Saving results of [{job.job_name}] to database")
    if result.is_error:
        err_msg = result.error_message or "no error message was provided."
        status_repo.error(job_name=job.job_name, error=err_msg)
        logger.error(err_msg)
    else:
        status_repo.done(job_name=job.job_name)
        logger.info(f"[{job.job_name}] finished.")


def run_job_in_process(
    *,
    logger: domain.Logger,
    job: domain.Job,
) -> domain.JobResult:
    result_queue: "mp.Queue[domain.JobResult]" = mp.Queue()
    p = mp.Process(
        target=run_job_with_retry,
        args=(result_queue, job, logger, 0),
    )
    try:
        p.start()
        result = result_queue.get(block=True, timeout=job.timeout_seconds)
        return result
    except queue.Empty:
        return domain.JobResult.error(
            domain.error.JobTimedOut(
                f"The job, [{job.job_name}], timed out after {job.timeout_seconds} seconds."
            )
        )
    except Exception as e:
        return domain.JobResult.error(e)
    finally:
        p.join()
        result_queue.close()


# noinspection PyBroadException
def run_job_with_retry(
    result_queue: "mp.Queue[domain.JobResult]",
    job: domain.Job,
    logger: domain.Logger,
    retries_so_far: int = 0,
) -> None:
    try:
        result = job.run(job.config, logger)  # type: ignore
        if result is None:
            result = domain.JobResult.success()
        result_queue.put(result)
    except Exception as e:
        if job.retries > retries_so_far:
            run_job_with_retry(
                result_queue=result_queue,
                job=job,
                logger=logger,
                retries_so_far=retries_so_far + 1,
            )
        else:
            result_queue.put(domain.JobResult.error(e))
