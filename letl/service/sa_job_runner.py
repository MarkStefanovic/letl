import datetime
import multiprocessing as mp
import typing
from queue import Empty

import pykka
import sqlalchemy as sa

from letl import adapter, domain

__all__ = ("run_job",)


def run_job(
    *,
    engine: sa.engine.Engine,
    job_queue: pykka.ActorProxy,
    logger: domain.Logger,
) -> None:
    status_repo = adapter.SAStatusRepo(engine=engine)
    job = job_queue.pop().get()
    if job:
        logger.info(f"Starting [{job.job_name}]...")
        status_repo.start(job_name=job.job_name)
        result = run_job_in_process(logger=logger.new(name=job.job_name), job=job)
        logger.debug(f"Saving results of [{job.job_name}] to database")
        if result.is_error:
            err_msg = result.error_message or " o error message was provided."
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
    queue_logger = QueueLogger(job_name=job.job_name)
    try:
        result_queue = mp.Queue()
        p = mp.Process(
            target=run_job_with_retry,
            args=(result_queue, job, queue_logger, 0),
        )
        p.start()
        result = result_queue.get(block=True, timeout=job.timeout_seconds)
        p.join()
        result_queue.close()
        return result
    except Empty:
        return domain.JobResult.error(
            domain.error.JobTimedOut(
                f"The job, [{job.job_name}], timed out after {job.timeout_seconds} seconds."
            )
        )
    except Exception as e:
        return domain.JobResult.error(e)
    finally:
        job_logger = logger.new(name=job.job_name)
        for message in queue_logger.messages:
            if message.is_debug:
                job_logger.debug(message.message)
            elif message.is_error:
                job_logger.error(message.message)
            elif message.is_info:
                job_logger.info(message.message)


# noinspection PyBroadException
def run_job_with_retry(
    result_queue: mp.Queue,
    job: domain.Job,
    logger: domain.Logger,
    retries_so_far: int = 0,
) -> None:
    try:
        result = job.run(job.config, logger)
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
            raise


class QueueLogger(domain.Logger):
    def __init__(self, *, job_name: str):
        self._job_name = job_name

        self._messages: typing.List[domain.LogMessage] = []

    def debug(
        self, /, message: str, *, ts: typing.Optional[datetime.datetime] = None
    ) -> None:
        self._log(level=domain.LogLevel.Debug, message=message)

    def error(
        self, /, message: str, *, ts: typing.Optional[datetime.datetime] = None
    ) -> None:
        self._log(level=domain.LogLevel.Error, message=message)

    def exception(
        self, /, e: BaseException, *, ts: typing.Optional[datetime.datetime] = None
    ) -> None:
        message = domain.error.parse_exception(e).text()
        self._log(level=domain.LogLevel.Error, message=message)

    def info(
        self, /, message: str, *, ts: typing.Optional[datetime.datetime] = None
    ) -> None:
        self._log(level=domain.LogLevel.Info, message=message)

    @property
    def messages(self) -> typing.List[domain.LogMessage]:
        return self._messages

    def new(
        self,
        *,
        name: str,
        log_to_console: typing.Optional[bool] = None,
        min_log_level: typing.Optional[domain.LogLevel] = None,
    ) -> domain.Logger:
        return QueueLogger(job_name=self._job_name)

    def _log(self, *, level: domain.LogLevel, message: str) -> None:
        log_msg = domain.LogMessage(
            logger_name=self._job_name,
            level=level,
            message=message,
            ts=datetime.datetime.now(),
        )
        self._messages.append(log_msg)
