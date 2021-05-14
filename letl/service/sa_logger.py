import dataclasses
import datetime
import logging
import threading
import types
import typing

import pykka
import sqlalchemy as sa

from letl import adapter, domain

__all__ = (
    "JobLogger",
    "SALogger",
)

mod_logger = domain.root_logger.getChild("sa_logger")


@dataclasses.dataclass(frozen=True)
class LogMessage:
    name: str
    level: domain.LogLevel
    message: str


class SALogger(pykka.ThreadingActor):
    def __init__(self, *, db_uri: str):
        super().__init__()

        self._db_uri = db_uri

        self._logger = mod_logger.getChild(self.__class__.__name__)

        self._engine: typing.Optional[sa.engine.Engine] = None
        self._con: typing.Optional[sa.engine.Connection] = None
        self._repo: typing.Optional[domain.LogRepo] = None

    def on_failure(
        self,
        exception_type: typing.Type[BaseException],
        exception_value: BaseException,
        traceback: types.TracebackType,
    ) -> None:
        self._logger.debug("on_failure(...) called")
        msg = domain.error.parse_exception(exception_value).text()
        if self._repo:
            self._repo.add(
                name=f"{self.__class__.__name__}.on_failure",
                level=domain.LogLevel.Error,
                message=msg,
            )

        self._cleanup()

    def on_start(self) -> None:
        self._logger.debug("on_start() called")
        self._engine = sa.create_engine(self._db_uri, echo=False)
        self._con = self._engine.connect()
        self._logger.info("Connected.")

        if self._db_uri == "sqlite://":
            self._con.execute("ATTACH ':memory:' as etl")
            adapter.db.create_tables(engine=self._engine)

        self._repo = adapter.SALogRepo(con=self._con)

    def on_stop(self) -> None:
        self._logger.debug("on_stop() called")
        self._cleanup()

    def on_receive(self, message: LogMessage) -> None:
        if self._repo:
            self._repo.add(
                name=message.name,
                level=message.level,
                message=message.message,
            )
        else:
            self._logger.error("repo has not been instantiated.")

    def _cleanup(self):
        self._logger.debug("_cleanup() called")
        if self._con:
            self._logger.debug("Closing connection...")
            self._con.close()
            self._logger.info("Connection closed.")
        self._con = None
        self._engine = None
        self._repo = None


class JobLogger(domain.Logger):
    def __init__(
        self,
        *,
        name: typing.Optional[str],
        sql_logger: pykka.ActorRef,
        log_to_console: bool = False,
    ):
        self._job_name = name
        self._sql_logger = sql_logger
        self._log_to_console = log_to_console

    def _log(self, *, level: domain.LogLevel, message: str) -> None:
        msg = LogMessage(job_name=self._job_name, level=level, message=message)
        self._sql_logger.tell(msg)
        if self._log_to_console:
            print(
                f"{datetime.datetime.now().strftime('%H:%M:%S')} ({level.value!s}) "
                f"[{self._job_name}]: {message}"
            )

    def debug(self, /, message: str) -> None:
        return self._log(
            level=domain.LogLevel.Debug,
            message=message,
        )

    def error(self, /, message: str) -> None:
        return self._log(
            level=domain.LogLevel.Error,
            message=message,
        )

    def exception(self, /, e: Exception) -> None:
        msg = domain.error.parse_exception(e).text()
        self._log(level=domain.LogLevel.Error, message=msg)

    def info(self, /, message: str) -> None:
        return self._log(
            level=domain.LogLevel.Info,
            message=message,
        )


if __name__ == "__main__":
    import time

    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger().setLevel(logging.DEBUG)
    # logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.ERROR)
    logging.getLogger("pykka").setLevel(logging.DEBUG)
    print(threading.currentThread())
    db_uri = "sqlite://"

    logger_actor = SALogger.start(db_uri=db_uri)
    # proxy = logger_actor.proxy()
    job_logger = JobLogger(name="test", sql_logger=logger_actor, log_to_console=True)
    job_logger.info("test")
    time.sleep(1)
    job_logger.info("test2")
    time.sleep(1)
    logger_actor.stop()
