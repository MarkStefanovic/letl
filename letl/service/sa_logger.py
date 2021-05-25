import datetime
import logging
import threading
import types
import typing

import pykka
import sqlalchemy as sa

from letl import adapter, domain, Logger

__all__ = (
    "NamedLogger",
    "SALogger",
)

mod_logger = domain.root_logger.getChild("sa_logger")


class SALogger(pykka.ThreadingActor):
    def __init__(self, *, engine: sa.engine.Engine):
        super().__init__()

        self._logger = mod_logger.getChild(self.__class__.__name__)

        self._repo = adapter.SALogRepo(engine=engine)

    def on_failure(
        self,
        exception_type: typing.Type[BaseException],
        exception_value: BaseException,
        traceback: types.TracebackType,
    ) -> None:
        self._logger.debug("on_failure(...) called")
        msg = domain.error.parse_exception(exception_value).text()
        self._repo.add(
            name=f"{self.__class__.__name__}.on_failure",
            level=domain.LogLevel.Error,
            message=msg,
        )

    def on_start(self) -> None:
        self._logger.debug("on_start() called")
        self._logger.info("Connected.")

    def on_receive(self, message: domain.LogMessage) -> None:
        self._repo.add(
            name=message.logger_name,
            level=message.level,
            message=message.message,
        )


class NamedLogger(domain.Logger):
    def __init__(
        self,
        *,
        name: str,
        sql_logger: pykka.ActorRef,
        log_to_console: bool = False,
        min_log_level: domain.LogLevel = domain.LogLevel.Info,
    ):
        self._name = name
        self._sql_logger = sql_logger
        self._log_to_console = log_to_console
        self._min_log_level = min_log_level

        self._recent_messages: typing.Dict[str, datetime.datetime] = {}

    def _log(
        self,
        *,
        level: domain.LogLevel,
        message: str,
        ts: typing.Optional[datetime.datetime] = None,
    ) -> None:
        last_actor_dead_msg: typing.Optional[datetime.datetime] = None
        if level == domain.LogLevel.Error:
            over_threshold = True
        elif level == domain.LogLevel.Info and self._min_log_level in (
            domain.LogLevel.Info,
            domain.LogLevel.Error,
        ):
            over_threshold = True
        elif self._min_log_level == domain.LogLevel.Debug:
            over_threshold = True
        else:
            over_threshold = False
        if over_threshold:
            self._recent_messages = {
                msg: last_sent
                for msg, last_sent in sorted(
                    self._recent_messages.items(),
                    key=lambda tup: tup[1],
                    reverse=True,
                )[:30]
            }
            if message in self._recent_messages:
                last_sent = self._recent_messages[message]
                if last_sent:
                    seconds_since_last_sent: typing.Optional[float] = (
                        datetime.datetime.now() - last_sent
                    ).total_seconds()
                else:
                    seconds_since_last_sent = None
            else:
                seconds_since_last_sent = None

            if not seconds_since_last_sent or seconds_since_last_sent > 10:
                self._recent_messages[message] = datetime.datetime.now()
                if ts is None:
                    ts = datetime.datetime.now()
                msg = domain.LogMessage(
                    logger_name=self._name, level=level, message=message, ts=ts
                )
                try:
                    self._sql_logger.tell(msg)
                except pykka.ActorDeadError:
                    if last_actor_dead_msg is None:
                        show_actor_dead_msg = True
                    elif (
                        datetime.datetime.now() - last_actor_dead_msg
                    ).total_seconds() > 30:
                        show_actor_dead_msg = True
                    else:
                        show_actor_dead_msg = False
                    if show_actor_dead_msg:
                        print(f"The logger actor is dead! Printing to console instead.")
                    print(msg)
                if self._log_to_console:
                    print(
                        f"{datetime.datetime.now().strftime('%H:%M:%S')} ({level.value!s}) "
                        f"[{self._name}]: {message}"
                    )

    def debug(
        self, /, message: str, *, ts: typing.Optional[datetime.datetime] = None
    ) -> None:
        return self._log(
            level=domain.LogLevel.Debug,
            message=message,
        )

    def error(
        self, /, message: str, *, ts: typing.Optional[datetime.datetime] = None
    ) -> None:
        return self._log(
            level=domain.LogLevel.Error,
            message=message,
        )

    def exception(
        self, /, e: BaseException, *, ts: typing.Optional[datetime.datetime] = None
    ) -> None:
        msg = domain.error.parse_exception(e).text()
        self._log(level=domain.LogLevel.Error, message=msg)

    def info(
        self, /, message: str, *, ts: typing.Optional[datetime.datetime] = None
    ) -> None:
        return self._log(
            level=domain.LogLevel.Info,
            message=message,
        )

    def new(
        self,
        *,
        name: str,
        log_to_console: typing.Optional[bool] = None,
        min_log_level: typing.Optional[domain.LogLevel] = None,
    ) -> Logger:
        return NamedLogger(
            name=name,
            sql_logger=self._sql_logger,
            log_to_console=log_to_console or self._log_to_console,
            min_log_level=min_log_level or self._min_log_level,
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
    job_logger = NamedLogger(name="test", sql_logger=logger_actor, log_to_console=True)
    job_logger.info("test")
    time.sleep(1)
    job_logger.info("test2")
    time.sleep(1)
    logger_actor.stop()
