import datetime
import multiprocessing as mp
import threading
import typing

import sqlalchemy as sa

from letl import adapter, domain, Logger

__all__ = ("LoggerThread", "NamedLogger")

std_logger = domain.root_logger.getChild("sa_logger")


class NamedLogger(domain.Logger):
    def __init__(
        self,
        *,
        name: str,
        message_queue: "mp.Queue[domain.LogMessage]",
        min_log_level: domain.LogLevel = domain.LogLevel.Info,
    ):
        self._name = name
        self._message_queue = message_queue
        self._min_log_level = min_log_level

        self._recent_messages: typing.Dict[str, datetime.datetime] = {}

    def _log(
        self,
        *,
        level: domain.LogLevel,
        message: str,
        ts: typing.Optional[datetime.datetime] = None,
    ) -> None:
        try:
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
                    self._message_queue.put_nowait(msg)
                    self._std_log(level=level, message=message)
        except Exception as e:
            std_logger.exception(e)

    def _std_log(self, *, level: domain.LogLevel, message: str) -> None:
        msg = f"[{self._name}]: {message}"
        if level == domain.LogLevel.Debug:
            std_logger.debug(msg)
        elif level == domain.LogLevel.Info:
            std_logger.info(msg)
        elif level == domain.LogLevel.Error:
            std_logger.error(msg)

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
        self, /, e: Exception, *, ts: typing.Optional[datetime.datetime] = None
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

    @property
    def name(self) -> str:
        return self._name

    def new(
        self,
        *,
        name: str,
        log_to_console: typing.Optional[bool] = None,
        min_log_level: typing.Optional[domain.LogLevel] = None,
    ) -> Logger:
        return NamedLogger(
            name=name,
            message_queue=self._message_queue,
            min_log_level=min_log_level or self._min_log_level,
        )


class LoggerThread(threading.Thread):
    def __init__(
        self,
        *,
        message_queue: "mp.Queue[domain.LogMessage]",
        engine: sa.engine.Engine,
    ):
        super().__init__()

        self._message_queue = message_queue

        self._repo = adapter.SALogRepo(engine=engine)

    def run(self) -> None:
        while True:
            # noinspection PyBroadException
            try:
                msg: domain.LogMessage = self._message_queue.get()
                self._repo.add(
                    name=msg.logger_name,
                    level=msg.level,
                    message=msg.message,
                    ts=msg.ts or datetime.datetime.now(),
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except EOFError:
                break
            except Exception as e:
                std_logger.exception(e)
