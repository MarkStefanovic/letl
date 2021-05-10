import datetime
import typing

import sqlalchemy as sa

from letl import adapter, domain

__all__ = ("SALogger",)


class SALogger(domain.Logger):
    def __init__(
        self,
        *,
        batch_id: str,
        job_name: typing.Optional[str],
        con: sa.engine.Connection,
        log_to_console: bool = False,
    ):
        self._batch_id = batch_id
        self._job_name = job_name
        self._con = con
        self._log_to_console = log_to_console

        self.__repo: typing.Optional[domain.LogRepo] = None

    @property
    def _repo(self) -> domain.LogRepo:
        if self.__repo is None:
            self.__repo = adapter.SALogRepo(con=self._con)
        return self.__repo

    def _log(self, *, level: domain.LogLevel, message: str) -> None:
        self._repo.add(
            batch_id=self._batch_id,
            job_name=self._job_name,
            level=level,
            message=message,
        )
        if self._log_to_console:
            print(
                f"{datetime.datetime.now().strftime('%H:%M:%S')} ({level.value!s}) "
                f"[{self._job_name}]: {message}"
            )

    def error(self, /, message: str) -> None:
        return self._log(
            level=domain.LogLevel.error(),
            message=message,
        )

    def exception(self, /, e: Exception) -> None:
        msg = domain.error.parse_exception(e).text()
        self._log(level=domain.LogLevel.Error, message=msg)

    def info(self, /, message: str) -> None:
        return self._log(
            level=domain.LogLevel.info(),
            message=message,
        )
