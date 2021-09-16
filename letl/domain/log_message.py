import dataclasses
import datetime

from letl.domain import log_level

__all__ = ("LogMessage",)


@dataclasses.dataclass(frozen=True)
class LogMessage:
    logger_name: str
    level: log_level.LogLevel
    message: str
    ts: datetime.datetime

    @property
    def is_debug(self) -> bool:
        if self.level == log_level.LogLevel.Debug:
            return True
        return False

    @property
    def is_error(self) -> bool:
        if self.level == log_level.LogLevel.Error:
            return True
        return False

    @property
    def is_info(self) -> bool:
        if self.level == log_level.LogLevel.Info:
            return True
        return False

    def __str__(self) -> str:
        return f"{self.ts.strftime('%H:%M:%S')} ({self.level}): {self.message}"
