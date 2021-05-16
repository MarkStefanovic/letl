from __future__ import annotations

import dataclasses
import typing

from letl.domain import error

__all__ = ("JobResult",)


@dataclasses.dataclass(frozen=True)
class JobResult:
    is_success: bool
    is_error: bool
    error_message: typing.Optional[str]

    @staticmethod
    def success() -> JobResult:
        return JobResult(
            is_success=True,
            is_error=False,
            error_message=None,
        )

    @staticmethod
    def error(e: Exception, /) -> JobResult:
        msg = error.parse_exception(e).text()
        return JobResult(
            is_success=False,
            is_error=True,
            error_message=msg,
        )
