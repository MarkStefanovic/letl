from __future__ import annotations

import dataclasses
import typing

from letl.domain import error

__all__ = ("JobResult",)


@dataclasses.dataclass(frozen=True)
class JobResult:
    is_error: bool
    is_skipped: bool
    is_success: bool
    error_message: typing.Optional[str]
    skipped_reason: typing.Optional[str]

    @staticmethod
    def error(e: Exception, /) -> JobResult:
        msg = error.parse_exception(e).text()
        return JobResult(
            is_error=True,
            is_skipped=False,
            is_success=False,
            error_message=msg,
            skipped_reason=None,
        )

    @staticmethod
    def skipped(*, reason: str) -> JobResult:
        return JobResult(
            is_error=False,
            is_skipped=True,
            is_success=True,
            error_message=None,
            skipped_reason=reason,
        )

    @staticmethod
    def success() -> JobResult:
        return JobResult(
            is_error=False,
            is_skipped=False,
            is_success=True,
            error_message=None,
            skipped_reason=None,
        )
