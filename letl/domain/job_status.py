from __future__ import annotations

import dataclasses
import datetime
import typing

from letl.domain import status

__all__ = ("JobStatus",)


@dataclasses.dataclass(frozen=True)
class JobStatus:
    batch_id: str
    job_name: str
    status: status.Status
    skipped_reason: typing.Optional[str]
    execution_millis: typing.Optional[int]
    ts: datetime.datetime
    error: typing.Optional[str]

    @staticmethod
    def running(*, batch_id: str, job_name: str) -> JobStatus:
        return JobStatus(
            batch_id=batch_id,
            job_name=job_name,
            ts=datetime.datetime.now(),
            execution_millis=None,
            status=status.Status.Running,
            error=None,
            skipped_reason=None,
        )

    @staticmethod
    def error(*, batch_id: str, job_name: str, error: str) -> JobStatus:
        return JobStatus(
            batch_id=batch_id,
            job_name=job_name,
            ts=datetime.datetime.now(),
            execution_millis=None,
            status=status.Status.Error,
            error=error,
            skipped_reason=None,
        )

    @staticmethod
    def skipped(*, batch_id: str, job_name: str, reason: str) -> JobStatus:
        return JobStatus(
            batch_id=batch_id,
            job_name=job_name,
            ts=datetime.datetime.now(),
            execution_millis=None,
            status=status.Status.Skipped,
            error=None,
            skipped_reason=reason,
        )

    @staticmethod
    def success(*, batch_id: str, job_name: str, execution_millis: int) -> JobStatus:
        return JobStatus(
            batch_id=batch_id,
            job_name=job_name,
            ts=datetime.datetime.now(),
            execution_millis=execution_millis,
            status=status.Status.Success,
            error=None,
            skipped_reason=None,
        )
