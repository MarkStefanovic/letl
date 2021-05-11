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
    started: datetime.datetime
    ended: typing.Optional[datetime.datetime]
    error_message: typing.Optional[str]

    @property
    def is_error(self) -> bool:
        return self.status == status.Status.Error

    @property
    def is_running(self) -> bool:
        return self.status == status.Status.Running

    @property
    def is_skipped(self) -> bool:
        return self.status == status.Status.Skipped

    # @staticmethod
    # def running(*, batch_id: str, job_name: str) -> JobStatus:
    #     return JobStatus(
    #         batch_id=batch_id,
    #         job_name=job_name,
    #         started=datetime.datetime.now(),
    #         ended=None,
    #         status=status.Status.Running,
    #         error_message=None,
    #         skipped_reason=None,
    #     )
    #
    # @staticmethod
    # def error(
    #     *,
    #     batch_id: str,
    #     job_name: str,
    #     started: datetime.datetime,
    #     error_message: str,
    # ) -> JobStatus:
    #     return JobStatus(
    #         batch_id=batch_id,
    #         job_name=job_name,
    #         started=started,
    #         ended=datetime.datetime.now(),
    #         status=status.Status.Error,
    #         error_message=error_message,
    #         skipped_reason=None,
    #     )
    #
    # @staticmethod
    # def skipped(
    #     *,
    #     batch_id: str,
    #     job_name: str,
    #     started: datetime.datetime,
    #     skipped_reason: str,
    # ) -> JobStatus:
    #     return JobStatus(
    #         batch_id=batch_id,
    #         job_name=job_name,
    #         started=started,
    #         ended=datetime.datetime.now(),
    #         status=status.Status.Skipped,
    #         error_message=None,
    #         skipped_reason=skipped_reason,
    #     )
    #
    # @staticmethod
    # def success(
    #     *,
    #     batch_id: str,
    #     job_name: str,
    #     started: datetime.datetime,
    # ) -> JobStatus:
    #     return JobStatus(
    #         batch_id=batch_id,
    #         job_name=job_name,
    #         started=started,
    #         ended=datetime.datetime.now(),
    #         status=status.Status.Success,
    #         error_message=None,
    #         skipped_reason=None,
    #     )
