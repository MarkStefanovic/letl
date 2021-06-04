import dataclasses
import typing

from letl.domain import logger as log
from letl.domain.job_result import JobResult
from letl.domain.schedule import Schedule

__all__ = ("Job",)


@dataclasses.dataclass(frozen=True)
class Job:
    job_name: str
    timeout_seconds: int
    dependencies: typing.FrozenSet[str]
    retries: int
    run: typing.Callable[
        [typing.Hashable, log.Logger],
        typing.Optional[JobResult],
    ]
    config: typing.Optional[typing.Hashable]
    schedule: typing.FrozenSet[Schedule]
