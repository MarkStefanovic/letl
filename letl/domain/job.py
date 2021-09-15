import dataclasses
import typing

from letl.domain import logger as log
from letl.domain.job_result import JobResult
from letl.domain.resource_manager import ResourceManager
from letl.domain.schedule import Schedule

__all__ = ("Job",)


@dataclasses.dataclass(frozen=True)
class Job:
    job_name: str
    timeout_seconds: int
    retries: int
    run: typing.Callable[
        [typing.Hashable, log.Logger, ResourceManager],
        typing.Optional[JobResult],
    ]
    schedule: typing.FrozenSet[Schedule]
    config: typing.Optional[typing.Hashable] = None
    dependencies: typing.FrozenSet[str] = frozenset()
