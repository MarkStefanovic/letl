from __future__ import annotations

import dataclasses
import typing

from letl.domain import logger as log
from letl.domain.schedule import Schedule

__all__ = ("Job",)


@dataclasses.dataclass(frozen=True)
class Job:
    job_name: str
    timeout_seconds: int
    dependencies: typing.Set[str]
    retries: int
    run: typing.Callable[[typing.Dict[str, typing.Any], log.Logger], None]
    config: typing.Dict[str, typing.Any]
    schedule: typing.List[Schedule]
