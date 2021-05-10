from __future__ import annotations

import dataclasses
import datetime
import typing

from letl.domain import logger as log

__all__ = ("Job",)

Cfg = typing.TypeVar(
    "Cfg",
    bound=typing.Dict[
        str, typing.Union[bool, datetime.date, datetime.datetime, int, str]
    ],
)


@dataclasses.dataclass(frozen=True)
class Job:
    job_name: str
    min_seconds_between_refreshes: int
    timeout_seconds: int
    dependencies: typing.Set[str]
    retries: int
    run: typing.Callable[[Cfg, log.Logger], None]
    config: Cfg


if __name__ == "__main__":

    def dummy(
        config: typing.Dict[str, typing.Union[int, str]], logger: log.Logger
    ) -> None:
        print(config)

    j = Job(
        job_name="test_job",
        min_seconds_between_refreshes=300,
        timeout_seconds=3600,
        retries=1,
        dependencies={"test_job_0"},
        run=dummy,
        config={},
    )
    print(j)
