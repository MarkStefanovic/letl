import dataclasses
import typing

from letl.domain import cfg, logger as log, job_result, resource_manager, schedule

__all__ = ("Job",)


@dataclasses.dataclass(frozen=True)
class Job:
    job_name: str
    timeout_seconds: int
    retries: int
    run: typing.Callable[
        [cfg.Config, log.Logger, resource_manager.ResourceManager],
        typing.Optional[job_result.JobResult],
    ]
    schedule: typing.FrozenSet[schedule.Schedule]
    config: cfg.Config
    dependencies: typing.FrozenSet[str] = frozenset()
