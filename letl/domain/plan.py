from __future__ import annotations

import dataclasses
import datetime
import typing

__all__ = ("Plan",)


@dataclasses.dataclass(frozen=True)
class Plan:
    run_flag: bool
    skip_flag: bool
    skip_reason: typing.Optional[str]
    added: datetime.datetime

    @staticmethod
    def run():
        return Plan(
            run_flag=True,
            skip_flag=False,
            skip_reason=None,
            added=datetime.datetime.now(),
        )

    @staticmethod
    def skip(*, reason: str) -> Plan:
        return Plan(
            run_flag=False,
            skip_flag=True,
            skip_reason=reason,
            added=datetime.datetime.now(),
        )
