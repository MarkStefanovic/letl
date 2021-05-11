from __future__ import annotations

import abc
import typing

__all__ = ("Scheduler",)


class Scheduler(abc.ABC):
    def schedule(self) -> None:
        raise NotImplementedError
