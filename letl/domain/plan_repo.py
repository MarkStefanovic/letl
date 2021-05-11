import abc
import typing

from letl.domain.plan import Plan

__all__ = ("PlanRepo",)


class PlanRepo(abc.ABC):
    @abc.abstractmethod
    def add(self, /, plan: Plan) -> int:
        raise NotImplementedError

    @abc.abstractmethod
    def all(self) -> typing.Set[Plan]:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_stale_plans(self) -> None:
        raise NotImplementedError
