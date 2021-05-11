import typing

from letl import domain, Plan

__all__ = ("SAPlanRepo",)


class SAPlanRepo(domain.PlanRepo):
    def add(self, /, plan: Plan) -> int:
        pass

    def all(self) -> typing.Set[Plan]:
        pass

    def delete_stale_plans(self) -> None:
        pass
