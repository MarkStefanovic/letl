import queue
import typing


__all__ = ("SetQueue",)

T = typing.TypeVar("T")


class SetQueue(queue.Queue, typing.Generic[T]):
    def _init(self, maxsize: int) -> None:
        self.queue = set()

    def _put(self, item: T) -> None:
        self.queue.add(item)

    def _get(self) -> T:
        return self.queue.pop()
