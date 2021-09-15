import abc
import types
import typing

__all__ = ("Resource",)

Handle = typing.TypeVar("Handle")


class Resource(abc.ABC, typing.Generic[Handle]):
    def __init__(self, *, key: str):
        self._key = key

        self._handle: typing.Optional[Handle] = None

    @abc.abstractmethod
    def open(self) -> Handle:
        raise NotImplementedError

    @abc.abstractmethod
    def close(self, /, handle: Handle) -> None:
        raise NotImplementedError

    @property
    def key(self) -> str:
        return self._key

    def __eq__(self, other: object) -> bool:
        if other.__class__ is self.__class__:
            return self.key == typing.cast(Resource[typing.Any], other).key
        else:
            return NotImplemented

    def __enter__(self) -> Handle:
        if self._handle is None:
            self._handle = self.open()
        return self._handle

    def __exit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        exc_val: typing.Optional[BaseException],
        exc_tb: typing.Optional[types.TracebackType],
    ) -> typing.Literal[False]:
        if self._handle is not None:
            self.close(self._handle)
        return False

    def __hash__(self) -> int:
        return hash(self._key)

    def __ne__(self, other: object) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return NotImplemented
        else:
            return not result

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}: {self._key}"
