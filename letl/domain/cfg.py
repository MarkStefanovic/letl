from __future__ import annotations

import typing

from letl.domain import error

__all__ = ("Config", "config")

T = typing.TypeVar("T", bound=typing.Hashable)


class Config:
    def __init__(
        self,
        /,
        options: typing.FrozenSet[typing.Tuple[str, typing.Hashable]] = frozenset(),
    ):
        self._options = options

    def add_option(self, key: str, value: typing.Hashable) -> Config:
        options = frozenset((k, value if key == k else v) for k, v in self._options)
        return Config(options)

    def add_options(self, **kwargs: typing.Hashable) -> Config:
        options = dict(self._options) | {k: v for k, v in kwargs.items()}
        return Config(frozenset((k, v) for k, v in options.items()))

    def key_exists(self, /, key: str) -> bool:
        return key in self.keys

    def get(self, /, key: str, _type: typing.Type[T]) -> T:
        try:
            value = next(v for k, v in self._options if k == key)
            assert isinstance(value, _type)
            return value
        except StopIteration:
            raise error.OptionNotFound(key=key, available_keys=self.keys)

    @property
    def keys(self) -> set[str]:
        return {k for k, v in self._options}

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Config):
            return self._options == other._options
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self._options)

    def __repr__(self) -> str:
        options = ", ".join(sorted(f"{k} = {v!r}" for k, v in self._options))
        return f"{self.__class__.__name__}: {options}"

    def __str__(self) -> str:
        options = ", ".join(sorted(f"  {k}: {v!r}" for k, v in self._options))
        return f"{self.__class__.__name__}:\n  {options}"


def config(**kwargs: typing.Any) -> Config:
    return Config(frozenset((k, v) for k, v in kwargs.items()))
