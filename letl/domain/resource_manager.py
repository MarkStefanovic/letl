import collections
import typing

from letl.domain import error, logger, resource

__all__ = ("ResourceManager",)

Handle = typing.TypeVar("Handle")


class ResourceManager:
    def __init__(
        self,
        *,
        resources: typing.FrozenSet[resource.Resource[typing.Any]],
        log: logger.Logger,
    ):
        check_keys_are_unique(resources)

        self._resources = resources
        self._log = log

        self._handles: typing.Dict[
            resource.Resource[typing.Any], typing.Optional[typing.Any]
        ] = {res: None for res in resources}

    def close(self) -> None:
        for res, handle in self._handles.items():
            if handle is not None:
                try:
                    res.close(handle)
                except Exception as e:
                    self._log.error(
                        f"An error occurred while closing the resource, {res.key}: {e}"
                    )

    def key_exists(self, /, key: str) -> bool:
        return key in (res.key for res in self._resources)

    def get(self, /, key: str, resource_type: typing.Type[Handle]) -> Handle:
        try:
            res = next(res for res in self._resources if res.key == key)
            handle = res.open()
            assert isinstance(handle, resource_type)
            self._handles[res] = handle
            return handle
        except StopIteration:
            raise error.ResourceKeyNotFound(
                key=key, available_keys={res.key for res in self._resources}
            )

    @property
    def keys(self) -> set[str]:
        return {res.key for res in self._resources}

    def __eq__(self, other: object) -> bool:
        if isinstance(other, ResourceManager):
            return self.keys == other.keys
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self._resources)

    def __repr__(self) -> str:
        resources_str = ", ".join(sorted(res.key for res in self._resources))
        return f"{self.__class__.__name__}: {resources_str}"


def check_keys_are_unique(
    resources: typing.Iterable[resource.Resource[typing.Any]],
) -> None:
    keys = [res.key for res in resources]
    counts = {k: ct for k, ct in collections.Counter(keys).items() if ct > 1}
    if counts:
        raise error.DuplicateResourceKey(counts=counts)
