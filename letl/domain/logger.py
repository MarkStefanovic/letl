import abc

__all__ = ("Logger",)


class Logger(abc.ABC):
    @abc.abstractmethod
    def debug(self, /, message: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def error(self, /, message: str) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def exception(self, /, e: Exception) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def info(self, /, message: str) -> None:
        raise NotImplementedError
