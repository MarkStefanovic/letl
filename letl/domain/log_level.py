import enum


__all__ = ("LogLevel",)


class LogLevel(str, enum.Enum):
    Debug = "DEBUG"
    Info = "INFO"
    Error = "ERROR"

    def __str__(self) -> str:
        return str.__str__(self)
