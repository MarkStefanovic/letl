import enum


__all__ = ("Status",)


class Status(str, enum.Enum):
    Error = "failure"
    Running = "running"
    Skipped = "skipped"
    Success = "success"

    def __str__(self) -> str:
        return str.__str__(self)
