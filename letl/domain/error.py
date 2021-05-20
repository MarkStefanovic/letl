import dataclasses
import pathlib
import typing
import traceback

__all__ = (
    "DuplicateJobNames",
    "parse_exception",
)


@dataclasses.dataclass(frozen=True)
class Frame:
    file: str
    frame: str
    line: int
    code: str

    def __str__(self) -> str:
        return f"{self.file} [{self.line}]: {self.code}"


@dataclasses.dataclass(frozen=True)
class ExceptionInfo:
    error_type: str
    error_msg: str
    frames: typing.List[Frame]

    def text(self) -> str:
        if self.frames:
            prefix = "\n  > "
            frames = prefix + prefix.join(str(f) for f in self.frames)
        else:
            frames = ""
        return f"{self.error_type!s}: {self.error_msg!s}" + frames


def parse_exception(e: BaseException) -> ExceptionInfo:
    frames = [
        Frame(
            file=pathlib.Path(f.filename).name,
            frame=f.name,
            line=f.lineno,
            code=f.line,
        )
        for f in traceback.extract_tb(e.__traceback__)
    ]
    return ExceptionInfo(
        error_type=type(e).__name__,
        error_msg=str(e),
        frames=frames,
    )


class LetlError(Exception):
    def __init__(self, /, message: str):
        super().__init__(message)
        self.message = message


class DuplicateJobNames(LetlError):
    def __init__(self, jobs: typing.Set[str]):
        self.message = "The following job names are duplicated: " + ", ".join(jobs)
        super().__init__(self.message)


class JobTimedOut(LetlError):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class MissingJobImplementation(LetlError):
    def __init__(self, *, job_name: str):
        self.job_name = job_name
        self.message = f"No implementation was found for the job, {job_name}."
        super().__init__(self.message)
