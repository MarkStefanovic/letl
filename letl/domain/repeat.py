import typing
import threading

__all__ = ("repeat",)


def repeat(seconds: int, fn, **kwargs: typing.Any) -> typing.Callable[[], None]:
    stopped = threading.Event()

    def loop() -> None:
        while not stopped.wait(seconds):
            fn(**kwargs)

    threading.Thread(target=loop).start()
    return stopped.set
