from uuid import uuid4

__all__ = ("generate",)


def generate() -> str:
    return uuid4().hex
