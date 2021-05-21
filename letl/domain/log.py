import logging

__all__ = ("root_logger",)

root_logger = logging.getLogger("letl")
root_logger.addHandler(logging.NullHandler())
