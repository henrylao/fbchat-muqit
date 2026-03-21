"""Persistent session storage backends."""

from .base import SessionStorage
from .json_store import JsonSessionStorage

__all__ = ["SessionStorage", "JsonSessionStorage", "RedisSessionStorage"]


def __getattr__(name: str):
    if name == "RedisSessionStorage":
        from .redis_store import RedisSessionStorage

        return RedisSessionStorage
    raise AttributeError(name)
