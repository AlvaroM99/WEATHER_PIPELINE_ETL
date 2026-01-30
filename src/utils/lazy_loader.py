"""
Lazy Loading Utilities

Provides a decorator for lazy-loading configuration values.
Centralizes the memoization pattern used across config modules.
"""

from __future__ import annotations

from functools import wraps
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


def lazy_load(func: Callable[[], T]) -> Callable[[], T]:
    """
    Decorator that caches the result of a function call.

    The decorated function will only be called once; subsequent calls
    return the cached value. This is useful for lazy-loading configuration
    values that are expensive to compute or require I/O.

    Usage:
        @lazy_load
        def get_api_key():
            return secrets_manager.get_api_key()

        # First call executes the function
        key = get_api_key()

        # Second call returns cached value
        key = get_api_key()

    Args:
        func: A no-argument function that returns the value to cache

    Returns:
        A wrapper function that caches and returns the result
    """
    cache: dict[str, T] = {}

    @wraps(func)
    def wrapper() -> T:
        if "value" not in cache:
            cache["value"] = func()
        return cache["value"]

    # Allow cache clearing for testing
    def clear_cache() -> None:
        cache.clear()

    wrapper.clear_cache = clear_cache  # type: ignore[attr-defined]

    return wrapper


class LazyProperty:
    """
    Descriptor for lazy-loading class properties.

    Similar to @property but caches the result after first access.

    Usage:
        class Config:
            @LazyProperty
            def api_key(self):
                return expensive_fetch()

        config = Config()
        key = config.api_key  # Computed once
        key = config.api_key  # Returns cached value
    """

    def __init__(self, func: Callable[[Any], T]) -> None:
        self.func = func
        self.attr_name: Optional[str] = None

    def __set_name__(self, owner: type, name: str) -> None:
        self.attr_name = f"_lazy_{name}"

    def __get__(self, obj: Any, objtype: Optional[type] = None) -> T:
        if obj is None:
            return self  # type: ignore[return-value]

        if self.attr_name is None:
            raise RuntimeError("LazyProperty not properly initialized")

        if not hasattr(obj, self.attr_name):
            setattr(obj, self.attr_name, self.func(obj))

        return getattr(obj, self.attr_name)
