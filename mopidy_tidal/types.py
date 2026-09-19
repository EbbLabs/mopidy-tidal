from dataclasses import dataclass
from enum import Enum
from typing import Callable

_UNSET = Enum("_UNSET", "_UNSET")


@dataclass
class Lazy[T]:
    """Type-safe T | None for lazy attributes."""

    inner: T | _UNSET = _UNSET._UNSET

    def get(self) -> T:
        if self.inner is _UNSET._UNSET:
            raise ValueError("Lazy value not set")
        else:
            return self.inner

    def set(self, val: T) -> None:
        self.inner = val

    def get_or(self, init: Callable[[], T]) -> T:
        if self.inner is _UNSET._UNSET:
            self.set(init())
        return self.get()

    def get_or_none(self) -> T | None:
        if self.inner is _UNSET._UNSET:
            return None
        else:
            return self.inner
