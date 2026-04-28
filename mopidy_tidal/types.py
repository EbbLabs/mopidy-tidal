from dataclasses import dataclass
from typing import Callable


@dataclass
class Lazy[T]:
    """Type-safe T | None for lazy attributes."""

    inner: T | None = None

    def get(self) -> T:
        if self.inner is None:
            raise ValueError("Lazy value not set")
        else:
            return self.inner

    def set(self, val: T) -> None:
        self.inner = val

    def get_or(self, init: Callable[[], T]) -> T:
        if self.inner is None:
            self.set(init())
        return self.get()

    def get_or_none(self) -> T | None:
        return self.inner
