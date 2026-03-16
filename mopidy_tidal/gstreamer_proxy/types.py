import urllib.parse
from dataclasses import dataclass
from typing import NamedTuple, Self


class FullRange(NamedTuple):
    start: int
    end: int
    total: int

    def to_header(self) -> str:
        return f"Content-Range: bytes {self.start}-{self.end}/{self.total}"


class Range(NamedTuple):
    start: int | None
    end: int | None

    def expand(self, total: int) -> FullRange:
        return FullRange(
            self.start or 0, total - 1 if self.end is None else self.end, total
        )

    def to_header(self) -> str | None:
        match (self.start, self.end):
            case int(a), int(b):
                return f"Range: bytes={a}-{b}"
            case None, int(b):
                return f"Range: bytes=-{b}"
            case int(a), None:
                return f"Range: bytes={a}-"
            case None, None:
                return None

    @classmethod
    def parse_header(cls, line: bytes | bytearray) -> Self:
        assert line.lower().startswith(b"range:")
        unit, range = line[7:].split(b"=")
        assert unit == b"bytes"
        assert b"," not in range, "Multiple ranges not supported"
        start, end = None, None
        a, _, b = range.strip().partition(b"-")
        if a:
            start = int(a)
        if b:
            end = int(b)
        return cls(start, end)

    def empty(self) -> bool:
        return self == (None, None)


@dataclass
class Request:
    """An HTTP request, minimally parsed."""

    path: urllib.parse.ParseResultBytes
    keep_alive: bool
    range: Range
    raw_first: bytes
    raw_host: bytes
    raw_rest: bytearray

    def into_remote(self, remote_host: str, remote_port: int) -> Self:
        """Transform this request to send it to the remote server."""
        self.raw_host = f"Host: {remote_host}:{remote_port}\r\n".encode()
        return self

    def raw(self) -> bytes:
        return self.raw_first + self.raw_host + self.raw_rest

    def cache_key(self) -> bytes:
        return self.path.path


class Head(NamedTuple):
    raw: bytes | bytearray
    content_length: int | None
    keep_alive: bool | None

    @classmethod
    def from_raw(cls, raw: bytes | bytearray) -> Self:
        length = None
        lower = raw.lower()

        try:
            idx = lower.index(b"content-length")
        except ValueError:
            pass
        else:
            length_line = raw[idx : idx + 100].splitlines()[0]
            _, val = length_line.split(b":")
            length = int(val)

        keep_alive = True if b"connection: keep-alive" in lower else None

        return cls(raw, length, keep_alive)


@dataclass
class StatusError(Exception):
    """Invalid status"""

    status: int

    def __repr__(self) -> str:
        return f"StatusError({self.status})"


@dataclass
class Buffer:
    """A buffer backed by a bytearray. The buffer will outgrow the stated capacity if needed."""

    capacity: int
    contains: int
    _data: bytearray

    @classmethod
    def with_capacity(cls, capacity: int) -> Self:
        assert capacity
        return cls(capacity, 0, bytearray(capacity))

    def data(self) -> memoryview:
        return memoryview(self._data)[: self.contains]

    def clear(self) -> None:
        self.contains = 0

    def extend(self, data: bytes) -> None:
        end = self.contains + len(data)
        diff = end - self.capacity
        if diff > 0:
            self._data.extend(b"\x00" * diff * 2)
            self.capacity = end
        self._data[self.contains : end] = data
        self.contains = end
