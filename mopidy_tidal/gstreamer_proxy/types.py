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
            self.start or 0, total if self.end is None else self.end, total
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


class Head(NamedTuple):
    raw: bytes | bytearray
    content_length: int | None

    @classmethod
    def from_raw(cls, raw: bytes | bytearray) -> Self:
        if (idx := raw.lower().index(b"content-length")) > 0:
            length_line = raw[idx : idx + 100].splitlines()[0]
            _, val = length_line.split(b":")
            return cls(raw, int(val))
        else:
            return cls(raw, None)
