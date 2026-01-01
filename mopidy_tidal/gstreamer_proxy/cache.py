import sqlite3
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Callable, ContextManager, NewType, Self, Sequence, assert_never

Bytes = bytes | bytearray
Head = NewType("Head", bytes)
Path = NewType("Path", bytes)


@dataclass
class Chunk:
    data: Iterator[Bytes]
    total: int


class Insertion(ABC):
    @abstractmethod
    def save_head(self, head: Head) -> None:
        """Save the head for the given path."""

    @abstractmethod
    def save_body_chunk(self, data: Bytes, start: int) -> None:
        """Save a chunk of the body for the given path at the given offset."""

    @abstractmethod
    def finalise(self) -> None:
        """Mark this response as complete.

        Implementations may choose to compact data here.
        """


class Cache[I: Insertion](ABC):
    @abstractmethod
    def get_head(self, path: Path) -> Head | None:
        """Get the head if present for the given path."""

    @abstractmethod
    def get_body_chunk(self, path: Path, start: int, end: int) -> Chunk:
        """Get a chunk of the body in the given half-closed range."""

    @abstractmethod
    def get_body(self, path: Path) -> Chunk:
        """Get the whole body."""

    @abstractmethod
    def insertion(self, path: Path) -> ContextManager[I]: ...

    def init(self) -> None:
        """Initialise storage if needed."""


@dataclass
class SparseBuffer:
    offsets: Sequence[int]
    chunks: Callable[[int], Bytes]

    def get_range(self, start: int, end: int) -> Iterator[Bytes]:
        offsets = sorted(self.offsets)

        start_offset = None
        for offset in reversed(offsets):
            if offset <= start:
                start_offset = offset
                break
        if start_offset is None:
            raise KeyError("Data does not contain start range")

        end_offset = None
        for offset in offsets:
            if offset >= end:
                break
            else:
                end_offset = offset

        if end_offset is None:
            end_offset = offsets[-1]

        sent = 0

        def shift_out(
            chunk: Bytes, start: int | None = None, end: int | None = None
        ) -> Bytes:
            nonlocal sent
            data = chunk[start:end]
            sent += len(data)
            return data

        other_chunks = (
            shift_out(self.chunks(x))
            for x in self.offsets[
                offsets.index(start_offset) + 1 : offsets.index(end_offset)
            ]
        )

        assert start >= start_offset
        assert end >= end_offset

        yield shift_out(self.chunks(start_offset), start=start - start_offset)
        yield from other_chunks
        if start_offset != end_offset:
            yield shift_out(self.chunks(end_offset), end=end - end_offset)

        assert sent == end - start, f"Sent {sent}, end={end}, start={start}"

    @classmethod
    def from_dict(cls, data: dict[int, Bytes]) -> Self:
        return cls(tuple(data.keys()), data.__getitem__)

    @classmethod
    def from_db(cls, cur: sqlite3.Cursor, path: Path, *offsets: int) -> Self:
        def get_chunk(start: int) -> Bytes:
            cur.execute("SELECT data FROM body WHERE path=? and start=?", (path, start))
            return cur.fetchone()[0]

        return cls(offsets, get_chunk)


@dataclass
class DictInsertion(Insertion):
    head: Head | None = None
    body: dict[int, Bytes] = field(default_factory=dict)
    final: bool = False

    def save_head(self, head: Head) -> None:
        self.head = head

    def save_body_chunk(self, data: Bytes, start: int) -> None:
        self.body[start] = data

    def finalise(self) -> None:
        self.final = True


class DictCache(Cache[DictInsertion]):
    """A cache using dicts to store the data."""

    def __init__(self) -> None:
        self.heads = {}
        self.bodies = defaultdict(dict)

    def get_head(self, path: Path) -> Head | None:
        return self.heads.get(path)

    def get_body_chunk(self, path: Path, start: int, end: int) -> Chunk:
        data = SparseBuffer.from_dict(self.bodies[path]).get_range(start, end)
        total = sum(len(x) for x in self.bodies[path].values())
        return Chunk(data, total)

    def get_body(self, path: Path) -> Chunk:
        chunks = self.bodies[path]
        data = [chunks[x] for x in sorted(chunks)]
        total = sum(len(x) for x in self.bodies[path].values())
        return Chunk(iter(data), total)

    # TODO test this actually drops correctly
    @contextmanager
    def insertion(self, path: Path) -> Iterator[DictInsertion]:
        insertion = DictInsertion()

        yield insertion

        if insertion.head and insertion.final:
            self.heads[path] = insertion.head
            self.bodies[path] = insertion.body


@dataclass
class SqliteInsertion(Insertion):
    cur: sqlite3.Cursor
    path: Path
    final: bool = False

    def save_head(self, head: Head) -> None:
        self.cur.execute(
            "INSERT INTO head (path, data) VALUES (?, ?)", (self.path, head)
        )

    def save_body_chunk(self, data: Bytes, start: int) -> None:
        self.cur.execute(
            "INSERT INTO body (path, start, data, len) VALUES (?, ?, ?, ?)",
            (self.path, start, data, len(data)),
        )

    def finalise(self) -> None:
        self.final = True


def _metadata(cur: sqlite3.Cursor, path: Path) -> tuple[int, list[int]]:
    cur.execute("select start, len from body where path=?;", (path,))
    offsets = []
    total = 0
    for start, length in cur.fetchall():
        offsets.append(start)
        total += length

    return total, offsets


@dataclass
class SQLiteCache(Cache[SqliteInsertion]):
    conn: sqlite3.Connection

    def init(self) -> None:
        with self.conn as conn:
            conn.execute("""
create table if not exists head
(
   id integer primary key autoincrement
   , path varchar
   , data blob
   , timestamp timestamp default CURRENT_TIMESTAMP
   , last_used timestamp default CURRENT_TIMESTAMP
);
            """)
            conn.execute("""
create table if not exists body
(
   id integer primary key autoincrement
   , path varchar
   , start integer
   , data blob
   , len integer
   , timestamp timestamp default CURRENT_TIMESTAMP
   , last_used timestamp default CURRENT_TIMESTAMP
);
            """)
            conn.execute("create index if not exists head_idx on head (path);")
            conn.execute("create index if not exists body_idx on body (path);")

    def get_head(self, path: Path) -> Head | None:
        row = self.conn.execute(
            "SELECT data FROM head WHERE path=? LIMIT 1", (path,)
        ).fetchone()
        match row:
            case [head]:
                return Head(head)
            case None:
                return None
            case _:
                assert_never(row)

    def get_body(self, path: Path) -> Chunk:
        cur = self.conn.cursor()
        total, offsets = _metadata(cur, path)

        return Chunk(
            data=SparseBuffer.from_db(cur, path, *offsets).get_range(0, total),
            total=total,
        )

    def get_body_chunk(self, path: Path, start: int, end: int) -> Chunk:
        cur = self.conn.cursor()
        total, offsets = _metadata(cur, path)
        return Chunk(
            data=SparseBuffer.from_db(cur, path, *offsets).get_range(start, end),
            total=total,
        )

    @contextmanager
    def insertion(self, path: Path) -> Iterator[SqliteInsertion]:
        cur = self.conn.cursor()
        insertion = SqliteInsertion(cur, path)

        yield insertion

        if insertion.final:
            self.conn.commit()
