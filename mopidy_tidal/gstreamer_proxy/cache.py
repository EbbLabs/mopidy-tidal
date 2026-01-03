import sqlite3
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from logging import getLogger
from typing import (
    Callable,
    ContextManager,
    NamedTuple,
    NewType,
    Self,
    Sequence,
    assert_never,
)
from uuid import uuid4

logger = getLogger()

Bytes = bytes | bytearray
Head = NewType("Head", bytes)
Path = NewType("Path", bytes)
EntryID = NewType("EntryID", bytes)


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
    def get_body_chunk(self, path: Path, start: int, end: int) -> Chunk | None:
        """Get a chunk of the body in the given half-closed range."""

    @abstractmethod
    def get_body(self, path: Path) -> Chunk | None:
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
        end += 1  # http uses closed ranges
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

        logger.debug("Sent %s", sent)
        assert sent == end - start, f"Sent {sent}, end={end}, start={start}"

    @classmethod
    def from_dict(cls, data: dict[int, Bytes]) -> Self:
        return cls(tuple(data.keys()), data.__getitem__)

    @classmethod
    def from_db(cls, cur: sqlite3.Cursor, entry_id: EntryID, *offsets: int) -> Self:
        def get_chunk(start: int) -> Bytes:
            cur.execute(
                "SELECT data FROM body WHERE entry_id=? AND start=?", (entry_id, start)
            )
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
        assert start not in self.body, start
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

    def get_body_chunk(self, path: Path, start: int, end: int) -> Chunk | None:
        chunks = self.bodies[path]
        if chunks:
            data = SparseBuffer.from_dict(chunks).get_range(start, end)
            total = sum(len(x) for x in chunks.values())
            return Chunk(data, total)
        else:
            return None

    def get_body(self, path: Path) -> Chunk | None:
        chunks = self.bodies[path]
        if chunks:
            data = [chunks[x] for x in sorted(chunks)]
            total = sum(len(x) for x in self.bodies[path].values())
            return Chunk(iter(data), total)
        else:
            return None

    # TODO test this actually drops correctly
    @contextmanager
    def insertion(self, path: Path) -> Iterator[DictInsertion]:
        insertion = DictInsertion()

        yield insertion

        if insertion.head and insertion.final:
            self.heads[path] = insertion.head
            self.bodies[path] = insertion.body


def entry_id() -> EntryID:
    return EntryID(uuid4().bytes)


@dataclass
class SqliteInsertion(Insertion):
    cur: sqlite3.Cursor
    path: Path
    final: bool = False
    entry_id: EntryID = field(default_factory=entry_id)

    def save_head(self, head: Head) -> None:
        self.cur.execute(
            "INSERT INTO head (entry_id, path, data) VALUES (?, ?, ?)",
            (self.entry_id, self.path, head),
        )

    def save_body_chunk(self, data: Bytes, start: int) -> None:
        self.cur.execute(
            "INSERT INTO body (entry_id, path, start, data, len) VALUES (?, ?, ?, ?, ?)",
            (self.entry_id, self.path, start, data, len(data)),
        )

    def finalise(self) -> None:
        self.final = True


class Metadata(NamedTuple):
    total: int
    offsets: list[int]
    entry_id: EntryID


def _metadata(cur: sqlite3.Cursor, path: Path) -> Metadata | None:
    cur.execute(
        """
WITH target AS (
  SELECT entry_id FROM head
  WHERE path=? AND is_final
  ORDER BY last_used DESC
  LIMIT 1
)
SELECT
  start
  , len
  , body.entry_id
FROM body
JOIN target ON target.entry_id=body.entry_id
ORDER BY start ASC
    """,
        (path,),
    )
    offsets = []
    total = 0
    entry_id = None
    for start, length, entry_id in cur.fetchall():
        offsets.append(start)
        total += length

    if entry_id:
        return Metadata(total, offsets, entry_id)
    else:
        return None


@dataclass
class SQLiteCache(Cache[SqliteInsertion]):
    conn: sqlite3.Connection

    def init(self) -> None:
        with self.conn as conn:
            conn.execute("""
CREATE TABLE IF NOT EXISTS head
(
   id INTEGER PRIMARY KEY AUTOINCREMENT
   , entry_id BLOB
   , is_final INTEGER DEFAULT FALSE
   , path VARCHAR
   , data BLOB
   , timestamp INTEGER DEFAULT (unixepoch('now'))
   , last_used INTEGER DEFAULT (unixepoch('now'))
);
            """)
            conn.execute("""
CREATE TABLE IF NOT EXISTS body
(
   id INTEGER PRIMARY KEY AUTOINCREMENT
   , entry_id BLOB
   , is_final INTEGER DEFAULT FALSE
   , path VARCHAR
   , start INTEGER
   , data BLOB
   , len INTEGER
   , timestamp INTEGER DEFAULT (unixepoch('now'))
   , FOREIGN KEY(entry_id) REFERENCES head(entry_id) ON DELETE CASCADE
);
            """)
            conn.execute("""
CREATE TABLE IF NOT EXISTS metadata
(
   id INTEGER PRIMARY KEY AUTOINCREMENT
   , schema_version VARCHAR NOT NULL
   , extra VARCHAR
);
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS head_path_idx ON head (path);")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS HEAD_ENTRY_IDX ON head (entry_id);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS BODY_ENTRY_IDX ON body (entry_id);"
            )
            if not conn.execute("SELECT count(*) FROM metadata").fetchone()[0]:
                conn.execute(
                    "INSERT INTO metadata (schema_version, extra) VALUES (?, ?)",
                    ("v0.1.0", "{}"),
                )

            conn.execute("DELETE FROM head WHERE NOT is_final;")
            conn.execute("DELETE FROM body WHERE NOT is_final;")

    def get_head(self, path: Path) -> Head | None:
        with self.conn as conn:
            row = conn.execute(
                """
WITH target AS (
  SELECT id FROM head
  WHERE path=? AND is_final
  ORDER BY last_used DESC
  LIMIT 1
)
UPDATE head
SET last_used=unixepoch('now')
WHERE id in (SELECT id FROM target)
RETURNING data
            """,
                (path,),
            ).fetchone()
        match row:
            case [head]:
                return Head(head)
            case None:
                return None
            case _:
                assert_never(row)

    def get_body(self, path: Path) -> Chunk | None:
        cur = self.conn.cursor()
        if metadata := _metadata(cur, path):
            total, offsets, entry_id = metadata
            return Chunk(
                data=SparseBuffer.from_db(cur, entry_id, *offsets).get_range(
                    0, total - 1
                ),
                total=total,
            )

    def get_body_chunk(self, path: Path, start: int, end: int) -> Chunk | None:
        cur = self.conn.cursor()
        if metadata := _metadata(cur, path):
            total, offsets, entry_id = metadata
            return Chunk(
                data=SparseBuffer.from_db(cur, entry_id, *offsets).get_range(
                    start, end
                ),
                total=total,
            )

    @contextmanager
    def insertion(self, path: Path) -> Iterator[SqliteInsertion]:
        with self.conn as conn:
            cur = conn.cursor()

            insertion = SqliteInsertion(cur, path)
            yield insertion

            if insertion.final:
                cur.execute(
                    "UPDATE body SET is_final=true WHERE entry_id=?",
                    (insertion.entry_id,),
                )
                cur.execute(
                    "UPDATE head SET is_final=true WHERE entry_id=?",
                    (insertion.entry_id,),
                )
            else:
                cur.execute("DELETE from head WHERE entry_id=?", (insertion.entry_id,))
                cur.execute("DELETE from body WHERE entry_id=?", (insertion.entry_id,))
