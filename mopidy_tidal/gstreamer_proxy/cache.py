import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Iterator
from contextlib import contextmanager, suppress
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
TidalID = NewType("TidalID", str)
EntryID = NewType("EntryID", bytes)


class Entry(NamedTuple):
    path: Path
    entry_id: EntryID


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
class ChunkedBuffer:
    """A lazily resolved buffer over discrete chunks of data."""

    offsets: Sequence[int]
    chunks: Callable[[int], Bytes]

    def get_range(self, start: int, end: int) -> Iterator[Bytes]:
        """Get a (half closed) range from the backing data."""
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
    def from_db(cls, cur: sqlite3.Cursor, entry_id: EntryID, *offsets: int) -> Self:
        def get_chunk(start: int) -> Bytes:
            cur.execute(
                "SELECT data FROM body WHERE entry_id=? AND start=?", (entry_id, start)
            )
            return cur.fetchone()[0]

        return cls(offsets, get_chunk)


def entry_id() -> EntryID:
    return EntryID(uuid4().bytes)


@dataclass
class SQLiteInsertion(Insertion):
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
class SQLiteCache(Cache[SQLiteInsertion]):
    conn: sqlite3.Connection
    max_entries: int | None = None

    def __post_init__(self) -> None:
        self.conn.execute("PRAGMA foreign_keys = ON")

    def init(self) -> None:
        with self.conn as conn:
            conn.execute("""
CREATE TABLE IF NOT EXISTS head
(
   id          INTEGER PRIMARY KEY AUTOINCREMENT
   , entry_id  BLOB    NOT NULL UNIQUE
   , is_final  INTEGER NOT NULL DEFAULT FALSE
   , manual    INTEGER NOT NULL DEFAULT FALSE
   , path      VARCHAR NOT NULL
   , data      BLOB    NOT NULL
   , timestamp INTEGER NOT NULL DEFAULT (unixepoch('now'))
   , last_used INTEGER NOT NULL DEFAULT (unixepoch('now'))
);
            """)
            conn.execute("""
CREATE TABLE IF NOT EXISTS body
(
   id          INTEGER PRIMARY KEY AUTOINCREMENT
   , entry_id  BLOB    NOT NULL
   , is_final  INTEGER NOT NULL DEFAULT FALSE
   , path      VARCHAR NOT NULL
   , start     INTEGER NOT NULL
   , data      BLOB    NOT NULL
   , len       INTEGER NOT NULL
   , timestamp INTEGER NOT NULL DEFAULT (unixepoch('now'))
   , FOREIGN KEY(entry_id) REFERENCES head(entry_id) ON DELETE CASCADE
);
            """)
            conn.execute("""
CREATE TABLE IF NOT EXISTS metadata
(
   id               INTEGER PRIMARY KEY AUTOINCREMENT
   , schema_version VARCHAR NOT NULL
   , extra          VARCHAR NOT NULL
);
            """)
            conn.execute("""
CREATE TABLE IF NOT EXISTS path
(
   id               INTEGER PRIMARY KEY AUTOINCREMENT
   , tidal_id       VARCHAR NOT NULL
   , path           VARCHAR NOT NULL
   , entry_id       BLOB                               -- Note nullable
   , FOREIGN KEY(entry_id) REFERENCES head(entry_id) ON DELETE CASCADE
);
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS head_path_idx ON head (path);")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS head_entry_idx ON head (entry_id);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS body_entry_idx ON body (entry_id);"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS path_id_idx ON path (tidal_id);")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS path_entry_idx ON path (entry_id);"
            )
            if not conn.execute("SELECT count(*) FROM metadata").fetchone()[0]:
                conn.execute(
                    "INSERT INTO metadata (schema_version, extra) VALUES (?, ?)",
                    ("v0.1.0", "{}"),
                )

            conn.execute("DELETE FROM head WHERE NOT is_final;")

    def evict(self) -> None:
        if max_entries := self.max_entries:
            with self.conn as conn:
                conn.execute(
                    """
DELETE FROM head
WHERE id NOT IN (
  SELECT id FROM head
  WHERE not manual
  ORDER BY last_used DESC, id DESC
  LIMIT ?
)
                """,
                    (max_entries,),
                )

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
                data=ChunkedBuffer.from_db(cur, entry_id, *offsets).get_range(
                    0, total - 1
                ),
                total=total,
            )

    def get_body_chunk(self, path: Path, start: int, end: int) -> Chunk | None:
        cur = self.conn.cursor()
        if metadata := _metadata(cur, path):
            total, offsets, entry_id = metadata
            return Chunk(
                data=ChunkedBuffer.from_db(cur, entry_id, *offsets).get_range(
                    start, end
                ),
                total=total,
            )

    @contextmanager
    def insertion(self, path: Path) -> Iterator[SQLiteInsertion]:
        with self.conn as conn:
            cur = conn.cursor()

            insertion = SQLiteInsertion(cur, path)
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
                with suppress(sqlite3.OperationalError):
                    cur.execute(
                        "UPDATE path SET entry_id=? WHERE path=?",
                        (insertion.entry_id, path),
                    )
                self.evict()
            else:
                cur.execute("DELETE from head WHERE entry_id=?", (insertion.entry_id,))
                cur.execute("DELETE from body WHERE entry_id=?", (insertion.entry_id,))

    def lookup_path(self, id: TidalID) -> Path | None:
        match self.conn.execute(
            "SELECT path FROM path WHERE tidal_id=? LIMIT 1", (id,)
        ).fetchone():
            case (path,):
                return Path(path)
            case _:
                return None

    def insert_path(self, id: TidalID, path: Path) -> None:
        with self.conn as conn:
            conn.execute("INSERT INTO path (tidal_id, path) VALUES (?, ?)", (id, path))

    def lookup_entry(self, id: TidalID) -> Entry | None:
        res = self.conn.execute(
            """
SELECT
  path.path
  , head.entry_id
FROM path
JOIN head on path.entry_id = head.entry_id
WHERE path.tidal_id = ? AND head.is_final
        """,
            (id,),
        ).fetchone()
        match res:
            case (path, entry_id):
                return Entry(path, entry_id)
            case _:
                return None
