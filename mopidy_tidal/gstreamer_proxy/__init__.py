import functools
import sqlite3
from pathlib import Path

from .cache import SQLiteCache
from .proxy import Proxy, ProxyConfig, ThreadedProxy


@functools.cache
def mopidy_playback_cache(
    path: Path,
    buffer_bytes: int,
    max_entries: int | None,
) -> ThreadedProxy:
    path.parent.mkdir(parents=True, exist_ok=True)
    proxy = Proxy(
        ProxyConfig.build(
            None,
            "https://lgf.audio.tidal.com/",
        ),
        lambda: SQLiteCache(sqlite3.connect(path), max_entries=max_entries),
        buffer_bytes=buffer_bytes,
    )
    instance = ThreadedProxy(proxy)

    return instance
