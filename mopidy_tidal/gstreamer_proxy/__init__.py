import functools
import sqlite3
from pathlib import Path

from .cache import SQLiteCache
from .proxy import Proxy, ProxyConfig, ThreadedProxy


@functools.cache
def mopidy_track_cache(path: Path, max_entries: int | None = None) -> ThreadedProxy:
    path.parent.mkdir(parents=True, exist_ok=True)
    proxy = Proxy(
        ProxyConfig.build(
            None,
            "https://lgf.audio.tidal.com/",
        ),
        lambda: SQLiteCache(sqlite3.connect(path), max_entries=max_entries),
    )
    instance = ThreadedProxy(proxy)

    return instance
