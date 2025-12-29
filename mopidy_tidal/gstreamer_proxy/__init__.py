import sqlite3
from pathlib import Path

from .cache import SQLiteCache
from .proxy import Proxy, ProxyConfig, ThreadedProxy


def mopidy_track_cache(path: Path) -> ThreadedProxy:
    path.parent.mkdir(parents=True, exist_ok=True)
    proxy = Proxy(
        ProxyConfig(None, "https://listen.tidal.com/"),
        lambda: SQLiteCache(sqlite3.connect(path)),
    )
    instance = ThreadedProxy(proxy)

    return instance
