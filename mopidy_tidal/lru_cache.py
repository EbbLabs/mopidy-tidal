from __future__ import unicode_literals

import logging
import pickle
from collections import OrderedDict
from pathlib import Path
from typing import Optional

from mopidy_tidal import Extension, context

logger = logging.getLogger(__name__)


def id_to_cachef(id: str) -> Path:
    return Path(id.replace(":", "-") + ".cache")


class LruCache(OrderedDict):
    def __init__(self, max_size: Optional[int] = 1024, persist=True, directory=""):
        """
        :param max_size: Max size of the cache in memory. Set 0 or None for no
            limit (default: 1024)
        :param persist: Whether the cache should be persisted to disk
            (default: True)
        :param directory: If `persist=True`, store the cached entries in this
            subfolder of the cache directory (default: '')
        """
        super().__init__(self)
        if max_size:
            assert max_size > 0, f"Invalid cache size: {max_size}"

        self._max_size = max_size or 0
        self._cache_dir = Path(Extension.get_cache_dir(context.get_config()), directory)
        self._persist = persist
        if persist:
            self._cache_dir.mkdir(parents=True, exist_ok=True)

        self._check_limit()

    @property
    def max_size(self):
        return self._max_size

    @property
    def persist(self):
        return self._persist

    def cache_file(self, key: str, cache_dir: Optional[Path] = None) -> Path:
        parts = key.split(":")
        assert len(parts) > 2, f"Invalid TIDAL ID: {key}"
        _, obj_type, id, *_ = parts
        if not cache_dir:
            cache_dir = Path(obj_type)

        cache_dir = self._cache_dir / cache_dir / id[:2]
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / id_to_cachef(key)
        legacy_cache_file = cache_dir / f"{key}.cache"
        if legacy_cache_file.is_file():
            return legacy_cache_file

        return cache_file

    def _get_from_storage(self, key):
        cache_file = self.cache_file(key)
        err = KeyError(key)
        if not cache_file.is_file():
            # Cache miss on the filesystem
            raise err

        # Cache hit on the filesystem
        with open(cache_file, "rb") as f:
            try:
                value = pickle.load(f)
            except Exception as e:
                # If the cache entry on the filesystem is corrupt, reset it
                logger.warning(
                    "Could not deserialize cache file %s: refreshing the entry: %s",
                    cache_file,
                    e,
                )
                self._reset_stored_entry(key)
                raise err

        # Store the filesystem item in memory
        if value is not None:
            self.__setitem__(key, value, _sync_to_fs=False)
        logger.debug(f"Filesystem cache hit for {key}")
        return value

    def __getitem__(self, key, *_, **__):
        try:
            # Cache hit in memory
            return super().__getitem__(key)
        except KeyError as e:
            if not self.persist:
                # No persisted storage -> cache miss
                raise e

        # Check on the persisted cache
        return self._get_from_storage(key)

    def __setitem__(self, key, value, _sync_to_fs=True, *_, **__):
        if super().__contains__(key):
            del self[key]

        super().__setitem__(key, value)
        if self.persist and _sync_to_fs:
            cache_file = self.cache_file(key)
            with open(cache_file, "wb") as f:
                pickle.dump(value, f)

        self._check_limit()

    def __contains__(self, key):
        return self.get(key) is not None

    def _reset_stored_entry(self, key):
        cache_file = self.cache_file(key)
        if cache_file.is_file():
            cache_file.unlink()

    def get(self, key, default=None, *args, **kwargs):
        try:
            return self.__getitem__(key, *args, **kwargs)
        except KeyError:
            return default

    def prune(self, *keys):
        """
        Delete the specified keys both from memory and disk.
        """
        for key in keys:
            logger.debug("Pruning key %r from cache %s", key, self.__class__.__name__)

            self._reset_stored_entry(key)
            self.pop(key, None)

    def prune_all(self):
        """
        Prune all the keys in the cache.
        """
        self.prune(*[*self.keys()])

    def update(self, *args, **kwargs):
        super().update(*args, **kwargs)
        self._check_limit()

    def _check_limit(self):
        if self.max_size:
            # delete oldest entries
            while len(self) > self.max_size:
                self.popitem(last=False)


class SearchCache(LruCache):
    def __init__(self, search_function):
        super().__init__(persist=False)
        self._search_function = search_function

    def __call__(self, *args, **kwargs):
        key = str(SearchKey(**kwargs))
        cached_result = self.get(key)
        logger.info(
            "Search cache miss" if cached_result is None else "Search cache hit"
        )
        if cached_result is None:
            cached_result = self._search_function(*args, **kwargs)
            self[key] = cached_result

        return cached_result


class SearchKey(object):
    def __init__(self, **kwargs):
        fixed_query = self.fix_query(kwargs["query"])
        self._query = tuple(sorted(fixed_query.items()))
        self._exact = kwargs["exact"]
        self._hash = None

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(self._exact)
            self._hash ^= hash(repr(self._query))

        return self._hash

    def __str__(self):
        return f"tidal:search:{self.__hash__()}"

    def __eq__(self, other):
        if not isinstance(other, SearchKey):
            return False

        return self._exact == other._exact and self._query == other._query

    @staticmethod
    def fix_query(query):
        """
        Removes some query parameters that otherwise will lead to a cache miss.
        Eg: 'track_no' since we can't query TIDAL for a specific album's track.
        :param query: query dictionary
        :return: sanitized query dictionary
        """
        query.pop("track_no", None)
        return query
