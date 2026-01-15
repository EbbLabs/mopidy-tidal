import os
import shutil

import pytest

from mopidy_tidal.lru_cache import LruCache


@pytest.fixture
def lru_disk_cache() -> LruCache:
    return LruCache(max_size=8, persist=True, directory="cache")


@pytest.fixture
def lru_ram_cache() -> LruCache:
    return LruCache(max_size=8, persist=False)


@pytest.fixture(params=[True, False])
def lru_cache(request) -> LruCache:
    return LruCache(max_size=8, persist=request.param, directory="cache")


def test_config_stored_on_cache():
    cache = LruCache(max_size=1678, persist=True, directory="cache")

    assert cache.max_size == 1678
    assert cache.persist


class TestDiskPersistence:
    def test_raises_keyerror_if_file_corrupted(self):
        cache = LruCache(max_size=8, persist=True, directory="cache")
        cache.update({"tidal:uri:val": "hi", "tidal:uri:otherval": 17})
        cache.cache_file("tidal:uri:val").write_text("hahaha")
        del cache

        new_cache = LruCache(max_size=8, persist=True, directory="cache")
        assert new_cache["tidal:uri:otherval"] == 17
        with pytest.raises(KeyError):
            new_cache["tidal:uri:val"]

    def test_raises_keyerror_if_file_deleted(self):
        cache = LruCache(max_size=8, persist=True, directory="cache")
        cache.update({"tidal:uri:val": "hi", "tidal:uri:otherval": 17})
        cache.cache_file("tidal:uri:val").unlink()
        del cache

        new_cache = LruCache(max_size=8, persist=True, directory="cache")
        assert new_cache["tidal:uri:otherval"] == 17
        with pytest.raises(KeyError):
            new_cache["tidal:uri:val"]

    def test_prune_removes_files(self):
        cache = LruCache(max_size=8, persist=True, directory="cache")
        cache.update({"tidal:uri:val": "hi", "tidal:uri:otherval": 17})
        assert cache.cache_file("tidal:uri:otherval").exists()
        assert cache.cache_file("tidal:uri:val").exists()

        cache.prune("tidal:uri:otherval")
        cache.prune("tidal:uri:val")

        assert not cache.cache_file("tidal:uri:otherval").exists()
        assert not cache.cache_file("tidal:uri:val").exists()

    def test_prune_ignores_already_deleted_files(self):
        cache = LruCache(max_size=8, persist=True, directory="cache")
        cache.update({"tidal:uri:val": "hi", "tidal:uri:otherval": 17})
        cache.cache_file("tidal:uri:val").unlink()
        del cache

        new_cache = LruCache(max_size=8, persist=True, directory="cache")
        new_cache.prune("tidal:uri:otherval")
        new_cache.prune("tidal:uri:val")

    def test_migrates_old_filename_if_present(self, lru_disk_cache):
        uri = "tidal:uri:val"
        value = "hi"
        lru_disk_cache[uri] = value
        assert lru_disk_cache[uri] == value

        # The cache filename should be dash-separated
        filename = lru_disk_cache.cache_file(uri)
        assert filename.name == "-".join(uri.split(":")) + ".cache"

        # Rename the cache filename to match the old file format
        new_filename = os.path.join(os.path.dirname(filename), f"{uri}.cache")
        shutil.move(filename, new_filename)

        # Remove the in-memory cache element in order to force a filesystem reload
        lru_disk_cache.pop(uri)
        cached_value = lru_disk_cache.get(uri)
        assert cached_value == value

        # The cache filename should be column-separated
        filename = lru_disk_cache.cache_file(uri)
        assert filename.name == f"{uri}.cache"

    def test_values_persisted_between_caches(self):
        cache = LruCache(max_size=8, persist=True, directory="cache")
        cache.update(
            {"tidal:uri:val": "hi", "tidal:uri:otherval": 17, "tidal:uri:none": None}
        )
        del cache

        new_cache = LruCache(max_size=8, persist=True, directory="cache")

        assert new_cache["tidal:uri:val"] == "hi"
        assert new_cache["tidal:uri:otherval"] == 17
        assert new_cache["tidal:uri:none"] is None


def test_raises_key_error_if_target_missing(lru_cache):
    with pytest.raises(KeyError):
        lru_cache["tidal:uri:nonsuch"]


def test_simple_objects_persisted_in_cache(lru_cache):
    lru_cache["tidal:uri:val"] = "hi"
    lru_cache["tidal:uri:none"] = None

    assert lru_cache["tidal:uri:val"] == "hi" == lru_cache.get("tidal:uri:val")
    assert lru_cache["tidal:uri:none"] is None
    assert len(lru_cache) == 2


def test_complex_objects_persisted_in_cache(lru_cache):
    lru_cache["tidal:uri:otherval"] = {"complex": "object", "with": [0, 1]}

    assert (
        lru_cache["tidal:uri:otherval"]
        == {"complex": "object", "with": [0, 1]}
        == lru_cache.get("tidal:uri:otherval")
    )
    assert len(lru_cache) == 1


def test_update_adds_or_replaces(lru_cache):
    lru_cache.update({"tidal:uri:val": "hi", "tidal:uri:otherval": 17})

    assert lru_cache["tidal:uri:val"] == "hi"
    assert lru_cache["tidal:uri:otherval"] == 17
    assert "tidal:uri:val" in lru_cache
    assert "tidal:uri:nonesuch" not in lru_cache


def test_dict_style_update_behaves_like_update(lru_cache):
    lru_cache |= {"tidal:uri:val": "hi", "tidal:uri:otherval": 17}

    assert lru_cache["tidal:uri:val"] == "hi"
    assert lru_cache["tidal:uri:otherval"] == 17


def test_get_returns_default_if_supplied_and_no_match(lru_cache):
    uniq = object()

    assert lru_cache.get("tidal:uri:nonsuch", default=uniq) is uniq


def test_prune_removes_from_cache(lru_cache):
    lru_cache.update({"tidal:uri:val": "hi", "tidal:uri:otherval": 17})
    assert "tidal:uri:val" in lru_cache

    lru_cache.prune("tidal:uri:val")

    assert "tidal:uri:val" not in lru_cache
    assert "tidal:uri:otherval" in lru_cache


def test_prune_all_empties_cache(lru_cache):
    lru_cache.update({"tidal:uri:val": "hi", "tidal:uri:otherval": 17})
    assert len(lru_cache) == 2

    lru_cache.prune_all()

    assert len(lru_cache) == 0
    assert "tidal:uri:val" not in lru_cache
    assert "tidal:uri:otherval" not in lru_cache


def test_compares_equal_to_dict(lru_cache):
    data = {"tidal:uri:val": "hi", "tidal:uri:otherval": 17}
    lru_cache.update(data)

    assert lru_cache == data


@pytest.mark.parametrize("persist", (True, False))
def test_maintains_size_by_excluding_values(persist: bool):
    cache = LruCache(max_size=8, persist=persist)
    cache.update({f"tidal:uri:{val}": val for val in range(8)})
    assert len(cache) == 8

    cache["tidal:uri:8"] = 8

    assert len(cache) == 8


def test_excludes_least_recently_inserted_value_when_no_accesses_made():
    cache = LruCache(max_size=8, persist=False)

    cache.update({f"tidal:uri:{val}": val for val in range(9)})

    assert len(cache) == 8
    assert "tidal:uri:8" in cache
    assert "tidal:uri:0" not in cache


@pytest.mark.xfail(reason="Disk cache grows indefinitely")
def test_removes_least_recently_inserted_value_from_disk_when_cache_overflows():
    cache = LruCache(max_size=8, persist=True)

    cache.update({f"tidal:uri:{val}": val for val in range(9)})

    assert len(cache) == 8
    assert "tidal:uri:8" in cache
    assert "tidal:uri:0" not in cache


@pytest.mark.xfail(reason="Cache ignores usage")
def test_excludes_least_recently_accessed_value():
    cache = LruCache(max_size=8, persist=False)

    cache.update({f"tidal:uri:{val}": val for val in range(8)})
    cache.get("tidal:uri:0")
    cache["tidal:uri:8"] = 8

    assert len(cache) == 8
    assert "tidal:uri:8" in cache
    assert "tidal:uri:0" in cache
    assert "tidal:uri:1" not in cache


def test_cache_grows_indefinitely_if_max_size_zero():
    cache = LruCache(max_size=0, persist=False)

    cache.update({f"tidal:uri:{val}": val for val in range(2**12)})

    assert len(cache) == 2**12
