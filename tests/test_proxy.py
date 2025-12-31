import asyncio
import functools
import sqlite3
import ssl
from dataclasses import dataclass
from typing import Iterator, NamedTuple

import httpx
import pytest
import pytest_mock
import trustme
from pytest_cases import fixture, parametrize, parametrize_with_cases
from pytest_httpserver import HTTPServer

from mopidy_tidal.gstreamer_proxy.cache import (
    Cache,
    DictCache,
    Head,
    Insertion,
    Path,
    SQLiteCache,
)
from mopidy_tidal.gstreamer_proxy.proxy import Proxy as ProxyInstance
from mopidy_tidal.gstreamer_proxy.proxy import ProxyConfig, ThreadedProxy
from mopidy_tidal.gstreamer_proxy.types import Buffer, FullRange, Range


@dataclass
class Remote:
    server: HTTPServer

    def add_resource(self, path: str, data: bytes, **kwargs):
        self.server.expect_request(path).respond_with_data(data, **kwargs)

    def add_ordered_resources(self, *resources: tuple[str, bytes], **kwargs):
        for path, data in resources:
            self.server.expect_ordered_request(path, **kwargs).respond_with_data(data)


@dataclass
class Proxy:
    port: int
    proxy: ThreadedProxy

    def url_for(self, path: str) -> str:
        return self.proxy.config.local_url(f"http://localhost/{path}")


class SSLContexts(NamedTuple):
    server: ssl.SSLContext
    client: ssl.SSLContext


@functools.cache
def ssl_contexts() -> SSLContexts:
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    client = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    ca = trustme.CA()
    cert = ca.issue_cert("localhost")
    cert.configure_cert(server)
    ca.configure_trust(client)

    return SSLContexts(server, client)


@fixture(scope="session")
def ssl_httpserver() -> Iterator[HTTPServer]:
    server, _ = ssl_contexts()
    server = HTTPServer(ssl_context=server)
    server.start()

    yield server

    server.clear()
    if server.is_running():
        server.stop()


class SSLCases:
    def case_http(self, httpserver: HTTPServer) -> HTTPServer:
        return httpserver

    def case_https(
        self, ssl_httpserver: HTTPServer, mocker: pytest_mock.MockFixture
    ) -> HTTPServer:
        _, client = ssl_contexts()
        mocker.patch(
            "mopidy_tidal.gstreamer_proxy.proxy.ssl_context", return_value=client
        )

        ssl_httpserver.clear()
        return ssl_httpserver


@fixture
@parametrize_with_cases("server", cases=SSLCases)
def remote(server: HTTPServer) -> Remote:
    return Remote(server)


@fixture
def proxy(remote: Remote) -> Iterator[Proxy]:
    proxy = ProxyInstance(
        ProxyConfig.build(None, remote.server.url_for("/")),
        # DictCache,
        lambda: SQLiteCache(sqlite3.connect(":memory:")),
    )
    instance = ThreadedProxy(proxy)
    yield Proxy(instance.config.port, instance)
    instance.stop()


class TestCacheMiss:
    def test_simple_retrieval(self, remote: Remote, proxy: Proxy):
        remote.add_resource("/foo", b"12345")

        resp = httpx.get(proxy.url_for("/foo"))
        data = resp.read()

        assert resp.status_code == 200
        assert data == b"12345"

    def test_offset_respected(self, remote: Remote, proxy: Proxy):
        remote.add_resource("/foo", b"23", status=206)

        resp = httpx.get(proxy.url_for("/foo"), headers={"Range": "bytes=2-4"})
        data = resp.read()

        assert data == b"23"
        assert resp.status_code == 206


class TestCacheHit:
    def test_simple_retrieval_comes_from_cache(self, remote: Remote, proxy: Proxy):
        remote.add_ordered_resources(
            ("/foo", b"12345"),
            ("/foo", b"67890"),
        )

        resp = httpx.get(proxy.url_for("/foo"))
        data = resp.read()

        assert resp.status_code == 200
        assert data == b"12345"

        resp = httpx.get(proxy.url_for("/foo"))
        data = resp.read()

        assert resp.status_code == 200
        assert data == b"12345"

    def test_offset_data_still_comes_from_cache(self, remote: Remote, proxy: Proxy):
        remote.add_ordered_resources(
            ("/foo", b"12345"),
            ("/foo", b"67890"),
        )

        resp = httpx.get(proxy.url_for("/foo"))
        data = resp.read()

        assert resp.status_code == 200
        assert data == b"12345"

        resp = httpx.get(proxy.url_for("/foo"), headers={"Range": "bytes=2-4"})
        data = resp.read()

        assert resp.status_code == 206
        assert data == b"345"


class TestRangeParsed:
    def test_from_simple_header(self):
        assert Range.parse_header(b"Range: bytes=1-10") == Range(1, 10)

    def test_from_open_end(self):
        assert Range.parse_header(b"Range: bytes=1-") == Range(1, None)

    def test_from_open_start(self):
        assert Range.parse_header(b"Range: bytes=-10") == Range(None, 10)

    def test_raises_if_units_not_bytes(self):
        with pytest.raises(Exception):
            Range.parse_header(b"Range: kilobytes=-10")

    def test_raises_if_completely_invalid(self):
        with pytest.raises(Exception):
            Range.parse_header(b"Hi: There")

    def test_raises_if_multiple_ranges_supplied(self):
        with pytest.raises(Exception):
            Range.parse_header(b"Range: bytes=1-2,10-")


class TestRangeSerialised:
    def test_to_header_when_simple(self):
        assert Range(1, 10).to_header() == "Range: bytes=1-10"

    def test_to_header_when_open_ended(self):
        assert Range(1, None).to_header() == "Range: bytes=1-"

    def test_to_header_when_open_start(self):
        assert Range(None, 10).to_header() == "Range: bytes=-10"

    def test_to_none_when_neither_end_present(self):
        assert Range(None, None).to_header() is None


class TestFullRange:
    def test_hydrated_from_half_empty_range(self):
        assert Range(1, None).expand(20) == FullRange(1, 20, 20)

    def test_hydrated_from_range(self):
        assert Range(1, 10).expand(20) == FullRange(1, 10, 20)

    def test_serialised_to_response_header(self):
        assert FullRange(1, 10, 20).to_header() == "Content-Range: bytes 1-10/20"


class TestBuffer:
    def test_extends(self):
        buffer = Buffer.with_capacity(10)

        buffer.extend(b"123")

        assert buffer.capacity == 10
        assert buffer.contains == 3
        assert buffer.data() == b"123"

    def test_extends_past_end(self):
        buffer = Buffer.with_capacity(4)

        buffer.extend(b"\x12\x34")
        buffer.extend(b"\x22\x34")
        buffer.extend(b"\x32\x34")

        assert buffer.data() == bytearray(b"\x12\x34\x22\x34\x32\x34")
        assert buffer.contains == 6
        assert buffer.capacity == 6

    def test_clears_and_can_be_reused(self):
        buffer = Buffer.with_capacity(10)

        buffer.extend(b"12345")
        buffer.extend(b"22345")

        assert buffer.contains == 10
        assert buffer.data() == bytearray(b"1234522345")

        buffer.clear()
        buffer.extend(b"67890")

        assert buffer.contains == 5
        assert buffer.data() == b"67890"


class CacheCases:
    def case_dict(self) -> DictCache:
        cache = DictCache()
        cache.init()
        return cache

    def case_sqlite(self) -> SQLiteCache:
        db = sqlite3.connect(":memory:", check_same_thread=False)
        cache = SQLiteCache(db)
        cache.init()
        return cache


@parametrize_with_cases("cache", cases=CacheCases)
class TestCache:
    def test_an_empty_cache_has_no_heads(self, cache: Cache):
        assert not cache.get_head(Path(b"foo"))

    def test_an_empty_cache_has_no_bodies(self, cache: Cache):
        assert not cache.get_body(Path(b"foo")).total

    def test_a_finalised_record_can_be_retrieved(self, cache: Cache[Insertion]):
        with cache.insertion(Path(b"foo")) as insertion:
            insertion.save_head(Head(b"head"))
            insertion.save_body_chunk(b"body", 0)
            insertion.save_body_chunk(b"data", 4)
            insertion.finalise()

        assert cache.get_head(Path(b"foo")) == b"head"

        body = b"".join(cache.get_body(Path(b"foo")).data)
        assert body == b"bodydata"

    def test_an_unfinalised_record_cannot_be_retrieved(self, cache: Cache[Insertion]):
        with cache.insertion(Path(b"foo")) as insertion:
            insertion.save_head(Head(b"head"))
            insertion.save_body_chunk(b"body", 0)
            insertion.save_body_chunk(b"data", 4)

        assert not cache.get_head(Path(b"foo"))
        assert not cache.get_body(Path(b"foo")).total

    @parametrize("n_concurrent", [2, 8, 24, 64, 128])
    async def test_the_same_path_can_be_inserted_concurrently(
        self, cache: Cache[Insertion], n_concurrent: int
    ):
        """This is not particularly useful, but it's better to insert the path
        than to e.g. corrupt the data. So long as we return a complete record,
        the cache will still work.

        Note the pool ensures this can't happen in normal operation *when
        driven by mopidy*. But we don't reject connections from other sockets,
        so someone might trigger a parallel insertion by e.g. clicking on the
        translated link they see in the debug logs.

        This test is the full spec for concurrent insertions: none will raise,
        and at least one will succeed, and you will only ever get one record
        back.
        """

        semaphore = asyncio.Semaphore(n_concurrent)

        async def insert():
            async with semaphore:
                with cache.insertion(Path(b"foo")) as insertion:
                    insertion.save_head(Head(b"head"))
                    await asyncio.sleep(0)
                    insertion.save_body_chunk(b"body", 0)
                    await asyncio.sleep(0)
                    insertion.save_body_chunk(b"data", 4)
                    await asyncio.sleep(0)
                    insertion.finalise()
                    await asyncio.sleep(0)

        await asyncio.gather(*(insert() for _ in range(128)))

        assert cache.get_head(Path(b"foo")) == b"head"
        body = b"".join(cache.get_body(Path(b"foo")).data)
        assert body == b"bodydata"

        body = b"".join(cache.get_body(Path(b"foo")).data)
        assert body == b"bodydata"

    @parametrize("n_concurrent", [2, 8, 24, 64, 128])
    async def test_different_paths_can_be_inserted_concurrently(
        self, cache: Cache[Insertion], n_concurrent: int
    ):
        semaphore = asyncio.Semaphore(n_concurrent)

        async def insert(id: int):
            async with semaphore:
                with cache.insertion(Path(f"foo-{id}".encode())) as insertion:
                    await asyncio.sleep(0)
                    insertion.save_head(Head(f"head-{id}".encode()))
                    await asyncio.sleep(0)
                    insertion.save_body_chunk(b"body", 0)
                    await asyncio.sleep(0)
                    insertion.save_body_chunk(b"data", 4)
                    await asyncio.sleep(0)
                    insertion.save_body_chunk(id.to_bytes(8), 8)
                    await asyncio.sleep(0)
                    insertion.finalise()
                    await asyncio.sleep(0)

        await asyncio.gather(*(insert(i) for i in range(128)))

        for id in range(128):
            path = Path(f"foo-{id}".encode())

            assert cache.get_head(path) == f"head-{id}".encode()

            body = b"".join(cache.get_body(path).data)
            assert body[:8] == b"bodydata"
            assert int.from_bytes(body[8:]) == id
