import sqlite3
from dataclasses import dataclass
from typing import Iterator

import httpx
from pytest_cases import fixture
from pytest_httpserver import HTTPServer

from mopidy_tidal.gstreamer_proxy.cache import SQLiteCache
from mopidy_tidal.gstreamer_proxy.proxy import Proxy as ProxyInstance
from mopidy_tidal.gstreamer_proxy.proxy import ProxyConfig, ThreadedProxy


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
    proxy: ProxyInstance

    def url_for(self, path: str) -> str:
        return f"http://localhost:{self.port}/{path}"


@fixture
def remote(httpserver: HTTPServer) -> Remote:
    return Remote(httpserver)


@fixture
def proxy(remote: Remote) -> Iterator[Proxy]:
    proxy = ProxyInstance(
        ProxyConfig(None, remote.server.url_for("/")),
        # DictCache,
        lambda: SQLiteCache(sqlite3.connect(":memory:")),
    )
    instance = ThreadedProxy(proxy)
    instance.wait_for_start()
    yield Proxy(instance.inner.port(), instance.inner)
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
