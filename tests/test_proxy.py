import functools
import sqlite3
import ssl
from dataclasses import dataclass
from typing import Iterator, NamedTuple

import httpx
import pytest_mock
import trustme
from pytest_cases import fixture, parametrize_with_cases
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
        # NOTE this is always http: there's no point serving over https on localhost.
        return f"http://localhost:{self.port}/{path}"


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
        ProxyConfig(None, remote.server.url_for("/")),
        # DictCache,
        lambda: SQLiteCache(sqlite3.connect(":memory:")),
    )
    instance = ThreadedProxy(proxy)
    yield Proxy(instance.config.port, instance.inner)
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
