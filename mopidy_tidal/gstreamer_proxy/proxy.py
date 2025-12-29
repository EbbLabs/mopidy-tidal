import asyncio
import threading
import urllib.parse
from dataclasses import dataclass, field
from logging import basicConfig, getLogger
from ssl import SSLContext
from typing import Callable, Self
from urllib.parse import urlparse, urlunparse

from .cache import Cache, Head, Path

logger = getLogger()
basicConfig()


@dataclass
class ProxyConfig:
    port: int | None
    remote_url: str


@dataclass
class StartedProxyConfig:
    port: int
    remote_url: str

    def local_url(self, remote_url: str) -> str:
        parsed = urlparse(remote_url)._replace(
            scheme="http", netloc=f"localhost:{self.port}"
        )
        return urlunparse(parsed)


@dataclass
class Stream:
    rx: asyncio.StreamReader
    tx: asyncio.StreamWriter

    async def write(self, data: bytes | bytearray):
        self.tx.write(data)
        await self.tx.drain()

    @classmethod
    async def connect(cls, *args, **kwargs) -> Self:
        rx, tx = await asyncio.open_connection(*args, **kwargs)
        return cls(rx, tx)


@dataclass
class Connection:
    stream: Stream
    path: bytes


@dataclass
class Request:
    path: bytes
    keep_alive: bool
    range_start: int | None
    range_end: int | None
    raw: bytearray


def ssl_context() -> SSLContext | None:
    return None


type CacheFactory[C] = Callable[[], C]


@dataclass
class Proxy[C: Cache]:
    config: ProxyConfig
    cache_factory: CacheFactory[C]
    started: bool = False
    event: asyncio.Event = field(default_factory=asyncio.Event)

    async def block(self) -> None:
        await self.event.wait()
        logger.info("Proxy exiting...")

    async def start(self) -> StartedProxyConfig:
        self.cache = self.cache_factory()
        server = await asyncio.start_server(
            self.handle_request, "127.0.0.1", self.config.port or 0
        )
        _, port = server.sockets[0].getsockname()
        self.cache.init()
        self.started = True
        return StartedProxyConfig(port, self.config.remote_url)

    async def parse_request(self, local: Stream) -> Request:
        raw = bytearray()
        first = await local.rx.readline()
        verb, path, protocol = first.split()
        # Standardise for cache coverage
        if path.startswith(b"//"):
            path = path[1:]
        if not path:
            path = b"/"
        assert verb == b"GET"
        assert protocol == b"HTTP/1.1"
        raw.extend(first)
        keep_alive: bool = False
        start, end = None, None
        async for line in local.rx:
            raw.extend(line)
            if b"Connection: keep-alive" in line:
                keep_alive = True
            elif b"Range:" in line:
                unit, range = line[7:].split(b"=")
                assert unit == b"bytes"
                # TODO parse this properly and break out parsing.
                # bytes=-500
                # bytes=9500-
                # bytes=0-0,-1
                start, end = (int(x) for x in range.split(b"-"))
                assert start < end
            if line == b"\r\n":
                break
        return Request(path, keep_alive, start, end, raw)

    async def open_connection(self, path: bytes) -> Stream:
        remote_url = urllib.parse.urlsplit(self.config.remote_url + path.decode())
        ssl = remote_url.scheme == "https"
        remote_port = remote_url.port or (443 if ssl else 443)
        return await Stream.connect(
            remote_url.hostname, remote_port, ssl=ssl_context() or ssl
        )

    async def stream_head(self, local: Stream, remote: Stream) -> bytearray:
        head = bytearray()

        line = await remote.rx.readline()
        protocol, status, *_ = line.split()
        assert protocol.startswith(b"HTTP/1")  # test server uses http/1.0
        assert 200 <= int(status.decode()) < 300
        head.extend(line)

        await local.write(line)

        async for line in remote.rx:
            head.extend(line)
            await local.write(line)
            if line == b"\r\n":
                break

        return head

    async def error(self, local: Stream, code: int = 400, msg: bytes = b"") -> None:
        await local.write(f"HTTP/1.1 {code}\r\n\r\n".encode())
        if msg:
            await local.write(msg)

    async def handle_request(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        local = Stream(reader, writer)
        del reader, writer
        request = await self.parse_request(local)

        path = Path(request.path)

        if head := self.cache.get_head(path):
            logger.debug("Serving %s from cache", request.path)
            if request.range_start is not None:
                assert request.range_end is not None
                start = request.range_start
                end = request.range_end
                chunks = self.cache.get_body_chunk(path, start, end)

                if start < 0 or end > chunks.total:
                    return await self.error(local, 400)

                head_lines = head.splitlines()[1:-1]
                del head
                head_lines = [h for h in head_lines if b"Content-Length" not in h]
                head_lines.insert(0, b"HTTP/1.1 206 Partial Content")
                head_lines.append(f"Content-Length: {end + 1 - start}".encode())
                head_lines.append(
                    f"Content-Range: bytes {start}-{end}/{chunks.total}".encode()
                )
                head_lines.append(b"")
                head_lines.append(b"")
                await local.write(b"\r\n".join(head_lines))
            else:
                await local.write(head)
                chunks = self.cache.get_body(path)

            buffer_bytes = 1024 * 64
            for chunk in chunks.data:
                assert chunk
                start = 0
                end = max(len(chunk), len(chunk) - buffer_bytes)
                while start < end:
                    await local.write(chunk[start:end])
                    start += buffer_bytes

        else:
            logger.debug("Proxying %s from remote", request.path)
            with self.cache.insertion(path) as insertion:
                remote = await self.open_connection(request.path)
                await remote.write(request.raw)

                head = await self.stream_head(local, remote)
                insertion.save_head(Head(head))

                body = bytearray()

                offset = 0
                buffer_bytes = 1024 * 64
                while True:
                    data = await remote.rx.read(buffer_bytes)
                    if not data:  # socket closed
                        break
                    body.extend(data)
                    await local.write(data)
                    insertion.save_body_chunk(data, offset)
                    offset += len(data)
                    if len(data) < buffer_bytes:  # partial read
                        break

                # TODO minimal validation: parse the header enough to find the
                # length and check we got the right length
                insertion.finalise()


class ThreadedProxy:
    def __init__(self, proxy: Proxy) -> None:
        self.inner = proxy
        loop = asyncio.new_event_loop()
        self.thread = threading.Thread(
            target=lambda: loop.run_until_complete(proxy.block())
        )
        self.thread.start()
        self.config = asyncio.run_coroutine_threadsafe(proxy.start(), loop).result()
        self.loop = loop

    def stop(self, block: bool = True):
        self.loop.call_soon_threadsafe(lambda: self.inner.event.set())
        if block:
            self.thread.join()
