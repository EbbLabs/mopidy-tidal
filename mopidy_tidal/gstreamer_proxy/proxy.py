import asyncio
import multiprocessing
import threading
import urllib.parse
from contextlib import suppress
from dataclasses import dataclass, field
from logging import basicConfig, getLogger
from ssl import SSLContext
from typing import Callable, Self
from urllib.parse import urlparse, urlunparse

from . import types
from .cache import Cache, Head, Path

logger = getLogger(__name__)
basicConfig()


@dataclass
class ProxyConfig:
    port: int | None
    remote_url: str
    parsed: urllib.parse.ParseResult

    @classmethod
    def build(cls, port: int | None, remote_url: str) -> Self:
        return cls(port, remote_url, urlparse(remote_url))

    def remote_host(self) -> str:
        assert self.parsed.hostname
        return self.parsed.hostname

    def ssl(self) -> bool:
        return self.parsed.scheme == "https"

    def remote_port(self) -> int:
        return self.parsed.port or (443 if self.ssl() else 80)


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

    async def close(self):
        with suppress(ConnectionError):
            await self.tx.drain()
        self.tx.close()


@dataclass
class Connection:
    stream: Stream
    path: bytes


@dataclass
class Request:
    path: urllib.parse.ParseResultBytes
    keep_alive: bool
    range: types.Range
    raw_first: bytes
    raw_host: bytes
    raw_rest: bytearray

    def into_remote(self, remote_host: str, remote_port: int) -> Self:
        self.raw_host = f"Host: {remote_host}:{remote_port}\r\n".encode()
        return self

    def raw(self) -> bytes:
        return self.raw_first + self.raw_host + self.raw_rest

    def cache_key(self) -> bytes:
        return self.path.path


def ssl_context() -> SSLContext | None:
    """SSL context to use for https.

    This is overridden when testing so we can trust our local TLS cert. There is
    probably no reason to use it otherwise."""
    return None


@dataclass
class StatusError(Exception):
    """Invalid status"""

    status: int

    def __repr__(self) -> str:
        return f"StatusError({self.status})"


def check_status(status: bytes | bytearray) -> None:
    val = int(status.decode())
    if 200 <= val <= 300:
        return None
    else:
        raise StatusError(val)


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

    async def block_start(self, queue: multiprocessing.Queue) -> None:
        config = await self.start()
        queue.put(config)
        await self.block()

    async def start(self) -> StartedProxyConfig:
        self.cache = self.cache_factory()
        server = await asyncio.start_server(
            self.handle_request, "127.0.0.1", self.config.port or 0
        )
        _, port = server.sockets[0].getsockname()
        self.cache.init()
        self.started = True
        config = StartedProxyConfig(port, self.config.remote_url)
        logger.debug("Proxy config: %s", config)
        return config

    async def parse_request(self, local: Stream) -> Request:
        raw_rest = bytearray()
        raw_first = await local.rx.readline()
        verb, path_args, protocol = raw_first.split()
        path = urlparse(path_args)
        assert verb == b"GET"
        assert protocol == b"HTTP/1.1"
        keep_alive: bool = False
        range = types.Range(None, None)
        raw_host = None
        async for line in local.rx:
            if line.startswith(b"Host:"):
                raw_host = line
                continue
            raw_rest.extend(line)
            if b"keep-alive" in line:
                keep_alive = True
            elif b"Range:" in line:
                range = types.Range.parse_header(line)
            if line == b"\r\n":
                break
        assert raw_host, "No host: line in request"
        return Request(path, keep_alive, range, raw_first, raw_host, raw_rest)

    async def open_connection(self) -> Stream:
        return await Stream.connect(
            self.config.remote_host(),
            self.config.remote_port(),
            ssl=self.config.ssl() and (ssl_context() or True),
        )

    async def stream_head(self, local: Stream, remote: Stream) -> types.Head:
        head = bytearray()

        line = await remote.rx.readline()
        protocol, status, *_ = line.split()
        assert protocol.startswith(b"HTTP/1")  # test server uses http/1.0
        check_status(status)
        head.extend(line)

        await local.write(line)

        async for line in remote.rx:
            head.extend(line)
            await local.write(line)
            if line == b"\r\n":
                break

        return types.Head.from_raw(head)

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

        path = Path(request.cache_key())

        if head := self.cache.get_head(path):
            logger.debug("Serving %s from cache (%s)", request.path.path, request.range)
            if not (range := request.range).empty():
                parsed = types.Head.from_raw(head)
                assert parsed.content_length
                range = range.expand(parsed.content_length)
                start, end, _ = range

                chunks = self.cache.get_body_chunk(path, start, end)

                head_lines = head.splitlines()[1:-1]
                del head
                head_lines = [h for h in head_lines if b"Content-Length" not in h]
                head_lines.insert(0, b"HTTP/1.1 206 Partial Content")

                head_lines.append(f"Content-Length: {end + 1 - start}".encode())
                head_lines.append(range.to_header().encode())
                head_lines.append(b"")
                head_lines.append(b"")
                await local.write(b"\r\n".join(head_lines))
            else:
                await local.write(head)
                chunks = self.cache.get_body(path)

            for chunk in chunks.data:
                await local.write(chunk)

        else:
            logger.debug("Proxying %s from remote", request.path.path)
            with self.cache.insertion(path) as insertion:
                remote = await self.open_connection()
                raw = request.into_remote(
                    self.config.remote_host(), self.config.remote_port()
                ).raw()
                await remote.write(raw)

                head, content_length, keep_alive = await self.stream_head(local, remote)
                insertion.save_head(Head(bytes(head)))

                buffer_bytes = 1024 * 1024 * 2  # 2 MiB for now
                buffer = types.Buffer.with_capacity(buffer_bytes)
                offset = 0
                read = 0
                finished = False

                while not finished:
                    data = await remote.rx.read(buffer_bytes)
                    read += len(data)
                    print(read, content_length)
                    if not data:  # socket closed
                        break
                    buffer.extend(data)
                    del data

                    finished = read == content_length

                    if buffer.contains >= buffer_bytes or finished:
                        await local.write(buffer.data())
                        insertion.save_body_chunk(buffer.data(), offset)
                        offset += buffer.contains
                        buffer.clear()

                await remote.close()
                await local.close()

                # right now we just throw away partial requests. We *could* in
                # theory save them and try to assemble later, but that's
                # probably more hassle than it's worth.
                if read == content_length and request.range.empty():
                    logger.debug(
                        "Fetched whole record; finalising insertion %s", insertion
                    )
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


class ProcessProxy:
    def __init__(self, proxy: Proxy) -> None:
        from multiprocessing import Process

        self.inner = proxy
        loop = asyncio.new_event_loop()
        queue = multiprocessing.Queue(1)
        self.proc = Process(
            target=lambda: loop.run_until_complete(proxy.block_start(queue))
        )
        self.proc.start()
        self.config = queue.get()

    def stop(self, block: bool = True):
        self.proc.terminate()
        if block:
            self.proc.join()
