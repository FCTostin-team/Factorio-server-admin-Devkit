"""Unit tests for the RCON client using an injected transport pair."""
from __future__ import annotations

import asyncio
import struct

import pytest

from factorio_admin.core.rcon_client import (
    RconAuthError,
    RconClient,
    RconTimeoutError,
    SERVERDATA_AUTH,
    SERVERDATA_AUTH_RESPONSE,
    SERVERDATA_EXECCOMMAND,
    SERVERDATA_RESPONSE_VALUE,
)

_HEADER = "<iii"


def _encode_packet(pid: int, ptype: int, body: str) -> bytes:
    payload = body.encode("utf-8") + b"\x00\x00"
    size = 8 + len(payload)
    return struct.pack(_HEADER, size, pid, ptype) + payload


def _decode_packet(buffer: bytes) -> tuple[int, int, int, str]:
    size, pid, ptype = struct.unpack(_HEADER, buffer[:12])
    body = buffer[12 : 12 + size - 8 - 2].decode("utf-8")
    return size, pid, ptype, body


class FakeTransport:
    """Lightweight stand-in for a real TCP transport.

    The fake feeds canned server responses into an :class:`asyncio.StreamReader`
    and records bytes written through a paired :class:`asyncio.StreamWriter`
    so tests can assert on the request side.
    """

    def __init__(self) -> None:
        self.reader = asyncio.StreamReader()
        self.written = bytearray()
        self._writer = _Writer(self.written)

    def feed(self, data: bytes) -> None:
        self.reader.feed_data(data)

    def eof(self) -> None:
        self.reader.feed_eof()

    def as_pair(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return self.reader, self._writer  # type: ignore[return-value]


class _Writer:
    def __init__(self, sink: bytearray) -> None:
        self._sink = sink
        self._closing = False

    def write(self, data: bytes) -> None:
        self._sink.extend(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self._closing = True

    async def wait_closed(self) -> None:
        return None

    def is_closing(self) -> bool:
        return self._closing


@pytest.mark.asyncio
async def test_successful_auth_and_command_execution() -> None:
    transport = FakeTransport()

    async def factory():
        return transport.as_pair()

    client = RconClient(
        "h", 1, "pw", command_timeout=1.0, transport_factory=factory
    )
    transport.feed(_encode_packet(1, SERVERDATA_AUTH_RESPONSE, ""))
    transport.feed(_encode_packet(2, SERVERDATA_RESPONSE_VALUE, "pong"))

    run_task = asyncio.create_task(client.run())
    try:
        response = await client.execute("/ping")
    finally:
        await client.close()
        await asyncio.gather(run_task, return_exceptions=True)

    assert response == "pong"
    _, _, ptype, body = _decode_packet(bytes(transport.written[:24]))
    assert ptype == SERVERDATA_AUTH
    assert body == "pw"
    _, _, ptype, body = _decode_packet(bytes(transport.written[24:]))
    assert ptype == SERVERDATA_EXECCOMMAND
    assert body == "/ping"


@pytest.mark.asyncio
async def test_auth_failure_raises_and_terminates_run() -> None:
    transport = FakeTransport()

    async def factory():
        return transport.as_pair()

    client = RconClient("h", 1, "wrong", transport_factory=factory)
    transport.feed(_encode_packet(-1, SERVERDATA_AUTH_RESPONSE, ""))

    run_task = asyncio.create_task(client.run())
    await asyncio.sleep(0.05)
    with pytest.raises(RconAuthError):
        await client.execute("anything")
    await asyncio.gather(run_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_command_timeout() -> None:
    transport = FakeTransport()

    async def factory():
        return transport.as_pair()

    client = RconClient(
        "h", 1, "pw", command_timeout=0.1, transport_factory=factory
    )
    transport.feed(_encode_packet(1, SERVERDATA_AUTH_RESPONSE, ""))

    run_task = asyncio.create_task(client.run())
    try:
        with pytest.raises(RconTimeoutError):
            await client.execute("/slow")
    finally:
        await client.close()
        await asyncio.gather(run_task, return_exceptions=True)
