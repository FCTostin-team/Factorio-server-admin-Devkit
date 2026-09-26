"""Asynchronous Source RCON protocol client for Factorio dedicated servers.

The client maintains a single persistent TCP connection to the game server,
serializes outbound commands through an :class:`asyncio.Queue` so concurrent
callers never interleave packets, and reconnects with exponential backoff
on any transport failure. Authentication failures are non-retryable and
surface as :class:`RconAuthError`.

The transport is injectable: a ``transport_factory`` callable may be passed
in for unit testing, returning a tuple of ``(StreamReader, StreamWriter)``
without touching the network.
"""
from __future__ import annotations

import asyncio
import logging
import struct
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional, Tuple

logger = logging.getLogger(__name__)

SERVERDATA_AUTH = 3
SERVERDATA_AUTH_RESPONSE = 2
SERVERDATA_EXECCOMMAND = 2
SERVERDATA_RESPONSE_VALUE = 0

_HEADER_FORMAT = "<iii"
_HEADER_SIZE = struct.calcsize(_HEADER_FORMAT)
_MAX_PACKET_SIZE = 4096
_AUTH_FAILED_ID = -1

TransportFactory = Callable[[], Awaitable[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]]


class RconError(Exception):
    """Base class for all RCON-related failures."""


class RconAuthError(RconError):
    """Raised when the server reports authentication failure.

    This is a terminal error. The client task will exit rather than retry
    so that an operator can correct credentials before restart.
    """


class RconTimeoutError(RconError):
    """Raised when a command does not receive a response within its budget."""


class RconProtocolError(RconError):
    """Raised when a malformed or unexpected packet is received."""


@dataclass(slots=True)
class _Packet:
    """An on-the-wire RCON packet."""

    pid: int
    ptype: int
    body: str

    def encode(self) -> bytes:
        """Serialize the packet to its Source RCON byte representation."""
        body_bytes = self.body.encode("utf-8") + b"\x00\x00"
        size = 8 + len(body_bytes)
        return struct.pack(_HEADER_FORMAT, size, self.pid, self.ptype) + body_bytes


@dataclass(slots=True)
class _Request:
    """A queued command awaiting transmission."""

    command: str
    future: "asyncio.Future[str]" = field(repr=False)


class RconClient:
    """Persistent, serialized async RCON client.

    Example:
        >>> client = RconClient("127.0.0.1", 27015, "secret")
        >>> task = asyncio.create_task(client.run())
        >>> response = await client.execute("/players")
        >>> await client.close()
    """

    def __init__(
        self,
        host: str,
        port: int,
        password: str,
        *,
        command_timeout: float = 5.0,
        connect_timeout: float = 5.0,
        max_backoff_seconds: float = 60.0,
        transport_factory: Optional[TransportFactory] = None,
    ) -> None:
        """Configure the client.

        Args:
            host: RCON server hostname.
            port: RCON TCP port.
            password: Shared RCON password.
            command_timeout: Per-command response budget in seconds.
            connect_timeout: TCP connect + auth budget in seconds.
            max_backoff_seconds: Upper bound for exponential reconnect delay.
            transport_factory: Optional async callable returning
                ``(reader, writer)``. Defaults to ``asyncio.open_connection``.
        """
        self._host = host
        self._port = port
        self._password = password
        self._command_timeout = command_timeout
        self._connect_timeout = connect_timeout
        self._max_backoff = max_backoff_seconds
        self._transport_factory = transport_factory or self._default_transport

        self._queue: asyncio.Queue[_Request] = asyncio.Queue()
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._packet_id = 0
        self._closed = asyncio.Event()
        self._connected = asyncio.Event()

    async def execute(self, command: str) -> str:
        """Submit ``command`` for execution and return the server response.

        Args:
            command: The Factorio console command to run, with or without
                leading slash. Routed through the shared command queue.

        Returns:
            The server's textual response.

        Raises:
            RconTimeoutError: If the response did not arrive within the
                configured per-command timeout.
            RconAuthError: If the client task has terminated due to an
                authentication failure.
        """
        if self._closed.is_set():
            raise RconError("RCON client is closed")
        loop = asyncio.get_running_loop()
        future: "asyncio.Future[str]" = loop.create_future()
        await self._queue.put(_Request(command=command, future=future))
        try:
            return await asyncio.wait_for(future, self._command_timeout)
        except asyncio.TimeoutError as exc:
            raise RconTimeoutError(
                f"RCON command timed out after {self._command_timeout:.1f}s"
            ) from exc

    async def run(self) -> None:
        """Service loop: maintain connection and drain the command queue.

        Schedule this with :func:`asyncio.create_task`. The coroutine exits
        only on :meth:`close` or on a fatal :class:`RconAuthError`.
        """
        backoff = 1.0
        while not self._closed.is_set():
            try:
                await self._connect_and_authenticate()
            except RconAuthError as exc:
                logger.error("RCON authentication failed: %s", exc)
                self._fail_all_pending(exc)
                return
            except (OSError, asyncio.TimeoutError, RconProtocolError) as exc:
                logger.warning(
                    "RCON connect failed (%s); retrying in %.1fs", exc, backoff
                )
                await self._sleep_with_cancel(backoff)
                backoff = min(backoff * 2.0, self._max_backoff)
                continue

            backoff = 1.0
            try:
                await self._serve_loop()
            except (
                ConnectionResetError,
                asyncio.IncompleteReadError,
                OSError,
                EOFError,
                RconProtocolError,
            ) as exc:
                logger.warning("RCON transport lost: %s; reconnecting", exc)
                await self._close_transport()

    async def close(self) -> None:
        """Signal the service loop to exit and tear down the connection."""
        self._closed.set()
        await self._close_transport()
        self._fail_all_pending(RconError("RCON client closed"))

    async def _connect_and_authenticate(self) -> None:
        """Open a transport and complete the AUTH handshake."""
        async with asyncio.timeout(self._connect_timeout):
            self._reader, self._writer = await self._transport_factory()
            await self._authenticate()
        self._connected.set()
        logger.info("RCON connected to %s:%d", self._host, self._port)

    async def _default_transport(
        self,
    ) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_connection(self._host, self._port)

    async def _authenticate(self) -> None:
        """Perform the SERVERDATA_AUTH handshake.

        The server may emit a SERVERDATA_RESPONSE_VALUE packet before the
        SERVERDATA_AUTH_RESPONSE; we tolerate that ordering.
        """
        request_id = self._next_id()
        await self._write_packet(_Packet(request_id, SERVERDATA_AUTH, self._password))
        response = await self._read_packet()
        if response.ptype == SERVERDATA_RESPONSE_VALUE:
            response = await self._read_packet()
        if response.ptype != SERVERDATA_AUTH_RESPONSE:
            raise RconProtocolError(
                f"Unexpected auth response type: {response.ptype}"
            )
        if response.pid == _AUTH_FAILED_ID:
            raise RconAuthError("RCON server rejected password")

    async def _serve_loop(self) -> None:
        """Pop requests from the queue and dispatch them one at a time."""
        while not self._closed.is_set():
            request = await self._queue.get()
            if request.future.done():
                continue
            try:
                response = await self._dispatch(request.command)
            except RconProtocolError as exc:
                if not request.future.done():
                    request.future.set_exception(exc)
                logger.error("RCON protocol error: %s", exc)
                raise
            else:
                if not request.future.done():
                    request.future.set_result(response)

    async def _dispatch(self, command: str) -> str:
        """Send a single command and read its response."""
        await self._write_packet(
            _Packet(self._next_id(), SERVERDATA_EXECCOMMAND, command)
        )
        response = await self._read_packet()
        if response.ptype != SERVERDATA_RESPONSE_VALUE:
            raise RconProtocolError(
                f"Unexpected command response type: {response.ptype}"
            )
        return response.body

    async def _write_packet(self, packet: _Packet) -> None:
        if self._writer is None:
            raise RconProtocolError("No active RCON writer")
        self._writer.write(packet.encode())
        await self._writer.drain()

    async def _read_packet(self) -> _Packet:
        if self._reader is None:
            raise RconProtocolError("No active RCON reader")
        header = await self._reader.readexactly(_HEADER_SIZE)
        size, pid, ptype = struct.unpack(_HEADER_FORMAT, header)
        if size < 10 or size > _MAX_PACKET_SIZE:
            raise RconProtocolError(f"Invalid RCON packet size: {size}")
        payload = await self._reader.readexactly(size - 8)
        if not payload.endswith(b"\x00\x00"):
            raise RconProtocolError("RCON packet missing null terminator")
        return _Packet(pid, ptype, payload[:-2].decode("utf-8", errors="replace"))

    async def _close_transport(self) -> None:
        self._connected.clear()
        writer = self._writer
        self._reader = None
        self._writer = None
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except OSError:
                pass

    def _fail_all_pending(self, exc: BaseException) -> None:
        """Resolve every queued request with ``exc`` so callers unblock."""
        while not self._queue.empty():
            try:
                pending = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if not pending.future.done():
                pending.future.set_exception(exc)

    async def _sleep_with_cancel(self, seconds: float) -> None:
        """Sleep that exits early if :meth:`close` is called."""
        try:
            await asyncio.wait_for(self._closed.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            return

    def _next_id(self) -> int:
        self._packet_id = (self._packet_id + 1) % 0x7FFFFFFF
        return self._packet_id or 1
