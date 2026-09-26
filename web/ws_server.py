"""Authenticated WebSocket broadcast server.

Streams every telemetry event published on the shared :class:`EventBus`
to all connected clients as newline-delimited JSON. Clients must present
a shared secret in the ``token`` query parameter on connect; absent or
wrong tokens are rejected with HTTP 403 before the WebSocket handshake
completes.
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
from datetime import datetime
from http import HTTPStatus
from typing import Awaitable, Callable
from urllib.parse import parse_qs, urlparse

from websockets.exceptions import ConnectionClosed
from websockets.server import WebSocketServerProtocol, serve

from factorio_admin.core.event_bus import EventBus
from factorio_admin.parsers.log_parser import ParsedEvent

logger = logging.getLogger(__name__)


def _serialize(event: ParsedEvent) -> str:
    """Convert a telemetry dataclass into a single-line JSON payload."""
    payload: dict[str, object] = {"kind": event.__class__.__name__}
    for field in dataclasses.fields(event):
        value = getattr(event, field.name)
        if isinstance(value, datetime):
            payload[field.name] = value.isoformat()
        else:
            payload[field.name] = value
    return json.dumps(payload, separators=(",", ":"))


class WebSocketBroadcaster:
    """Run a single WebSocket endpoint that fans out :class:`EventBus` traffic."""

    def __init__(
        self,
        bus: EventBus[ParsedEvent],
        *,
        host: str,
        port: int,
        secret_token: str,
    ) -> None:
        """Configure the broadcaster."""
        self._bus = bus
        self._host = host
        self._port = port
        self._token = secret_token
        self._clients: set[WebSocketServerProtocol] = set()
        self._lock = asyncio.Lock()
        self._stopped = asyncio.Event()

    async def serve_forever(self) -> None:
        """Run the WebSocket server until :meth:`stop` is called."""
        process_request = self._process_request
        async with serve(
            self._handle_client,
            self._host,
            self._port,
            process_request=process_request,
        ) as server:
            logger.info(
                "WebSocket server listening on ws://%s:%d", self._host, self._port
            )
            broadcast_task = asyncio.create_task(self._broadcast_loop())
            try:
                await self._stopped.wait()
            finally:
                broadcast_task.cancel()
                await asyncio.gather(broadcast_task, return_exceptions=True)
                await self._close_clients()
                server.close()
                await server.wait_closed()

    def stop(self) -> None:
        """Signal :meth:`serve_forever` to begin shutdown."""
        self._stopped.set()

    async def _process_request(
        self, path: str, _headers: object
    ) -> tuple[HTTPStatus, list[tuple[str, str]], bytes] | None:
        """Pre-handshake hook that enforces the shared-secret token."""
        query = parse_qs(urlparse(path).query)
        token = (query.get("token") or [""])[0]
        if token != self._token:
            logger.warning("Rejected WebSocket connection: bad token")
            return (
                HTTPStatus.FORBIDDEN,
                [("Content-Type", "text/plain")],
                b"Forbidden",
            )
        return None

    async def _handle_client(self, ws: WebSocketServerProtocol) -> None:
        """Track a connected client and keep its connection open."""
        async with self._lock:
            self._clients.add(ws)
        try:
            async for _ in ws:
                pass
        except ConnectionClosed:
            pass
        finally:
            async with self._lock:
                self._clients.discard(ws)

    async def _broadcast_loop(self) -> None:
        """Subscribe to the bus and forward each event to every client."""
        async for event in self._bus.subscribe():
            payload = _serialize(event) + "\n"
            async with self._lock:
                targets = list(self._clients)
            results = await asyncio.gather(
                *(self._safe_send(ws, payload) for ws in targets),
                return_exceptions=True,
            )
            for ws, result in zip(targets, results):
                if isinstance(result, Exception):
                    await self._drop_client(ws)

    async def _safe_send(
        self, ws: WebSocketServerProtocol, payload: str
    ) -> None:
        try:
            await ws.send(payload)
        except ConnectionClosed:
            raise

    async def _drop_client(self, ws: WebSocketServerProtocol) -> None:
        async with self._lock:
            self._clients.discard(ws)
        try:
            await ws.close()
        except OSError:
            pass

    async def _close_clients(self) -> None:
        async with self._lock:
            targets = list(self._clients)
            self._clients.clear()
        for ws in targets:
            try:
                await ws.close(code=1001, reason="server shutting down")
            except OSError:
                pass
