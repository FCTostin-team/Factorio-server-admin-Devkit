"""SQLite-backed telemetry store with batched, time-driven writes.

Three normalized tables hold the three event types emitted by the log
parser. All writes flow through an in-memory buffer flushed every
``flush_interval`` seconds (default 5 s) to keep contention low under
high log volume. Reads accept a ``since`` cutoff for graph queries.
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Iterable, Sequence

import aiosqlite

from factorio_admin.parsers.log_parser import (
    PlayerEvent,
    ProductionEvent,
    UpsEvent,
    ParsedEvent,
)

logger = logging.getLogger(__name__)

_SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS ups_metrics (
        ts INTEGER NOT NULL,
        value REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS player_events (
        ts INTEGER NOT NULL,
        player TEXT NOT NULL,
        event TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS production_stats (
        ts INTEGER NOT NULL,
        item TEXT NOT NULL,
        count INTEGER NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_ups_ts ON ups_metrics(ts)",
    "CREATE INDEX IF NOT EXISTS idx_player_ts ON player_events(ts)",
    "CREATE INDEX IF NOT EXISTS idx_prod_ts_item ON production_stats(ts, item)",
)


def _epoch(ts: datetime) -> int:
    return int(ts.timestamp())


def _from_epoch(value: int) -> datetime:
    return datetime.fromtimestamp(value, tz=timezone.utc)


class TelemetryStore:
    """Async batched persistence for telemetry events."""

    def __init__(
        self,
        path: Path,
        *,
        flush_interval: float = 5.0,
    ) -> None:
        """Configure the store.

        Args:
            path: Filesystem path to the SQLite database file.
            flush_interval: Seconds between automatic buffer flushes.
        """
        self._path = path
        self._flush_interval = flush_interval
        self._conn: aiosqlite.Connection | None = None
        self._buffer: Deque[ParsedEvent] = deque()
        self._lock = asyncio.Lock()
        self._stopped = asyncio.Event()

    async def open(self) -> None:
        """Open the connection and apply schema migrations."""
        self._conn = await aiosqlite.connect(self._path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        for statement in _SCHEMA:
            await self._conn.execute(statement)
        await self._conn.commit()
        logger.info("Telemetry store ready at %s", self._path)

    async def close(self) -> None:
        """Flush any buffered events and close the database connection."""
        self._stopped.set()
        await self.flush()
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    def enqueue(self, event: ParsedEvent) -> None:
        """Buffer ``event`` for the next flush. Cheap, sync-safe."""
        self._buffer.append(event)

    async def flush_loop(self) -> None:
        """Periodically flush buffered events until :meth:`close` is called."""
        while not self._stopped.is_set():
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=self._flush_interval
                )
            except asyncio.TimeoutError:
                pass
            try:
                await self.flush()
            except aiosqlite.Error as exc:
                logger.error("Telemetry flush failed: %s", exc)

    async def flush(self) -> None:
        """Drain the in-memory buffer into SQLite in a single transaction."""
        if self._conn is None or not self._buffer:
            return
        async with self._lock:
            ups_rows: list[tuple[int, float]] = []
            player_rows: list[tuple[int, str, str]] = []
            prod_rows: list[tuple[int, str, int]] = []
            while self._buffer:
                event = self._buffer.popleft()
                if isinstance(event, UpsEvent):
                    ups_rows.append((_epoch(event.timestamp), event.value))
                elif isinstance(event, PlayerEvent):
                    player_rows.append(
                        (_epoch(event.timestamp), event.player_name, event.event_type)
                    )
                elif isinstance(event, ProductionEvent):
                    prod_rows.append(
                        (_epoch(event.timestamp), event.item, event.count)
                    )
            if ups_rows:
                await self._conn.executemany(
                    "INSERT INTO ups_metrics(ts, value) VALUES (?, ?)", ups_rows
                )
            if player_rows:
                await self._conn.executemany(
                    "INSERT INTO player_events(ts, player, event) VALUES (?, ?, ?)",
                    player_rows,
                )
            if prod_rows:
                await self._conn.executemany(
                    "INSERT INTO production_stats(ts, item, count) VALUES (?, ?, ?)",
                    prod_rows,
                )
            await self._conn.commit()

    async def query_ups(self, since: datetime) -> list[tuple[datetime, float]]:
        """Return UPS samples on or after ``since`` ordered ascending by time."""
        assert self._conn is not None
        cursor = await self._conn.execute(
            "SELECT ts, value FROM ups_metrics WHERE ts >= ? ORDER BY ts ASC",
            (_epoch(since),),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [(_from_epoch(row[0]), float(row[1])) for row in rows]

    async def query_production(
        self,
        since: datetime,
        *,
        items: Sequence[str] | None = None,
    ) -> list[tuple[datetime, str, int]]:
        """Return production rows; optionally filter by item allow-list."""
        assert self._conn is not None
        if items:
            placeholders = ",".join("?" for _ in items)
            sql = (
                f"SELECT ts, item, count FROM production_stats "
                f"WHERE ts >= ? AND item IN ({placeholders}) ORDER BY ts ASC"
            )
            params: tuple = (_epoch(since), *items)
        else:
            sql = (
                "SELECT ts, item, count FROM production_stats "
                "WHERE ts >= ? ORDER BY ts ASC"
            )
            params = (_epoch(since),)
        cursor = await self._conn.execute(sql, params)
        rows = await cursor.fetchall()
        await cursor.close()
        return [(_from_epoch(row[0]), row[1], int(row[2])) for row in rows]

    async def query_players(
        self, since: datetime
    ) -> list[tuple[datetime, str, str]]:
        """Return player join/leave rows on or after ``since``."""
        assert self._conn is not None
        cursor = await self._conn.execute(
            "SELECT ts, player, event FROM player_events "
            "WHERE ts >= ? ORDER BY ts ASC",
            (_epoch(since),),
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [(_from_epoch(row[0]), row[1], row[2]) for row in rows]

    async def ingest_stream(self, events: Iterable[ParsedEvent]) -> None:
        """Helper for tests: enqueue an iterable and flush immediately."""
        for event in events:
            self.enqueue(event)
        await self.flush()
