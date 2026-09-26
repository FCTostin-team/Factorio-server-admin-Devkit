"""Asynchronous Factorio log tailer and pure-function event parsers.

The parsers are kept as module-level functions accepting a single line and
returning either a typed dataclass or ``None``, so they can be exercised by
unit tests without any I/O. The :class:`LogTailer` reads from the live log
file via :func:`asyncio.to_thread`, never polling in a tight loop and
detecting truncation/rotation by comparing file size to last position.
"""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator, Optional, Union

logger = logging.getLogger(__name__)

_UPS_RE = re.compile(r"(?P<value>\d+\.\d+)\s+UPS")
_JOIN_RE = re.compile(r"\[JOIN\]\s+(?P<player>\S+)")
_LEAVE_RE = re.compile(r"\[LEAVE\]\s+(?P<player>\S+)")
_PRODUCTION_RE = re.compile(
    r"item-produced\s+(?P<item>[\w\-]+)\s+(?P<count>\d+)"
)


@dataclass(slots=True, frozen=True)
class UpsEvent:
    """Server ticks-per-second sample."""

    timestamp: datetime
    value: float


@dataclass(slots=True, frozen=True)
class PlayerEvent:
    """A player join or leave event.

    ``event_type`` is one of ``"join"`` or ``"leave"``.
    """

    timestamp: datetime
    player_name: str
    event_type: str


@dataclass(slots=True, frozen=True)
class ProductionEvent:
    """Cumulative item-produced counter snapshot."""

    timestamp: datetime
    item: str
    count: int


ParsedEvent = Union[UpsEvent, PlayerEvent, ProductionEvent]


def parse_line(line: str, *, now: Optional[datetime] = None) -> Optional[ParsedEvent]:
    """Parse a single log line into a typed event.

    Args:
        line: A raw log line, with or without a trailing newline.
        now: Override timestamp for deterministic testing. Defaults to the
            current UTC time.

    Returns:
        A :class:`UpsEvent`, :class:`PlayerEvent`, :class:`ProductionEvent`,
        or ``None`` if no pattern matched.
    """
    line = line.rstrip("\r\n")
    if not line:
        return None
    timestamp = now if now is not None else datetime.now(timezone.utc)

    if match := _UPS_RE.search(line):
        return UpsEvent(timestamp=timestamp, value=float(match["value"]))
    if match := _JOIN_RE.search(line):
        return PlayerEvent(timestamp, match["player"], "join")
    if match := _LEAVE_RE.search(line):
        return PlayerEvent(timestamp, match["player"], "leave")
    if match := _PRODUCTION_RE.search(line):
        return ProductionEvent(
            timestamp=timestamp,
            item=match["item"],
            count=int(match["count"]),
        )
    return None


class LogTailer:
    """Async tailer that yields parsed events as the log file grows."""

    def __init__(
        self,
        path: Path,
        *,
        idle_sleep: float = 0.5,
        from_start: bool = False,
    ) -> None:
        """Configure the tailer.

        Args:
            path: Path to the Factorio log file (typically
                ``factorio-current.log``).
            idle_sleep: How long to sleep when no new bytes are present.
            from_start: If ``True``, read existing content first; otherwise
                seek to EOF on startup.
        """
        self._path = path
        self._idle_sleep = idle_sleep
        self._from_start = from_start
        self._stop = asyncio.Event()

    def stop(self) -> None:
        """Signal the tailer's :meth:`stream` loop to exit cleanly."""
        self._stop.set()

    async def stream(self) -> AsyncIterator[ParsedEvent]:
        """Yield parsed events forever (until :meth:`stop` is called).

        Truncation is detected by comparing the recorded read position to
        the current file size; if the file shrinks the tailer rewinds.
        """
        position = 0 if self._from_start else await asyncio.to_thread(
            self._eof_position
        )
        buffer = ""
        while not self._stop.is_set():
            try:
                chunk, position = await asyncio.to_thread(
                    self._read_chunk, position
                )
            except FileNotFoundError:
                await asyncio.sleep(self._idle_sleep)
                continue

            if not chunk:
                await asyncio.sleep(self._idle_sleep)
                continue

            buffer += chunk
            *complete_lines, buffer = buffer.split("\n")
            for line in complete_lines:
                event = parse_line(line)
                if event is not None:
                    yield event

    def _eof_position(self) -> int:
        try:
            return self._path.stat().st_size
        except FileNotFoundError:
            return 0

    def _read_chunk(self, position: int) -> tuple[str, int]:
        with self._path.open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            if size < position:
                logger.info("Log file truncated; rewinding to start")
                position = 0
            handle.seek(position)
            data = handle.read()
        return data.decode("utf-8", errors="replace"), position + len(data)
