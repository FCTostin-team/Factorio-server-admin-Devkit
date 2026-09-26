"""Unit tests for the pure log-line parser."""
from __future__ import annotations

from datetime import datetime, timezone

from factorio_admin.parsers.log_parser import (
    PlayerEvent,
    ProductionEvent,
    UpsEvent,
    parse_line,
)

_FIXED_TS = datetime(2026, 5, 13, 12, 0, 0, tzinfo=timezone.utc)


def test_parse_ups_line() -> None:
    line = "2026-05-13 12:00:00 60.0 UPS, average 59.8 over 60s"
    event = parse_line(line, now=_FIXED_TS)
    assert isinstance(event, UpsEvent)
    assert event.value == 60.0
    assert event.timestamp == _FIXED_TS


def test_parse_player_join() -> None:
    event = parse_line("[JOIN] Alice joined the game", now=_FIXED_TS)
    assert isinstance(event, PlayerEvent)
    assert event.player_name == "Alice"
    assert event.event_type == "join"


def test_parse_player_leave() -> None:
    event = parse_line("[LEAVE] Bob left the game", now=_FIXED_TS)
    assert isinstance(event, PlayerEvent)
    assert event.player_name == "Bob"
    assert event.event_type == "leave"


def test_parse_production() -> None:
    event = parse_line(
        "item-produced iron-plate 12345 since session start", now=_FIXED_TS
    )
    assert isinstance(event, ProductionEvent)
    assert event.item == "iron-plate"
    assert event.count == 12345


def test_unmatched_line_returns_none() -> None:
    assert parse_line("nothing interesting here", now=_FIXED_TS) is None


def test_empty_line_returns_none() -> None:
    assert parse_line("", now=_FIXED_TS) is None
    assert parse_line("\r\n", now=_FIXED_TS) is None
