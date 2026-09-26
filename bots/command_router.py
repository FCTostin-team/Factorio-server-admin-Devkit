"""Shared command routing layer used by messenger integrations."""
from __future__ import annotations

import logging
import shlex
from dataclasses import dataclass
from datetime import timedelta
from typing import Awaitable, Callable, Optional

from core.rcon_client import RconClient, RconError
from storage.metrics_store import MetricsStore
from visuals.graph_engine import TimeSeries, render_timeseries

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class CommandResult:
    text: Optional[str] = None
    image_png: Optional[bytes] = None


CommandHandler = Callable[[list[str]], Awaitable[CommandResult]]


class CommandRouter:
    """Maps text commands to RCON actions and telemetry queries."""

    def __init__(self, rcon: RconClient, metrics: MetricsStore) -> None:
        self._rcon = rcon
        self._metrics = metrics
        self._handlers: dict[str, CommandHandler] = {
            "status": self._cmd_status,
            "players": self._cmd_players,
            "kick": self._cmd_kick,
            "ban": self._cmd_ban,
            "say": self._cmd_say,
            "graph": self._cmd_graph,
        }

    async def dispatch(self, raw: str) -> CommandResult:
        try:
            tokens = shlex.split(raw.strip())
        except ValueError as exc:
            return CommandResult(text=f"Parse error: {exc}")
        if not tokens:
            return CommandResult(text="Empty command.")
        name = tokens[0].lstrip("/").lower()
        handler = self._handlers.get(name)
        if handler is None:
            return CommandResult(text=f"Unknown command: {name}")
        try:
            return await handler(tokens[1:])
        except RconError as exc:
            logger.exception("RCON failure handling %s", name)
            return CommandResult(text=f"RCON error: {exc}")
        except ValueError as exc:
            return CommandResult(text=f"Invalid arguments: {exc}")

    async def _cmd_status(self, _: list[str]) -> CommandResult:
        return CommandResult(text=await self._rcon.execute("/players online count") or "(no response)")

    async def _cmd_players(self, _: list[str]) -> CommandResult:
        return CommandResult(text=await self._rcon.execute("/players online") or "(no players)")

    async def _cmd_kick(self, args: list[str]) -> CommandResult:
        if not args:
            raise ValueError("Usage: /kick <player> [reason]")
        target, *reason = args
        cmd = f"/kick {target} {' '.join(reason)}".strip()
        return CommandResult(text=await self._rcon.execute(cmd))

    async def _cmd_ban(self, args: list[str]) -> CommandResult:
        if not args:
            raise ValueError("Usage: /ban <player> [reason]")
        target, *reason = args
        cmd = f"/ban {target} {' '.join(reason)}".strip()
        return CommandResult(text=await self._rcon.execute(cmd))

    async def _cmd_say(self, args: list[str]) -> CommandResult:
        if not args:
            raise ValueError("Usage: /say <message>")
        message = " ".join(args).replace("\\", "\\\\").replace("'", "\\'")
        return CommandResult(text=await self._rcon.execute(f"/silent-command game.print('{message}')"))

    async def _cmd_graph(self, args: list[str]) -> CommandResult:
        metric = args[0] if args else "ups"
        try:
            hours = int(args[1]) if len(args) > 1 else 24
        except ValueError as exc:
            raise ValueError("Hours must be an integer") from exc
        points = self._metrics.window(metric, timedelta(hours=hours))
        if not points:
            return CommandResult(text=f"No data for metric '{metric}'.")
        timestamps, values = zip(*points)
        png = render_timeseries(
            [TimeSeries(name=metric, timestamps=list(timestamps), values=list(values))],
            title=f"{metric.upper()} over last {hours}h",
            ylabel=metric,
        )
        return CommandResult(image_png=png)
