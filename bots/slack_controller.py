"""Slack bot integration backed by the slack-bolt async API.

Provides the same command surface as the Telegram controller, shares the
same RCON client (commands are serialized through that client's queue),
and applies an independent sliding-window rate limit keyed by Slack user
ID.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
from slack_bolt.async_app import AsyncApp

from factorio_admin.bots.telegram_controller import SlidingWindowRateLimiter
from factorio_admin.core.rcon_client import (
    RconAuthError,
    RconClient,
    RconError,
    RconTimeoutError,
)
from factorio_admin.persistence.db import TelemetryStore
from factorio_admin.visuals.graph_engine import (
    render_production_chart,
    render_ups_chart,
)

logger = logging.getLogger(__name__)


class SlackController:
    """Slack slash-command integration."""

    def __init__(
        self,
        bot_token: str,
        app_token: str,
        rcon: RconClient,
        store: TelemetryStore,
        *,
        rate_limit_max: int = 5,
        rate_limit_window_seconds: float = 30.0,
        ups_warning_threshold: float = 50.0,
    ) -> None:
        """Configure the Slack integration."""
        self._bot_token = bot_token
        self._app_token = app_token
        self._rcon = rcon
        self._store = store
        self._rate = SlidingWindowRateLimiter(
            max_events=rate_limit_max,
            window_seconds=rate_limit_window_seconds,
        )
        self._ups_threshold = ups_warning_threshold
        self._app: AsyncApp | None = None
        self._handler: AsyncSocketModeHandler | None = None

    async def start(self) -> None:
        """Register handlers and open the Socket Mode connection."""
        self._app = AsyncApp(token=self._bot_token)
        self._app.command("/status")(self._cmd_status)
        self._app.command("/players")(self._cmd_players)
        self._app.command("/kick")(self._cmd_kick)
        self._app.command("/graph")(self._cmd_graph)
        self._handler = AsyncSocketModeHandler(self._app, self._app_token)
        await self._handler.start_async()
        logger.info("Slack controller started")

    async def stop(self) -> None:
        """Close the Socket Mode connection."""
        if self._handler is not None:
            await self._handler.close_async()
            self._handler = None

    async def _guard(self, body: dict[str, Any], say: Any) -> bool:
        user_id = body.get("user_id")
        if user_id is None:
            return False
        if not await self._rate.allow(user_id):
            await say("Rate limit exceeded. Please slow down.")
            return False
        return True

    async def _cmd_status(self, ack: Any, body: dict[str, Any], say: Any) -> None:
        await ack()
        if not await self._guard(body, say):
            return
        await self._reply_rcon(say, "/players online count")

    async def _cmd_players(self, ack: Any, body: dict[str, Any], say: Any) -> None:
        await ack()
        if not await self._guard(body, say):
            return
        await self._reply_rcon(say, "/players online")

    async def _cmd_kick(self, ack: Any, body: dict[str, Any], say: Any) -> None:
        await ack()
        if not await self._guard(body, say):
            return
        text = (body.get("text") or "").strip()
        if not text:
            await say("Usage: /kick <player> [reason]")
            return
        await self._reply_rcon(say, f"/kick {text}")

    async def _cmd_graph(
        self, ack: Any, body: dict[str, Any], say: Any, client: Any
    ) -> None:
        await ack()
        if not await self._guard(body, say):
            return
        args = (body.get("text") or "").split()
        metric = args[0].lower() if args else "ups"
        hours = 1
        if len(args) > 1:
            try:
                hours = max(1, int(args[1]))
            except ValueError:
                await say("Hours must be an integer.")
                return
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        try:
            if metric == "ups":
                samples = await self._store.query_ups(since)
                png = await render_ups_chart(
                    samples,
                    warning_threshold=self._ups_threshold,
                    title=f"UPS (last {hours}h)",
                )
            elif metric == "production":
                rows = await self._store.query_production(since)
                png = await render_production_chart(
                    rows, title=f"Production (last {hours}h)"
                )
            else:
                await say(f"Unknown metric '{metric}'. Try 'ups' or 'production'.")
                return
        except Exception:  # noqa: BLE001
            logger.exception("Graph rendering failed")
            await say("Graph rendering failed; please check logs.")
            return
        await client.files_upload_v2(
            channel=body.get("channel_id"),
            content=png,
            filename=f"{metric}.png",
            title=f"{metric} chart",
        )

    async def _reply_rcon(self, say: Any, command: str) -> None:
        try:
            response = await self._rcon.execute(command)
        except RconTimeoutError:
            await say("RCON command timed out. The server may be unresponsive.")
        except RconAuthError:
            await say("RCON authentication is failing; check the password.")
        except RconError as exc:
            logger.warning("RCON failure for %s: %s", command, exc)
            await say(f"RCON error: {exc}")
        else:
            await say(response or "(no response)")
