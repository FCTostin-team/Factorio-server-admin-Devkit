"""Telegram bot integration backed by python-telegram-bot v20+.

Implements the shared command surface (``/status``, ``/players``,
``/kick``, ``/graph``), an asyncio-safe sliding-window rate limiter,
and graceful error handling that converts RCON exceptions into chat
messages instead of crashes.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Deque

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

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


class SlidingWindowRateLimiter:
    """Per-key sliding window rate limiter, safe for concurrent coroutines."""

    def __init__(self, *, max_events: int, window_seconds: float) -> None:
        """Configure the limiter."""
        self._max = max_events
        self._window = window_seconds
        self._buckets: dict[object, Deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def allow(self, key: object) -> bool:
        """Return ``True`` if ``key`` may proceed; otherwise ``False``."""
        loop = asyncio.get_running_loop()
        now = loop.time()
        cutoff = now - self._window
        async with self._lock:
            bucket = self._buckets[key]
            while bucket and bucket[0] < cutoff:
                bucket.popleft()
            if len(bucket) >= self._max:
                return False
            bucket.append(now)
            return True


class TelegramController:
    """Wire python-telegram-bot handlers to the RCON and telemetry layers."""

    def __init__(
        self,
        token: str,
        rcon: RconClient,
        store: TelemetryStore,
        *,
        allowed_user_ids: set[int] | None = None,
        rate_limit_max: int = 5,
        rate_limit_window_seconds: float = 30.0,
        ups_warning_threshold: float = 50.0,
    ) -> None:
        """Configure the Telegram integration."""
        self._token = token
        self._rcon = rcon
        self._store = store
        self._allowed = allowed_user_ids or set()
        self._rate = SlidingWindowRateLimiter(
            max_events=rate_limit_max,
            window_seconds=rate_limit_window_seconds,
        )
        self._ups_threshold = ups_warning_threshold
        self._app: Application | None = None

    async def start(self) -> None:
        """Build the application, register handlers, and start polling."""
        self._app = ApplicationBuilder().token(self._token).build()
        self._app.add_handler(CommandHandler("status", self._cmd_status))
        self._app.add_handler(CommandHandler("players", self._cmd_players))
        self._app.add_handler(CommandHandler("kick", self._cmd_kick))
        self._app.add_handler(CommandHandler("graph", self._cmd_graph))
        await self._app.initialize()
        await self._app.start()
        await self._app.updater.start_polling()
        logger.info("Telegram controller started")

    async def stop(self) -> None:
        """Shut down the polling task and tear down the application."""
        if self._app is None:
            return
        await self._app.updater.stop()
        await self._app.stop()
        await self._app.shutdown()
        self._app = None

    async def _guard(self, update: Update) -> bool:
        """Apply allow-list and rate limiting; reply on rejection."""
        user = update.effective_user
        chat = update.effective_chat
        if user is None or chat is None:
            return False
        if self._allowed and user.id not in self._allowed:
            await chat.send_message("Unauthorized.")
            return False
        if not await self._rate.allow(user.id):
            await chat.send_message(
                "Rate limit exceeded. Please slow down."
            )
            return False
        return True

    async def _cmd_status(
        self, update: Update, _ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not await self._guard(update):
            return
        await self._reply_rcon(update, "/players online count")

    async def _cmd_players(
        self, update: Update, _ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not await self._guard(update):
            return
        await self._reply_rcon(update, "/players online")

    async def _cmd_kick(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not await self._guard(update):
            return
        if not ctx.args:
            await update.effective_chat.send_message("Usage: /kick <player>")
            return
        target = ctx.args[0]
        reason = " ".join(ctx.args[1:])
        command = f"/kick {target} {reason}".strip()
        await self._reply_rcon(update, command)

    async def _cmd_graph(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not await self._guard(update):
            return
        metric = (ctx.args[0] if ctx.args else "ups").lower()
        hours = 1
        if len(ctx.args) > 1:
            try:
                hours = max(1, int(ctx.args[1]))
            except ValueError:
                await update.effective_chat.send_message(
                    "Hours must be an integer."
                )
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
                await update.effective_chat.send_message(
                    f"Unknown metric '{metric}'. Try 'ups' or 'production'."
                )
                return
        except Exception:  # noqa: BLE001 - rendering surfaces operator errors
            logger.exception("Graph rendering failed")
            await update.effective_chat.send_message(
                "Graph rendering failed; please check logs."
            )
            return
        await update.effective_chat.send_document(
            document=png, filename=f"{metric}.png"
        )

    async def _reply_rcon(self, update: Update, command: str) -> None:
        try:
            response = await self._rcon.execute(command)
        except RconTimeoutError:
            await update.effective_chat.send_message(
                "RCON command timed out. The server may be unresponsive."
            )
        except RconAuthError:
            await update.effective_chat.send_message(
                "RCON authentication is failing; check the password."
            )
        except RconError as exc:
            logger.warning("RCON failure for %s: %s", command, exc)
            await update.effective_chat.send_message(f"RCON error: {exc}")
        else:
            await update.effective_chat.send_message(
                response or "(no response)", parse_mode=ParseMode.MARKDOWN
            )
