"""Top-level entry point for the Factorio administration service.

Wires together the RCON client, log tailer, persistence layer, event bus,
WebSocket broadcaster, and messenger bots under a single
:func:`asyncio.gather` and installs SIGINT/SIGTERM handlers for graceful
shutdown. All errors are logged via the stdlib ``logging`` module; no
exception is silently swallowed.
"""
from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

from factorio_admin.bots.telegram_controller import TelegramController
from factorio_admin.config import Settings, load_settings
from factorio_admin.core.event_bus import EventBus
from factorio_admin.core.rcon_client import RconClient
from factorio_admin.parsers.log_parser import LogTailer, ParsedEvent
from factorio_admin.persistence.db import TelemetryStore
from factorio_admin.web.ws_server import WebSocketBroadcaster

logger = logging.getLogger(__name__)


async def _ingest_logs(
    tailer: LogTailer,
    bus: EventBus[ParsedEvent],
    store: TelemetryStore,
) -> None:
    """Forward every parsed log event into both the bus and the store."""
    async for event in tailer.stream():
        store.enqueue(event)
        await bus.publish(event)


async def _run(settings: Settings) -> None:
    """Construct services, gather them, and orchestrate graceful shutdown."""
    bus: EventBus[ParsedEvent] = EventBus()
    store = TelemetryStore(
        settings.db_path, flush_interval=settings.db_flush_interval_seconds
    )
    await store.open()

    rcon = RconClient(
        settings.rcon_host,
        settings.rcon_port,
        settings.rcon_password,
        command_timeout=settings.rcon_command_timeout,
        connect_timeout=settings.rcon_connect_timeout,
        max_backoff_seconds=settings.rcon_max_backoff_seconds,
    )

    tailer = LogTailer(settings.log_file_path)
    ws = WebSocketBroadcaster(
        bus,
        host=settings.ws_host,
        port=settings.ws_port,
        secret_token=settings.ws_secret_token,
    )

    services: list[Any] = []
    services.append(asyncio.create_task(rcon.run(), name="rcon"))
    services.append(asyncio.create_task(store.flush_loop(), name="db_flush"))
    services.append(
        asyncio.create_task(_ingest_logs(tailer, bus, store), name="log_ingest")
    )
    services.append(asyncio.create_task(ws.serve_forever(), name="ws_server"))

    telegram: TelegramController | None = None
    if settings.telegram_enabled:
        telegram = TelegramController(
            settings.telegram_bot_token,
            rcon,
            store,
            allowed_user_ids=set(settings.telegram_allowed_user_ids),
            rate_limit_max=settings.rate_limit_max_commands,
            rate_limit_window_seconds=settings.rate_limit_window_seconds,
            ups_warning_threshold=settings.ups_warning_threshold,
        )
        await telegram.start()

    slack = None
    if settings.slack_enabled:
        from factorio_admin.bots.slack_controller import SlackController

        slack = SlackController(
            settings.slack_bot_token,
            settings.slack_app_token,
            rcon,
            store,
            rate_limit_max=settings.rate_limit_max_commands,
            rate_limit_window_seconds=settings.rate_limit_window_seconds,
            ups_warning_threshold=settings.ups_warning_threshold,
        )
        await slack.start()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, AttributeError):
            pass

    try:
        done, _pending = await asyncio.wait(
            [asyncio.create_task(stop_event.wait()), *services],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            exc = task.exception() if not task.cancelled() else None
            if exc is not None:
                logger.error("Service task failed: %s", exc, exc_info=exc)
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        logger.info("Shutting down")
        tailer.stop()
        ws.stop()
        if telegram is not None:
            await telegram.stop()
        if slack is not None:
            await slack.stop()
        for task in services:
            task.cancel()
        await asyncio.gather(*services, return_exceptions=True)
        await rcon.close()
        await store.close()
        logger.info("Shutdown complete")


def main() -> None:
    """Process entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = load_settings()
    try:
        asyncio.run(_run(settings))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
