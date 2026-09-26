"""In-process publish/subscribe dispatcher for telemetry events.

The event bus decouples log-parsing producers from consumers such as the
persistence layer and the WebSocket broadcaster. Each subscriber receives
its own bounded :class:`asyncio.Queue`; when a slow consumer falls behind,
the oldest events are dropped rather than blocking the producer.
"""
from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator, Generic, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class EventBus(Generic[T]):
    """Fan-out multi-subscriber broadcast channel.

    Subscribers obtain an async iterator via :meth:`subscribe`; producers
    call :meth:`publish`. The bus owns no event loop; it is safe to share
    a single instance across every task in the process.
    """

    def __init__(self, *, queue_size: int = 256) -> None:
        """Create a bus where each subscriber holds at most ``queue_size`` events."""
        self._queue_size = queue_size
        self._subscribers: set[asyncio.Queue[T]] = set()
        self._lock = asyncio.Lock()

    async def publish(self, event: T) -> None:
        """Broadcast ``event`` to all current subscribers.

        Subscribers whose queues are full lose their oldest event rather
        than blocking the producer; this preserves real-time semantics.
        """
        async with self._lock:
            targets = list(self._subscribers)
        for queue in targets:
            if queue.full():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
            queue.put_nowait(event)

    async def subscribe(self) -> AsyncIterator[T]:
        """Yield events as they arrive for this subscriber.

        The iterator unregisters automatically when the consumer exits the
        ``async for`` (whether by ``break``, exception, or cancellation).
        """
        queue: asyncio.Queue[T] = asyncio.Queue(maxsize=self._queue_size)
        async with self._lock:
            self._subscribers.add(queue)
        try:
            while True:
                yield await queue.get()
        finally:
            async with self._lock:
                self._subscribers.discard(queue)

    def subscriber_count(self) -> int:
        """Return the number of currently registered subscribers."""
        return len(self._subscribers)
