"""In-memory ring buffer for telemetry retention."""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Deque, Iterable


class MetricsStore:
    """Bounded time-series store keyed by metric name."""

    def __init__(
        self,
        max_age: timedelta = timedelta(hours=24),
        max_points: int = 50_000,
    ) -> None:
        self._max_age = max_age
        self._max_points = max_points
        self._series: dict[str, Deque[tuple[datetime, float]]] = defaultdict(deque)

    def record(self, metric: str, value: float, *, timestamp: datetime | None = None) -> None:
        ts = timestamp or datetime.now(timezone.utc)
        series = self._series[metric]
        series.append((ts, value))
        self._evict(series)

    def window(self, metric: str, window: timedelta) -> list[tuple[datetime, float]]:
        cutoff = datetime.now(timezone.utc) - window
        return [(ts, value) for ts, value in self._series.get(metric, ()) if ts >= cutoff]

    def metrics(self) -> Iterable[str]:
        return self._series.keys()

    def _evict(self, series: Deque[tuple[datetime, float]]) -> None:
        cutoff = datetime.now(timezone.utc) - self._max_age
        while series and series[0][0] < cutoff:
            series.popleft()
        while len(series) > self._max_points:
            series.popleft()
