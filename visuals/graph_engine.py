"""Off-thread Matplotlib rendering for telemetry visualizations.

All CPU-bound chart rendering is dispatched to the default executor via
:func:`asyncio.get_running_loop().run_in_executor` so the main event loop
remains responsive while large frames are drawn. The module returns PNG
bytes ready for direct upload to Telegram, Slack, or HTTP responses.
"""
from __future__ import annotations

import asyncio
import io
import logging
from datetime import datetime
from typing import Sequence

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

logger = logging.getLogger(__name__)


async def render_ups_chart(
    samples: Sequence[tuple[datetime, float]],
    *,
    warning_threshold: float = 50.0,
    title: str = "Server UPS",
) -> bytes:
    """Render a UPS-over-time line chart annotated with min/max/avg markers.

    Args:
        samples: Time-ordered ``(timestamp, ups)`` pairs.
        warning_threshold: Region below this UPS value is shaded red to
            highlight degraded performance.
        title: Chart title.

    Returns:
        PNG-encoded bytes suitable for messenger upload.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, _render_ups_chart_sync, samples, warning_threshold, title
    )


async def render_production_chart(
    samples: Sequence[tuple[datetime, str, int]],
    *,
    bucket_seconds: int = 300,
    title: str = "Item Production",
) -> bytes:
    """Render a stacked bar chart of items produced per time bucket.

    Args:
        samples: ``(timestamp, item, cumulative_count)`` rows ordered by
            timestamp ascending.
        bucket_seconds: Width of each aggregation bucket, in seconds.
        title: Chart title.

    Returns:
        PNG-encoded bytes.
    """
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, _render_production_chart_sync, samples, bucket_seconds, title
    )


def _render_ups_chart_sync(
    samples: Sequence[tuple[datetime, float]],
    warning_threshold: float,
    title: str,
) -> bytes:
    fig, ax = plt.subplots(figsize=(10, 5), dpi=120)
    try:
        if not samples:
            ax.text(
                0.5,
                0.5,
                "No data",
                transform=ax.transAxes,
                ha="center",
                va="center",
                fontsize=14,
            )
        else:
            timestamps = [point[0] for point in samples]
            values = [point[1] for point in samples]
            ax.plot(
                timestamps,
                values,
                color="#1f77b4",
                linewidth=1.6,
                label="UPS",
            )
            ax.axhspan(
                0,
                warning_threshold,
                facecolor="#d62728",
                alpha=0.12,
                label=f"< {warning_threshold:g} UPS",
            )

            avg = sum(values) / len(values)
            vmin = min(values)
            vmax = max(values)
            ax.axhline(avg, color="#2ca02c", linestyle="--", linewidth=1.0)
            ax.annotate(
                f"avg {avg:.1f}",
                xy=(timestamps[-1], avg),
                xytext=(6, 0),
                textcoords="offset points",
                color="#2ca02c",
                va="center",
            )
            ax.scatter(
                [timestamps[values.index(vmin)]],
                [vmin],
                color="#d62728",
                zorder=5,
                label=f"min {vmin:.1f}",
            )
            ax.scatter(
                [timestamps[values.index(vmax)]],
                [vmax],
                color="#2ca02c",
                zorder=5,
                label=f"max {vmax:.1f}",
            )

        ax.set_title(title)
        ax.set_xlabel("Time (UTC)")
        ax.set_ylabel("UPS")
        ax.grid(True, linestyle="--", alpha=0.4)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.legend(loc="lower left")
        fig.autofmt_xdate()

        buffer = io.BytesIO()
        fig.savefig(buffer, format="png", bbox_inches="tight")
        return buffer.getvalue()
    finally:
        plt.close(fig)


def _render_production_chart_sync(
    samples: Sequence[tuple[datetime, str, int]],
    bucket_seconds: int,
    title: str,
) -> bytes:
    fig, ax = plt.subplots(figsize=(10, 5), dpi=120)
    try:
        if not samples:
            ax.text(
                0.5,
                0.5,
                "No data",
                transform=ax.transAxes,
                ha="center",
                va="center",
                fontsize=14,
            )
        else:
            buckets: dict[int, dict[str, int]] = {}
            for ts, item, count in samples:
                key = int(ts.timestamp()) // bucket_seconds * bucket_seconds
                buckets.setdefault(key, {})[item] = (
                    buckets.get(key, {}).get(item, 0) + count
                )
            sorted_keys = sorted(buckets)
            items = sorted({item for bucket in buckets.values() for item in bucket})
            bottom = [0.0] * len(sorted_keys)
            x_labels = [
                datetime.utcfromtimestamp(key).strftime("%H:%M")
                for key in sorted_keys
            ]
            indices = list(range(len(sorted_keys)))
            for item in items:
                heights = [float(buckets[key].get(item, 0)) for key in sorted_keys]
                ax.bar(indices, heights, bottom=bottom, label=item)
                bottom = [b + h for b, h in zip(bottom, heights)]
            ax.set_xticks(indices)
            ax.set_xticklabels(x_labels, rotation=45, ha="right")
            ax.legend(loc="upper left", fontsize=8, ncol=2)

        ax.set_title(title)
        ax.set_xlabel("Time (UTC)")
        ax.set_ylabel("Items produced")
        ax.grid(True, axis="y", linestyle="--", alpha=0.4)

        buffer = io.BytesIO()
        fig.savefig(buffer, format="png", bbox_inches="tight")
        return buffer.getvalue()
    finally:
        plt.close(fig)
