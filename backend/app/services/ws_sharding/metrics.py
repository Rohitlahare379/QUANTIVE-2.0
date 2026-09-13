"""
Observability Metrics for WebSocket Live Ingestion Pipeline (P0.2 Phase 3).

Exposes thread-safe/asyncio-safe counters, latency metrics, and queue utilization
hooks ready for Phase 6 Prometheus export.
"""

from dataclasses import asdict, dataclass
import time
from typing import Any, Dict
from prometheus_client import Counter, Gauge


WS_PIPELINE_QUEUE_DEPTH = Gauge(
    "quantive_ws_pipeline_queue_depth",
    "Current number of canonical candle events queued by an owned WebSocket shard",
    ["shard_id"],
)
WS_PIPELINE_QUEUE_UTILIZATION = Gauge(
    "quantive_ws_pipeline_queue_utilization_ratio",
    "Current queue utilization ratio for an owned WebSocket shard",
    ["shard_id"],
)
WS_PIPELINE_DEGRADED = Gauge(
    "quantive_ws_pipeline_degraded",
    "Whether an owned WebSocket shard has crossed its configured queue-pressure threshold",
    ["shard_id"],
)
WS_PIPELINE_EVENTS = Counter(
    "quantive_ws_pipeline_events_total",
    "Canonical WebSocket pipeline event outcomes",
    ["shard_id", "outcome"],
)
WS_PIPELINE_BATCH_LATENCY = Gauge(
    "quantive_ws_pipeline_last_batch_latency_milliseconds",
    "Latency of the latest successful canonical WebSocket batch persistence",
    ["shard_id"],
)


@dataclass
class PipelineMetrics:
    """
    Tracks runtime operational counters and queue utilization state for a shard pipeline.
    """
    candles_received: int = 0
    candles_persisted: int = 0
    duplicate_candles: int = 0
    rejected_candles: int = 0
    unmapped_symbol_rejections: int = 0
    inactive_asset_rejections: int = 0
    batches_flushed: int = 0
    persistence_errors: int = 0
    fenced_events_discarded: int = 0
    queue_overflow_count: int = 0
    asset_overflow_count: int = 0
    queue_size: int = 0
    queue_utilization_ratio: float = 0.0
    is_degraded: bool = False
    last_batch_latency_ms: float = 0.0
    last_flush_timestamp: float = 0.0

    def update_queue_stats(self, current_size: int, max_size: int, degraded_threshold: float = 0.90) -> None:
        """Updates instantaneous queue depth and utilization ratio."""
        self.queue_size = current_size
        self.queue_utilization_ratio = float(current_size) / float(max_size) if max_size > 0 else 0.0
        self.is_degraded = self.queue_utilization_ratio >= degraded_threshold

    def record_flush_complete(self, count: int, latency_ms: float) -> None:
        """Records a successful batch persistence operation."""
        self.candles_persisted += count
        self.batches_flushed += 1
        self.last_batch_latency_ms = latency_ms
        self.last_flush_timestamp = time.time()

    def to_dict(self) -> Dict[str, Any]:
        """Serializes current metrics state to a dictionary."""
        return asdict(self)


def publish_pipeline_metrics(shard_id: int, metrics: PipelineMetrics) -> None:
    """Publish bounded per-shard runtime state to the worker's Prometheus registry.

    The supervisor process owns these gauges, while durable freshness/fence/job
    metrics are separately scraped from PostgreSQL by the API metrics endpoint.
    """
    labels = {"shard_id": str(shard_id)}
    WS_PIPELINE_QUEUE_DEPTH.labels(**labels).set(metrics.queue_size)
    WS_PIPELINE_QUEUE_UTILIZATION.labels(**labels).set(metrics.queue_utilization_ratio)
    WS_PIPELINE_DEGRADED.labels(**labels).set(1 if metrics.is_degraded else 0)
    WS_PIPELINE_BATCH_LATENCY.labels(**labels).set(metrics.last_batch_latency_ms)


def record_pipeline_outcome(shard_id: int, outcome: str, count: int = 1) -> None:
    if count > 0:
        WS_PIPELINE_EVENTS.labels(shard_id=str(shard_id), outcome=outcome).inc(count)
