import json
import logging
import time
from datetime import datetime, timezone
from fastapi import APIRouter
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest, Gauge, CONTENT_TYPE_LATEST

from app.workers.config import borrow_async_redis_for_probe

router = APIRouter(tags=["Metrics"])
logger = logging.getLogger(__name__)

# Define Prometheus Metrics
DLQ_JOB_COUNT = Gauge(
    'dlq_job_count',
    'Total number of jobs currently in the Dead Letter Queue'
)
DLQ_JOBS_BY_TYPE = Gauge(
    'dlq_jobs_by_type',
    'Number of jobs in the Dead Letter Queue, grouped by actor name',
    ['actor_name']
)
DLQ_OLDEST_JOB_AGE_SECONDS = Gauge(
    'dlq_oldest_job_age_seconds',
    'Age of the oldest message in the Dead Letter Queue in seconds'
)

# CAGG Refresh Workflow Metrics
CAGG_REFRESH_PENDING_JOBS = Gauge(
    'cagg_refresh_pending_jobs',
    'Total number of CAGG refresh jobs currently pending'
)
CAGG_REFRESH_FAILED_JOBS = Gauge(
    'cagg_refresh_failed_jobs',
    'Total number of CAGG refresh jobs that have failed'
)

CANONICAL_INGESTION_LAST_RECEIVED_UNIX = Gauge(
    "quantive_canonical_ingestion_last_received_timestamp_seconds",
    "Unix timestamp of the most recently received canonical raw candle",
)
WS_PERSISTENCE_FENCE_SHARDS = Gauge(
    "quantive_ws_persistence_fence_shards",
    "Number of WebSocket shards with a durable PostgreSQL persistence fence",
)
WS_PERSISTENCE_FENCE_MAX_GENERATION = Gauge(
    "quantive_ws_persistence_fence_max_generation",
    "Highest durable WebSocket persistence fencing generation",
)
DURABLE_WORKFLOW_JOBS = Gauge(
    "quantive_durable_workflow_jobs",
    "Durable workflow jobs grouped by workflow and persisted state",
    ["workflow", "status"],
)
DURABLE_WORKFLOW_EXPIRED_LEASES = Gauge(
    "quantive_durable_workflow_expired_leases",
    "Processing durable workflow jobs whose persisted lease has expired",
    ["workflow"],
)

# A scrape that silently returns the previous successful values when a
# dependency is unavailable is worse than a failed scrape: it makes an outage
# look healthy.  Keep the endpoint scrapeable, but expose the freshness of
# each dependency explicitly so alerting can fail closed.
METRICS_DEPENDENCY_UP = Gauge(
    'quantive_metrics_dependency_up',
    'Whether a dependency used to collect Quantive operational metrics was reachable during this scrape',
    ['dependency'],
)


async def _collect_cagg_metrics() -> bool:
    """Collect bounded database metrics and report whether this scrape was fresh."""
    # Import lazily so the metrics module remains importable before database
    # configuration is fully initialized (for example during CLI migration
    # commands).
    from app.db.session import async_session_maker
    from sqlalchemy import select, func
    from app.models.cagg_refresh_jobs import CaggRefreshJob, RefreshStatus

    try:
        async with async_session_maker() as session:
            pending_count = await session.scalar(
                select(func.count()).where(CaggRefreshJob.status == RefreshStatus.PENDING)
            )
            failed_count = await session.scalar(
                select(func.count()).where(CaggRefreshJob.status == RefreshStatus.FAILED)
            )
        CAGG_REFRESH_PENDING_JOBS.set(pending_count or 0)
        CAGG_REFRESH_FAILED_JOBS.set(failed_count or 0)
        return True
    except Exception:
        logger.exception("Unable to collect CAGG refresh metrics")
        return False


async def _collect_durable_data_metrics() -> bool:
    """Export database-backed freshness, fencing, backlog, and lease health.

    These metrics are intentionally derived from canonical tables and durable
    job state, rather than process-local worker counters, so an API replica can
    expose them after a worker restart.  The separate WebSocket worker gauges
    report instantaneous queue pressure.
    """
    from sqlalchemy import func, select
    from app.db.session import async_session_maker
    from app.models.raw_1m_candles import Raw1mCandle
    from app.models.ws_shard_persistence_fence import WsShardPersistenceFence
    from app.models.gap_repair_jobs import GapRepairJob, GapRepairStatus
    from app.models.cagg_refresh_jobs import CaggRefreshJob, RefreshStatus
    from app.models.historical_merge_jobs import HistoricalMergeJob, HistoricalMergeStatus

    workflows = (
        ("gap_repair", GapRepairJob, GapRepairJob.status, tuple(GapRepairStatus), GapRepairStatus.PROCESSING),
        ("cagg_refresh", CaggRefreshJob, CaggRefreshJob.status, tuple(RefreshStatus), RefreshStatus.PROCESSING),
        (
            "historical_merge",
            HistoricalMergeJob,
            HistoricalMergeJob.status,
            tuple(HistoricalMergeStatus),
            HistoricalMergeStatus.PROCESSING,
        ),
    )
    try:
        async with async_session_maker() as session:
            latest_received = await session.scalar(select(func.max(Raw1mCandle.source_received_at)))
            fence_count, max_generation = (
                await session.execute(
                    select(func.count(WsShardPersistenceFence.shard_id), func.max(WsShardPersistenceFence.fencing_token))
                )
            ).one()
            for workflow, model, status_column, statuses, processing_status in workflows:
                rows = (
                    await session.execute(
                        select(status_column, func.count(model.id)).group_by(status_column)
                    )
                ).all()
                counts = {status: count for status, count in rows}
                for status_value in statuses:
                    DURABLE_WORKFLOW_JOBS.labels(workflow=workflow, status=status_value.value).set(
                        counts.get(status_value, 0)
                    )
                expired = await session.scalar(
                    select(func.count(model.id)).where(
                        status_column == processing_status,
                        model.lease_expires_at.is_not(None),
                        model.lease_expires_at < datetime.now(timezone.utc),
                    )
                )
                DURABLE_WORKFLOW_EXPIRED_LEASES.labels(workflow=workflow).set(expired or 0)

        CANONICAL_INGESTION_LAST_RECEIVED_UNIX.set(
            latest_received.timestamp() if latest_received is not None else 0
        )
        WS_PERSISTENCE_FENCE_SHARDS.set(fence_count or 0)
        WS_PERSISTENCE_FENCE_MAX_GENERATION.set(max_generation or 0)
        return True
    except Exception:
        logger.exception("Unable to collect durable data-foundation metrics")
        return False


async def _collect_dlq_metrics() -> bool:
    """Collect bounded Redis DLQ metrics without masking Redis failure."""
    dlq_key = "dramatiq:default.DQ"
    try:
        async with borrow_async_redis_for_probe() as redis:
            # 1. Total Job Count
            total_count = await redis.llen(dlq_key)
            DLQ_JOB_COUNT.set(total_count)

            # 2. Extract Oldest Message (Last element in the list)
            if total_count > 0:
                # Redis LRANGE -1 -1 gets the oldest element in O(1)
                oldest_raw = await redis.lrange(dlq_key, -1, -1)
                if oldest_raw:
                    try:
                        msg = json.loads(oldest_raw[0])
                        # Dramatiq messages have 'message_timestamp' in ms
                        timestamp_ms = msg.get("message_timestamp")
                        if timestamp_ms:
                            age_seconds = time.time() - (timestamp_ms / 1000.0)
                            DLQ_OLDEST_JOB_AGE_SECONDS.set(max(0, age_seconds))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        # A malformed dead-letter payload must not prevent bounded
                        # metrics for the rest of the queue.  Redis itself was
                        # still reachable, which is what this dependency gauge
                        # represents.
                        logger.warning("Unable to parse oldest Dramatiq DLQ payload")

                # 3. Jobs By Type (sample only the first 1,000 to prevent Redis
                # blocking if the DLQ explodes).
                sample_size = min(total_count, 1000)
                sample_raw = await redis.lrange(dlq_key, 0, sample_size - 1)

                actor_counts = {}
                for raw in sample_raw:
                    try:
                        msg = json.loads(raw)
                        actor = msg.get("actor_name", "unknown")
                        actor_counts[actor] = actor_counts.get(actor, 0) + 1
                    except (TypeError, ValueError, json.JSONDecodeError):
                        logger.warning("Unable to parse Dramatiq DLQ payload")
                        continue

                # We scale the sampled count to estimate the total distribution safely.
                scale_factor = total_count / sample_size if sample_size > 0 else 1
                for actor, count in actor_counts.items():
                    DLQ_JOBS_BY_TYPE.labels(actor_name=actor).set(count * scale_factor)
            else:
                DLQ_OLDEST_JOB_AGE_SECONDS.set(0)
                # Clear gauge labels if DLQ is empty (to reset alerts)
                DLQ_JOBS_BY_TYPE.clear()
        return True
    except Exception:
        logger.exception("Unable to collect Dramatiq DLQ metrics")
        return False

@router.get("/metrics")
async def get_metrics():
    """
    Exposes Prometheus metrics including Dramatiq Dead Letter Queue introspection.
    Operates in O(1) or bounded O(N) where N is DLQ size, ensuring no Redis exhaustion.
    """
    cagg_metrics_up = await _collect_cagg_metrics()
    durable_metrics_up = await _collect_durable_data_metrics()
    database_up = cagg_metrics_up and durable_metrics_up
    redis_up = await _collect_dlq_metrics()
    METRICS_DEPENDENCY_UP.labels(dependency="database").set(1 if database_up else 0)
    METRICS_DEPENDENCY_UP.labels(dependency="redis").set(1 if redis_up else 0)

    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)
