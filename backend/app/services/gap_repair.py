"""
REST Reconciliation & Gap Repair Service (P0.2 Phase 4).

Coordinates gap detection from sync_ranges, durable distributed job state machine
with FOR UPDATE SKIP LOCKED, lease heartbeating, Binance REST rate-limited streaming,
strict validation, bounded memory batching, atomic persistence, and CAGG refresh scheduling.
"""

import asyncio
import logging
import traceback
import uuid
from datetime import datetime, timezone, timedelta
from typing import TYPE_CHECKING, AsyncGenerator, Dict, Any, List, Optional, Tuple

import httpx
from sqlalchemy import select, update, text, delete
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

if TYPE_CHECKING:
    from app.connectors.binance import BinanceClient
from app.connectors.exceptions import (
    ConnectorError,
    NetworkError,
    APIError,
    RateLimitError,
    TemporaryBanError,
    PayloadCorruptionError,
    MalformedMessageError,
)
from app.core.config import settings
from app.models.asset_registry import AssetRegistry
from app.models.gap_repair_jobs import GapRepairJob, GapRepairStatus
from app.models.cagg_refresh_jobs import CaggRefreshJob, RefreshStatus
from app.models.sync_ranges import SyncRange
from app.services.cagg_refresh import compute_cagg_bucket_alignment
from app.services.ingestion import IngestionService

logger = logging.getLogger(__name__)


def classify_error(e: Exception) -> Tuple[str, bool]:
    """
    Classifies an exception into an error category and determines if it is retryable.

    Categories:
    - RATE_LIMITED (retryable)
    - NETWORK (retryable)
    - DATABASE (retryable)
    - TRANSIENT (retryable)
    - VALIDATION (non-retryable)
    - PERMANENT (non-retryable)
    - AUTHENTICATION (non-retryable)

    Returns:
        Tuple[str, bool]: (category_name, is_retryable)
    """
    if isinstance(e, (RateLimitError, TemporaryBanError)):
        return "RATE_LIMITED", True

    if isinstance(e, (NetworkError, httpx.RequestError, asyncio.TimeoutError, TimeoutError, ConnectionError)):
        return "NETWORK", True

    if isinstance(e, (PayloadCorruptionError, MalformedMessageError, ValueError)):
        return "VALIDATION", False

    if isinstance(e, APIError):
        err_str = str(e).lower()
        if "401" in err_str or "403" in err_str or "auth" in err_str or "api key" in err_str:
            return "AUTHENTICATION", False
        if "400" in err_str or "invalid symbol" in err_str or "illegal" in err_str:
            return "PERMANENT", False
        return "TRANSIENT", True

    # Database operational / connection issues
    err_type = type(e).__name__
    if "OperationalError" in err_type or "DBAPIError" in err_type or "InterfaceError" in err_type:
        return "DATABASE", True

    return "TRANSIENT", True


class GapRepairService:
    """
    Service responsible for detecting missing candle coverage intervals, scheduling durable
    repair jobs, executing distributed rate-limited REST reconciliations with bounded memory,
    and triggering downstream CAGG refresh jobs.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        binance_client: Optional[BinanceClient] = None,
    ):
        self.session_factory = session_factory
        self.client = binance_client

    async def detect_gaps(
        self, asset_id: int, start_time: datetime, end_time: datetime
    ) -> List[Tuple[datetime, datetime]]:
        """
        Detects missing intervals within [start_time, end_time] for the given asset_id.
        Reuses existing IngestionService.detect_missing_ranges.
        """
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        if start_time >= end_time:
            return []

        async with self.session_factory() as session:
            ingestion = IngestionService(db_session=session)
            return await ingestion.detect_missing_ranges(asset_id, start_time, end_time)

    async def schedule_repair_job(
        self,
        asset_id: int,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        max_retries: int = 5,
    ) -> Optional[GapRepairJob]:
        """
        Schedules a GapRepairJob for an identified gap interval.
        Idempotent: prevents creating duplicate active (PENDING or PROCESSING) jobs for the exact same asset and range.
        """
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        if start_time >= end_time:
            return None

        async with self.session_factory() as session:
            async with session.begin():
                # Check for existing active job covering exact range
                stmt = select(GapRepairJob).where(
                    GapRepairJob.asset_id == asset_id,
                    GapRepairJob.start_time == start_time,
                    GapRepairJob.end_time == end_time,
                    GapRepairJob.status.in_([GapRepairStatus.PENDING, GapRepairStatus.PROCESSING]),
                )
                result = await session.execute(stmt)
                existing = result.scalars().first()
                if existing:
                    logger.debug(
                        f"Active gap repair job #{existing.id} already exists for {symbol} [{start_time} -> {end_time}]"
                    )
                    return existing

                job = GapRepairJob(
                    asset_id=asset_id,
                    symbol=symbol.upper(),
                    start_time=start_time,
                    end_time=end_time,
                    status=GapRepairStatus.PENDING,
                    max_retries=max_retries,
                    retry_count=0,
                )
                session.add(job)
            await session.commit()
            return job

    async def claim_job(
        self,
        worker_id: str,
        lease_duration: timedelta = timedelta(minutes=5),
    ) -> Optional[GapRepairJob]:
        """
        Claims the oldest PENDING or stale PROCESSING job atomically using SELECT ... FOR UPDATE SKIP LOCKED.
        """
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            async with session.begin():
                stmt = (
                    select(GapRepairJob)
                    .where(
                        (GapRepairJob.status == GapRepairStatus.PENDING)
                        | (
                            (GapRepairJob.status == GapRepairStatus.PROCESSING)
                            & (GapRepairJob.lease_expires_at < now)
                        )
                    )
                    .order_by(GapRepairJob.created_at.asc(), GapRepairJob.id.asc())
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                result = await session.execute(stmt)
                job = result.scalars().first()

                if not job:
                    return None

                job.status = GapRepairStatus.PROCESSING
                job.worker_id = worker_id
                job.claimed_at = now
                job.lease_expires_at = now + lease_duration

                claimed_job_id = job.id

            # Commit closes transaction, row lock released.
            # Fetch fresh detached instance
            job = await session.get(GapRepairJob, claimed_job_id)
            return job

    async def _heartbeat_loop(
        self,
        job_id: int,
        worker_id: str,
        lease_duration: timedelta,
        sleep_interval: Optional[float] = None,
    ):
        """
        Periodically extends lease_expires_at on independent database sessions.
        """
        if sleep_interval is None:
            sleep_interval = max(0.5, lease_duration.total_seconds() / 2.0)

        while True:
            try:
                await asyncio.sleep(sleep_interval)
            except asyncio.CancelledError:
                break

            try:
                async with self.session_factory() as session:
                    async with session.begin():
                        new_expiry = datetime.now(timezone.utc) + lease_duration
                        stmt = (
                            update(GapRepairJob)
                            .where(
                                GapRepairJob.id == job_id,
                                GapRepairJob.status == GapRepairStatus.PROCESSING,
                                GapRepairJob.worker_id == worker_id,
                            )
                            .values(lease_expires_at=new_expiry)
                        )
                        result = await session.execute(stmt)
                        if result.rowcount == 0:
                            logger.warning(
                                f"Worker {worker_id} lost ownership of GapRepairJob #{job_id} during heartbeat."
                            )
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error for GapRepairJob #{job_id}: {e}")

    async def process_next_job(
        self,
        worker_id: Optional[str] = None,
        lease_duration: timedelta = timedelta(minutes=5),
        heartbeat_interval: Optional[float] = None,
        binance_client: Optional[BinanceClient] = None,
        batch_size: int = 1000,
    ) -> bool:
        """
        Claims and processes a single pending/stale gap repair job.
        Returns True if a job was processed, False if queue was empty.
        """
        worker_id = worker_id or str(uuid.uuid4())
        job = await self.claim_job(worker_id=worker_id, lease_duration=lease_duration)
        if not job:
            return False

        job_id = job.id
        asset_id = job.asset_id
        symbol = job.symbol
        start_time = job.start_time
        end_time = job.end_time

        logger.info(f"Worker {worker_id} claimed GapRepairJob #{job_id} for {symbol} [{start_time} -> {end_time}]")

        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(
                job_id=job_id,
                worker_id=worker_id,
                lease_duration=lease_duration,
                sleep_interval=heartbeat_interval,
            )
        )

        try:
            # Execute reconciliation with bounded streaming batches
            await self._execute_reconciliation(
                asset_id=asset_id,
                symbol=symbol,
                start_time=start_time,
                end_time=end_time,
                binance_client=binance_client,
                batch_size=batch_size,
            )

            # Schedule downstream CAGG refresh job for the repaired window
            aligned_start, aligned_end = compute_cagg_bucket_alignment(start_time, end_time)
            async with self.session_factory() as session:
                async with session.begin():
                    cagg_job = CaggRefreshJob(
                        window_start=aligned_start,
                        window_end=aligned_end,
                        status=RefreshStatus.PENDING,
                    )
                    session.add(cagg_job)

            # Mark completed conditionally (only if we still own the job)
            async with self.session_factory() as session:
                async with session.begin():
                    stmt = (
                        update(GapRepairJob)
                        .where(
                            GapRepairJob.id == job_id,
                            GapRepairJob.status == GapRepairStatus.PROCESSING,
                            GapRepairJob.worker_id == worker_id,
                        )
                        .values(
                            status=GapRepairStatus.COMPLETED,
                            error_message=None,
                            error_category=None,
                            lease_expires_at=None,
                        )
                    )
                    res = await session.execute(stmt)
                    if res.rowcount > 0:
                        logger.info(f"Worker {worker_id} successfully completed GapRepairJob #{job_id}")
                    else:
                        logger.warning(
                            f"Worker {worker_id} completed repair for job #{job_id} but no longer owned lease."
                        )

        except Exception as e:
            category, is_retryable = classify_error(e)
            err_msg = f"{type(e).__name__}: {str(e)}"
            logger.error(
                f"GapRepairJob #{job_id} failed with {category} error (retryable={is_retryable}): {err_msg}",
                exc_info=True,
            )

            async with self.session_factory() as session:
                async with session.begin():
                    current_job = await session.get(GapRepairJob, job_id)
                    if current_job and current_job.worker_id == worker_id and current_job.status == GapRepairStatus.PROCESSING:
                        current_job.retry_count += 1
                        current_job.error_message = err_msg + "\n" + traceback.format_exc()
                        current_job.error_category = category

                        if is_retryable and current_job.retry_count <= current_job.max_retries:
                            # Requeue job for retry
                            current_job.status = GapRepairStatus.PENDING
                            current_job.worker_id = None
                            current_job.claimed_at = None
                            current_job.lease_expires_at = None
                            logger.info(
                                f"Requeued GapRepairJob #{job_id} for retry ({current_job.retry_count}/{current_job.max_retries})"
                            )
                        else:
                            # Terminal failure
                            current_job.status = GapRepairStatus.FAILED
                            current_job.lease_expires_at = None
                            logger.warning(
                                f"GapRepairJob #{job_id} marked as FAILED (retries exhausted or non-retryable error)"
                            )

            raise
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

        return True

    async def _execute_reconciliation(
        self,
        asset_id: int,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        binance_client: Optional[BinanceClient] = None,
        batch_size: int = 1000,
    ) -> int:
        """
        Executes bounded REST streaming reconciliation for [start_time, end_time].
        Fetches candles page-by-page from Binance REST API, batches them in memory,
        and commits each batch to PostgreSQL in short, independent DB transactions.
        Guarantees O(1) memory consumption regardless of gap length.
        """
        client = binance_client or self.client
        owns_client = False
        if client is None:
            from app.connectors.binance import BinanceClient
            client = BinanceClient()
            owns_client = True

        total_candles = 0

        async def _run_stream(active_client: BinanceClient):
            nonlocal total_candles
            batch: List[Dict[str, Any]] = []

            async for candle in active_client.get_klines(symbol, "1m", start_time, end_time):
                candle["asset_id"] = asset_id
                batch.append(candle)
                total_candles += 1

                if len(batch) >= batch_size:
                    # Commit batch in short, dedicated DB session
                    async with self.session_factory() as session:
                        ingestion = IngestionService(db_session=session)
                        await ingestion._commit_batch(asset_id, batch)
                    batch = []

            # Flush remaining candles
            if batch:
                async with self.session_factory() as session:
                    ingestion = IngestionService(db_session=session)
                    await ingestion._commit_batch(asset_id, batch)
                batch = []

        if owns_client:
            async with client:
                await _run_stream(client)
        else:
            await _run_stream(client)

        return total_candles

    async def repair_gap_inline(
        self,
        asset_id: int,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        binance_client: Optional[BinanceClient] = None,
        batch_size: int = 1000,
    ) -> int:
        """
        Direct synchronous/async execution for immediate repair of a missing interval
        (e.g., triggered on WebSocket reconnection).
        """
        # 1. Detect actual missing sub-intervals within [start_time, end_time]
        gaps = await self.detect_gaps(asset_id, start_time, end_time)
        if not gaps:
            return 0

        total_repaired = 0
        for gap_start, gap_end in gaps:
            count = await self._execute_reconciliation(
                asset_id=asset_id,
                symbol=symbol,
                start_time=gap_start,
                end_time=gap_end,
                binance_client=binance_client,
                batch_size=batch_size,
            )
            total_repaired += count

            # Schedule downstream CAGG refresh
            aligned_start, aligned_end = compute_cagg_bucket_alignment(gap_start, gap_end)
            async with self.session_factory() as session:
                async with session.begin():
                    cagg_job = CaggRefreshJob(
                        window_start=aligned_start,
                        window_end=aligned_end,
                        status=RefreshStatus.PENDING,
                    )
                    session.add(cagg_job)

        return total_repaired

    async def scan_and_schedule_active_assets(
        self,
        lookback_window: timedelta = timedelta(hours=24),
    ) -> List[GapRepairJob]:
        """
        Scans all active assets in AssetRegistry, detects any gaps within [now - lookback_window, now],
        and schedules GapRepairJobs for each missing interval.
        """
        now = datetime.now(timezone.utc)
        start_time = now - lookback_window
        scheduled_jobs: List[GapRepairJob] = []

        async with self.session_factory() as session:
            stmt = select(AssetRegistry).where(AssetRegistry.is_active.is_(True))
            result = await session.execute(stmt)
            active_assets = result.scalars().all()

        for asset in active_assets:
            gaps = await self.detect_gaps(asset.id, start_time, now)
            for gap_start, gap_end in gaps:
                job = await self.schedule_repair_job(
                    asset_id=asset.id,
                    symbol=asset.symbol,
                    start_time=gap_start,
                    end_time=gap_end,
                )
                if job:
                    scheduled_jobs.append(job)

        return scheduled_jobs
