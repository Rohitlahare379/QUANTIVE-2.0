"""Bounded, durable REST reconciliation and gap-repair work."""

from __future__ import annotations

import asyncio
import logging
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Iterable, List, Optional, Tuple

import httpx
from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

if TYPE_CHECKING:
    from app.connectors.binance import BinanceClient

from app.connectors.exceptions import (
    APIError,
    MalformedMessageError,
    NetworkError,
    PayloadCorruptionError,
    RateLimitError,
    TemporaryBanError,
)
from app.core.config import settings
from app.models.asset_registry import AssetRegistry
from app.models.gap_repair_jobs import GapRepairJob, GapRepairStatus
from app.services.cagg_refresh import (
    LeaseLostError,
    compute_cagg_bucket_alignment,
    schedule_cagg_refresh_job,
)
from app.services.ingestion import IngestionService

logger = logging.getLogger(__name__)


class IncompleteCoverageError(RuntimeError):
    """REST returned/persisted less canonical coverage than the claimed window."""


def classify_error(error: Exception) -> Tuple[str, bool]:
    """Return a stable error category and whether the durable job may retry."""
    if isinstance(error, IncompleteCoverageError):
        return "INCOMPLETE_COVERAGE", True
    if isinstance(error, (RateLimitError, TemporaryBanError)):
        return "RATE_LIMITED", True
    if isinstance(error, (NetworkError, httpx.RequestError, asyncio.TimeoutError, TimeoutError, ConnectionError)):
        return "NETWORK", True
    if isinstance(error, (PayloadCorruptionError, MalformedMessageError, ValueError)):
        return "VALIDATION", False
    if isinstance(error, APIError):
        error_text = str(error).lower()
        if any(marker in error_text for marker in ("401", "403", "auth", "api key")):
            return "AUTHENTICATION", False
        if any(marker in error_text for marker in ("400", "invalid symbol", "illegal")):
            return "PERMANENT", False
        return "TRANSIENT", True
    error_type = type(error).__name__
    if any(marker in error_type for marker in ("OperationalError", "DBAPIError", "InterfaceError")):
        return "DATABASE", True
    return "TRANSIENT", True


class GapRepairService:
    """Schedule, fence, and execute bounded REST repair jobs.

    Binance calls occur with no PostgreSQL transaction open.  Each persisted
    batch is committed in a separate short transaction after the job token has
    been checked, so a heartbeat failure stops future writes instead of allowing
    a stale worker to continue indefinitely.
    """

    _ACTIVE_STATUSES: tuple[GapRepairStatus, ...] = (
        GapRepairStatus.PENDING,
        GapRepairStatus.PROCESSING,
        GapRepairStatus.AWAITING_MERGE,
    )

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        binance_client: Optional[BinanceClient] = None,
    ):
        self.session_factory = session_factory
        self.client = binance_client

    @staticmethod
    def _normalise_timestamp(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _retry_delay(retry_count: int) -> timedelta:
        return timedelta(seconds=min(3600, 30 * (2 ** max(0, retry_count - 1))))

    @staticmethod
    def _uncovered_intervals(
        start: datetime,
        end: datetime,
        existing: Iterable[GapRepairJob],
    ) -> list[tuple[datetime, datetime]]:
        """Subtract actively scheduled ranges from a proposed repair window."""
        intervals = sorted(
            (
                max(start, GapRepairService._normalise_timestamp(job.start_time)),
                min(end, GapRepairService._normalise_timestamp(job.end_time)),
            )
            for job in existing
            if job.start_time < end and job.end_time > start
        )
        cursor = start
        uncovered: list[tuple[datetime, datetime]] = []
        for current_start, current_end in intervals:
            if current_end <= cursor:
                continue
            if current_start > cursor:
                uncovered.append((cursor, current_start))
            cursor = max(cursor, current_end)
            if cursor >= end:
                break
        if cursor < end:
            uncovered.append((cursor, end))
        return uncovered

    async def detect_gaps(
        self, asset_id: int, start_time: datetime, end_time: datetime
    ) -> List[Tuple[datetime, datetime]]:
        start_time = self._normalise_timestamp(start_time)
        end_time = self._normalise_timestamp(end_time)
        if start_time >= end_time:
            return []
        async with self.session_factory() as session:
            ingestion = IngestionService(db_session=session)
            return await ingestion.detect_missing_ranges(asset_id, start_time, end_time)

    async def schedule_repair_jobs(
        self,
        asset_id: int,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        max_retries: int = 5,
    ) -> list[GapRepairJob]:
        """Create durable jobs only for uncovered active ranges.

        Locking the asset row gives every scheduler a common serialization point.
        The partial exclusion constraint installed by migration 015 remains a
        database backstop for writes that bypass this service.
        """
        start_time = self._normalise_timestamp(start_time)
        end_time = self._normalise_timestamp(end_time)
        if start_time >= end_time:
            return []
        if max_retries < 0:
            raise ValueError("max_retries must be non-negative")

        async with self.session_factory() as session:
            async with session.begin():
                asset_result = await session.execute(
                    select(AssetRegistry)
                    .where(
                        AssetRegistry.id == asset_id,
                        AssetRegistry.exchange == "BINANCE",
                        AssetRegistry.is_active.is_(True),
                    )
                    .with_for_update()
                )
                asset = asset_result.scalar_one_or_none()
                if asset is None:
                    logger.warning(
                        "Refusing REST repair for inactive/non-Binance asset %s", asset_id
                    )
                    return []

                active = (
                    await session.execute(
                        select(GapRepairJob)
                        .where(
                            GapRepairJob.asset_id == asset_id,
                            GapRepairJob.status.in_(self._ACTIVE_STATUSES),
                            GapRepairJob.start_time < end_time,
                            GapRepairJob.end_time > start_time,
                        )
                        .order_by(GapRepairJob.start_time.asc(), GapRepairJob.id.asc())
                        .with_for_update()
                        # An exclusion constraint prevents overlap, but a
                        # large requested window can still intersect many
                        # adjacent durable jobs.  Keep scheduling memory
                        # bounded and let existing work drain before a later
                        # scan fills any remaining suffix.
                        .limit(settings.GAP_REPAIR_MAX_ACTIVE_OVERLAPS + 1)
                    )
                ).scalars().all()
                if len(active) > settings.GAP_REPAIR_MAX_ACTIVE_OVERLAPS:
                    logger.warning(
                        "Active gap-repair overlap scan reached its cap for asset %s; "
                        "deferring new scheduling until existing work drains",
                        asset_id,
                        extra={
                            "asset_id": asset_id,
                            "max_active_overlaps": settings.GAP_REPAIR_MAX_ACTIVE_OVERLAPS,
                            "event": "gap_repair_overlap_scan_bounded",
                        },
                    )
                    return active[:1]
                uncovered = self._uncovered_intervals(start_time, end_time, active)
                created: list[GapRepairJob] = []
                for interval_start, interval_end in uncovered:
                    job = GapRepairJob(
                        asset_id=asset_id,
                        symbol=asset.symbol.upper(),
                        start_time=interval_start,
                        end_time=interval_end,
                        status=GapRepairStatus.PENDING,
                        max_retries=max_retries,
                        retry_count=0,
                    )
                    session.add(job)
                    created.append(job)
                await session.flush()
                return created or active[:1]

    async def schedule_repair_job(
        self,
        asset_id: int,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        max_retries: int = 5,
    ) -> Optional[GapRepairJob]:
        """Compatibility wrapper returning one newly-created/existing job."""
        jobs = await self.schedule_repair_jobs(
            asset_id=asset_id,
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            max_retries=max_retries,
        )
        return jobs[0] if jobs else None

    async def claim_job(
        self,
        worker_id: str,
        lease_duration: timedelta = timedelta(minutes=5),
        *,
        respect_retry_schedule: bool = False,
    ) -> Optional[GapRepairJob]:
        """Atomically claim one pending/expired job and issue a fresh fencing token.

        Normal workers set ``respect_retry_schedule=True``.  The default keeps
        the historical direct-service recovery API useful for a deliberate manual
        retry while all production actor paths honor durable backoff.
        """
        if lease_duration.total_seconds() <= 0:
            raise ValueError("lease_duration must be positive")
        now = datetime.now(timezone.utc)
        pending = GapRepairJob.status == GapRepairStatus.PENDING
        if respect_retry_schedule:
            pending = and_(
                pending,
                or_(
                    GapRepairJob.next_attempt_at.is_(None),
                    GapRepairJob.next_attempt_at <= now,
                ),
            )
        async with self.session_factory() as session:
            async with session.begin():
                job = (
                    await session.execute(
                        select(GapRepairJob)
                        .where(
                            or_(
                                pending,
                                and_(
                                    GapRepairJob.status == GapRepairStatus.PROCESSING,
                                    GapRepairJob.lease_expires_at < now,
                                ),
                            )
                        )
                        .order_by(GapRepairJob.created_at.asc(), GapRepairJob.id.asc())
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                ).scalars().first()
                if job is None:
                    return None
                job.status = GapRepairStatus.PROCESSING
                job.worker_id = worker_id
                job.lease_token = uuid.uuid4().hex
                job.claimed_at = now
                job.lease_expires_at = now + lease_duration
                job.next_attempt_at = None
                job_id = job.id
            return await session.get(GapRepairJob, job_id)

    async def _assert_lease_owned(
        self,
        *,
        job_id: int,
        worker_id: str,
        lease_token: str,
        session: Optional[AsyncSession] = None,
        lock: bool = False,
    ) -> None:
        """Ensure this exact unexpired claim remains authoritative."""
        now = datetime.now(timezone.utc)
        statement = select(GapRepairJob.id).where(
            GapRepairJob.id == job_id,
            GapRepairJob.status == GapRepairStatus.PROCESSING,
            GapRepairJob.worker_id == worker_id,
            GapRepairJob.lease_token == lease_token,
            GapRepairJob.lease_expires_at > now,
        )
        if lock:
            statement = statement.with_for_update()
        if session is not None:
            result = await session.execute(statement)
            if result.scalar_one_or_none() is None:
                raise LeaseLostError(f"Gap repair claim lost for job {job_id}")
            return
        async with self.session_factory() as owned_session:
            result = await owned_session.execute(statement)
            if result.scalar_one_or_none() is None:
                raise LeaseLostError(f"Gap repair claim lost for job {job_id}")

    async def _heartbeat_loop(
        self,
        job_id: int,
        worker_id: str,
        lease_duration: timedelta,
        sleep_interval: Optional[float] = None,
        *,
        lease_token: Optional[str] = None,
        ownership_lost: Optional[asyncio.Event] = None,
    ) -> None:
        """Renew an unexpired token-matching lease or fence this worker."""
        if sleep_interval is None:
            sleep_interval = max(0.5, lease_duration.total_seconds() / 2.0)
        if ownership_lost is None:
            ownership_lost = asyncio.Event()
        while not ownership_lost.is_set():
            try:
                await asyncio.sleep(sleep_interval)
            except asyncio.CancelledError:
                return
            now = datetime.now(timezone.utc)
            try:
                async with self.session_factory() as session:
                    async with session.begin():
                        conditions = [
                            GapRepairJob.id == job_id,
                            GapRepairJob.status == GapRepairStatus.PROCESSING,
                            GapRepairJob.worker_id == worker_id,
                            GapRepairJob.lease_expires_at > now,
                        ]
                        if lease_token is not None:
                            conditions.append(GapRepairJob.lease_token == lease_token)
                        result = await session.execute(
                            update(GapRepairJob)
                            .where(*conditions)
                            .values(lease_expires_at=now + lease_duration)
                        )
                        if result.rowcount != 1:
                            logger.warning("Gap worker %s lost job %s", worker_id, job_id)
                            ownership_lost.set()
                            return
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Gap heartbeat failed for job %s; fencing worker", job_id)
                ownership_lost.set()
                return

    async def _commit_owned_batch(
        self,
        *,
        asset_id: int,
        job_id: int,
        worker_id: str,
        lease_token: str,
        batch: list[dict[str, Any]],
        ownership_lost: asyncio.Event,
    ) -> None:
        """Fence a short canonical write transaction before committing a batch."""
        if ownership_lost.is_set():
            raise LeaseLostError(f"Gap repair claim lost for job {job_id}")
        async with self.session_factory() as session:
            async with session.begin():
                # Keep asset→job lock ordering consistent with scheduling, then
                # keep the job lock only across this bounded local transaction.
                await session.execute(
                    select(AssetRegistry.id)
                    .where(AssetRegistry.id == asset_id)
                    .with_for_update()
                )
                await self._assert_lease_owned(
                    job_id=job_id,
                    worker_id=worker_id,
                    lease_token=lease_token,
                    session=session,
                    lock=True,
                )
                if ownership_lost.is_set():
                    raise LeaseLostError(f"Gap repair claim lost for job {job_id}")
                ingestion = IngestionService(db_session=session)
                # This API is intentionally transactional: it validates, writes,
                # and only publishes sync coverage for verified raw candles.
                await ingestion.commit_batch_in_transaction(asset_id, batch)

    async def _execute_reconciliation(
        self,
        asset_id: int,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        binance_client: Optional[BinanceClient] = None,
        batch_size: int = 1000,
        *,
        job_id: Optional[int] = None,
        worker_id: Optional[str] = None,
        lease_token: Optional[str] = None,
        ownership_lost: Optional[asyncio.Event] = None,
    ) -> int:
        """Stream REST candles in bounded batches with no open DB network wait."""
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        client = binance_client or self.client
        owns_client = False
        if client is None:
            from app.connectors.binance import BinanceClient

            client = BinanceClient()
            owns_client = True
        total_candles = 0

        async def persist(batch: list[dict[str, Any]]) -> None:
            if not batch:
                return
            if job_id is None:
                # Inline/admin repair retains its public compatibility path; the
                # durable actor always supplies a fencing claim.
                async with self.session_factory() as session:
                    await IngestionService(db_session=session)._commit_batch(asset_id, batch)
                return
            assert worker_id is not None and lease_token is not None and ownership_lost is not None
            await self._commit_owned_batch(
                asset_id=asset_id,
                job_id=job_id,
                worker_id=worker_id,
                lease_token=lease_token,
                batch=batch,
                ownership_lost=ownership_lost,
            )

        async def run_stream(active_client: BinanceClient) -> None:
            nonlocal total_candles
            batch: list[dict[str, Any]] = []
            async for candle in active_client.get_klines(symbol, "1m", start_time, end_time):
                if ownership_lost is not None and ownership_lost.is_set():
                    raise LeaseLostError(f"Gap repair claim lost for job {job_id}")
                candle["asset_id"] = asset_id
                # REST is the authoritative reconciliation source.  Preserve its
                # receipt/event metadata so a correction is attributable rather
                # than looking like an anonymous duplicate.
                candle.setdefault("source", "binance_rest")
                candle.setdefault("source_event_time", candle.get("timestamp"))
                candle.setdefault("source_received_at", datetime.now(timezone.utc))
                batch.append(candle)
                total_candles += 1
                if len(batch) >= batch_size:
                    await persist(batch)
                    batch = []
            await persist(batch)

        if owns_client:
            async with client:
                await run_stream(client)
        else:
            await run_stream(client)
        return total_candles

    async def _mark_complete_and_schedule_cagg(
        self,
        *,
        job: GapRepairJob,
        worker_id: str,
        lease_token: str,
    ) -> bool:
        """Atomically schedule downstream work and complete only the current claim."""
        now = datetime.now(timezone.utc)
        # Gap-job windows derive from canonical candle timestamps and are
        # inclusive.  A final candle exactly on a CAGG boundary must refresh its
        # following bucket rather than silently leaving that aggregate stale.
        aligned_start, aligned_end = compute_cagg_bucket_alignment(
            job.start_time,
            job.end_time,
            end_inclusive=True,
        )
        async with self.session_factory() as session:
            async with session.begin():
                await session.execute(
                    select(AssetRegistry.id)
                    .where(AssetRegistry.id == job.asset_id)
                    .with_for_update()
                )
                await self._assert_lease_owned(
                    job_id=job.id,
                    worker_id=worker_id,
                    lease_token=lease_token,
                    session=session,
                    lock=True,
                )
                await schedule_cagg_refresh_job(session, aligned_start, aligned_end)
                result = await session.execute(
                    update(GapRepairJob)
                    .where(
                        GapRepairJob.id == job.id,
                        GapRepairJob.status == GapRepairStatus.PROCESSING,
                        GapRepairJob.worker_id == worker_id,
                        GapRepairJob.lease_token == lease_token,
                        GapRepairJob.lease_expires_at > now,
                    )
                    .values(
                        status=GapRepairStatus.COMPLETED,
                        error_message=None,
                        error_category=None,
                        lease_expires_at=None,
                        lease_token=None,
                        next_attempt_at=None,
                    )
                )
                return result.rowcount == 1

    async def _has_staged_rows(self, job: GapRepairJob) -> bool:
        """Whether unresolved coverage is waiting on the canonical merge worker."""
        from app.models.gap_staging_candles import GapStagingCandle

        async with self.session_factory() as session:
            result = await session.execute(
                select(GapStagingCandle.asset_id)
                .where(
                    GapStagingCandle.asset_id == job.asset_id,
                    GapStagingCandle.timestamp >= job.start_time,
                    GapStagingCandle.timestamp <= job.end_time,
                )
                .limit(1)
            )
            return result.scalar_one_or_none() is not None

    async def _mark_awaiting_merge_if_owned(
        self,
        *,
        job_id: int,
        worker_id: str,
        lease_token: str,
    ) -> bool:
        """Park staged historical work without retrying REST before promotion."""
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(GapRepairJob)
                    .where(
                        GapRepairJob.id == job_id,
                        GapRepairJob.status == GapRepairStatus.PROCESSING,
                        GapRepairJob.worker_id == worker_id,
                        GapRepairJob.lease_token == lease_token,
                        GapRepairJob.lease_expires_at > now,
                    )
                    .values(
                        status=GapRepairStatus.AWAITING_MERGE,
                        worker_id=None,
                        lease_token=None,
                        claimed_at=None,
                        lease_expires_at=None,
                        next_attempt_at=None,
                        error_message=None,
                        error_category=None,
                    )
                )
                return result.rowcount == 1

    async def _record_failure_if_owned(
        self,
        *,
        job_id: int,
        worker_id: str,
        lease_token: str,
        error: Exception,
    ) -> None:
        category, retryable = classify_error(error)
        now = datetime.now(timezone.utc)
        message = f"{type(error).__name__}: {error}\n{traceback.format_exc()}"
        async with self.session_factory() as session:
            async with session.begin():
                job = await session.get(GapRepairJob, job_id, with_for_update=True)
                if (
                    job is None
                    or job.status != GapRepairStatus.PROCESSING
                    or job.worker_id != worker_id
                    or job.lease_token != lease_token
                    or job.lease_expires_at is None
                    or job.lease_expires_at <= now
                ):
                    return
                job.retry_count += 1
                job.error_message = message
                job.error_category = category
                job.lease_expires_at = None
                job.lease_token = None
                job.claimed_at = None
                job.worker_id = None
                if retryable and job.retry_count <= job.max_retries:
                    job.status = GapRepairStatus.PENDING
                    job.next_attempt_at = now + self._retry_delay(job.retry_count)
                else:
                    job.status = GapRepairStatus.FAILED
                    job.next_attempt_at = None

    async def process_next_job(
        self,
        worker_id: Optional[str] = None,
        lease_duration: timedelta = timedelta(minutes=5),
        heartbeat_interval: Optional[float] = None,
        binance_client: Optional[BinanceClient] = None,
        batch_size: int = 1000,
        *,
        respect_retry_schedule: bool = False,
        raise_on_attempt_error: bool = True,
    ) -> bool:
        """Claim and process one job; no actor invocation drains unbounded work."""
        worker_id = worker_id or str(uuid.uuid4())
        job = await self.claim_job(
            worker_id=worker_id,
            lease_duration=lease_duration,
            respect_retry_schedule=respect_retry_schedule,
        )
        if job is None:
            return False
        assert job.lease_token is not None
        lease_token = job.lease_token
        ownership_lost = asyncio.Event()
        heartbeat = asyncio.create_task(
            self._heartbeat_loop(
                job.id,
                worker_id,
                lease_duration,
                heartbeat_interval,
                lease_token=lease_token,
                ownership_lost=ownership_lost,
            )
        )
        try:
            # A previous worker may have completed the canonical write before a
            # crash.  Never make a redundant Binance request in that case.
            gaps = await self.detect_gaps(job.asset_id, job.start_time, job.end_time)
            for gap_start, gap_end in gaps:
                if ownership_lost.is_set():
                    raise LeaseLostError(f"Gap repair claim lost for job {job.id}")
                await self._execute_reconciliation(
                    asset_id=job.asset_id,
                    symbol=job.symbol,
                    start_time=gap_start,
                    end_time=gap_end,
                    binance_client=binance_client,
                    batch_size=batch_size,
                    job_id=job.id,
                    worker_id=worker_id,
                    lease_token=lease_token,
                    ownership_lost=ownership_lost,
                )
            remaining = await self.detect_gaps(job.asset_id, job.start_time, job.end_time)
            if remaining:
                # Historical rows intentionally do not make a sync-range claim.
                # Parking the job here prevents every maintenance wake-up from
                # downloading the same REST range until promotion completes.
                if await self._has_staged_rows(job):
                    if not await self._mark_awaiting_merge_if_owned(
                        job_id=job.id,
                        worker_id=worker_id,
                        lease_token=lease_token,
                    ):
                        raise LeaseLostError(f"Gap repair claim lost for job {job.id}")
                    logger.info("Gap repair job %s awaits historical merge", job.id)
                    return True
                raise IncompleteCoverageError(
                    f"canonical coverage remains missing for job {job.id}: {remaining[:3]}"
                )
            if ownership_lost.is_set() or not await self._mark_complete_and_schedule_cagg(
                job=job,
                worker_id=worker_id,
                lease_token=lease_token,
            ):
                raise LeaseLostError(f"Gap repair claim lost for job {job.id}")
            logger.info("Completed gap repair job %s", job.id)
        except LeaseLostError:
            logger.warning("Stopped stale gap repair worker for job %s", job.id)
        except Exception as exc:
            logger.exception("Gap repair job %s failed", job.id)
            await self._record_failure_if_owned(
                job_id=job.id,
                worker_id=worker_id,
                lease_token=lease_token,
                error=exc,
            )
            if raise_on_attempt_error:
                raise
        finally:
            heartbeat.cancel()
            try:
                await heartbeat
            except asyncio.CancelledError:
                pass
        return True

    async def repair_gap_inline(
        self,
        asset_id: int,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        binance_client: Optional[BinanceClient] = None,
        batch_size: int = 1000,
    ) -> int:
        """Compatibility path for explicit admin repair; workers use durable jobs."""
        gaps = await self.detect_gaps(asset_id, start_time, end_time)
        repaired = 0
        for gap_start, gap_end in gaps:
            repaired += await self._execute_reconciliation(
                asset_id=asset_id,
                symbol=symbol,
                start_time=gap_start,
                end_time=gap_end,
                binance_client=binance_client,
                batch_size=batch_size,
            )
            async with self.session_factory() as session:
                async with session.begin():
                    await schedule_cagg_refresh_job(
                        session,
                        gap_start,
                        gap_end,
                        end_inclusive=True,
                    )
        return repaired

    async def scan_and_schedule_active_assets(
        self,
        lookback_window: timedelta = timedelta(hours=24),
        *,
        max_assets: int = 100,
        max_jobs: int = 500,
        page_size: int = 25,
    ) -> List[GapRepairJob]:
        """Page active Binance assets and cap the work created per scan cycle."""
        if lookback_window.total_seconds() <= 0:
            raise ValueError("lookback_window must be positive")
        if min(max_assets, max_jobs, page_size) <= 0:
            raise ValueError("scan bounds must be positive")
        now = datetime.now(timezone.utc)
        start_time = now - lookback_window
        scheduled: list[GapRepairJob] = []
        last_asset_id = 0
        inspected = 0

        while inspected < max_assets and len(scheduled) < max_jobs:
            limit = min(page_size, max_assets - inspected)
            async with self.session_factory() as session:
                assets = (
                    await session.execute(
                        select(AssetRegistry)
                        .where(
                            AssetRegistry.is_active.is_(True),
                            AssetRegistry.exchange == "BINANCE",
                            AssetRegistry.id > last_asset_id,
                        )
                        .order_by(AssetRegistry.id.asc())
                        .limit(limit)
                    )
                ).scalars().all()
            if not assets:
                break
            last_asset_id = assets[-1].id
            inspected += len(assets)
            for asset in assets:
                if len(scheduled) >= max_jobs:
                    break
                gaps = await self.detect_gaps(asset.id, start_time, now)
                for gap_start, gap_end in gaps:
                    if len(scheduled) >= max_jobs:
                        break
                    jobs = await self.schedule_repair_jobs(
                        asset_id=asset.id,
                        symbol=asset.symbol,
                        start_time=gap_start,
                        end_time=gap_end,
                    )
                    scheduled.extend(jobs[: max_jobs - len(scheduled)])
        return scheduled
