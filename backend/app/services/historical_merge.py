"""Durable promotion of staged historical candles into canonical raw storage."""

from __future__ import annotations

import asyncio
import logging
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from itertools import groupby
from operator import attrgetter
from typing import Optional

from sqlalchemy import and_, delete, func, or_, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.gap_staging_candles import GapStagingCandle
from app.models.gap_repair_jobs import GapRepairJob, GapRepairStatus
from app.models.historical_merge_jobs import HistoricalMergeJob, HistoricalMergeStatus
from app.services.cagg_refresh import LeaseLostError, schedule_cagg_refresh_job
from app.services.ingestion import IngestionService

logger = logging.getLogger(__name__)


class HistoricalMergeService:
    """Fenced per-day promotion worker with bounded page memory.

    Staging is deliberately not canonical coverage.  Every page is copied to raw,
    proven through ``commit_raw_batch_in_transaction``, and only then removed in
    the same transaction.  A crash leaves either the staging page intact or raw
    data plus a retryable job; it cannot produce false ``sync_ranges`` coverage.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    @staticmethod
    def _normalise_timestamp(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _retry_delay(retry_count: int) -> timedelta:
        return timedelta(seconds=min(3600, 30 * (2 ** max(0, retry_count - 1))))

    async def schedule_staged_days(self, *, max_days: int = 32) -> int:
        """Coalesce currently staged days into a bounded set of durable jobs."""
        if max_days <= 0:
            raise ValueError("max_days must be positive")
        async with self.session_factory() as session:
            async with session.begin():
                days = (
                    await session.execute(
                        select(func.date_trunc("day", GapStagingCandle.timestamp).label("day_bucket"))
                        .group_by("day_bucket")
                        .order_by("day_bucket")
                        .limit(max_days)
                    )
                ).scalars().all()
                for day in days:
                    day = self._normalise_timestamp(day)
                    # A completed/failed day is reopened only when new staging
                    # actually exists for it.  PENDING/PROCESSING rows retain
                    # their claim/retry state.
                    statement = insert(HistoricalMergeJob).values(
                        day_bucket=day,
                        status=HistoricalMergeStatus.PENDING,
                    )
                    statement = statement.on_conflict_do_update(
                        index_elements=[HistoricalMergeJob.day_bucket],
                        set_={
                            "status": HistoricalMergeStatus.PENDING,
                            "worker_id": None,
                            "lease_token": None,
                            "claimed_at": None,
                            "lease_expires_at": None,
                            "next_attempt_at": None,
                            "error_message": None,
                            "error_category": None,
                        },
                        where=HistoricalMergeJob.status.in_(
                            (HistoricalMergeStatus.COMPLETED, HistoricalMergeStatus.FAILED)
                        ),
                    )
                    await session.execute(statement)
                return len(days)

    async def claim_job(
        self,
        *,
        worker_id: str,
        lease_duration: timedelta,
        respect_retry_schedule: bool = True,
    ) -> Optional[HistoricalMergeJob]:
        if lease_duration.total_seconds() <= 0:
            raise ValueError("lease_duration must be positive")
        now = datetime.now(timezone.utc)
        pending = HistoricalMergeJob.status == HistoricalMergeStatus.PENDING
        if respect_retry_schedule:
            pending = and_(
                pending,
                or_(
                    HistoricalMergeJob.next_attempt_at.is_(None),
                    HistoricalMergeJob.next_attempt_at <= now,
                ),
            )
        async with self.session_factory() as session:
            async with session.begin():
                job = (
                    await session.execute(
                        select(HistoricalMergeJob)
                        .where(
                            or_(
                                pending,
                                and_(
                                    HistoricalMergeJob.status == HistoricalMergeStatus.PROCESSING,
                                    HistoricalMergeJob.lease_expires_at < now,
                                ),
                            )
                        )
                        .order_by(HistoricalMergeJob.created_at.asc(), HistoricalMergeJob.id.asc())
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                ).scalars().first()
                if job is None:
                    return None
                job.status = HistoricalMergeStatus.PROCESSING
                job.worker_id = worker_id
                job.lease_token = uuid.uuid4().hex
                job.claimed_at = now
                job.lease_expires_at = now + lease_duration
                job.next_attempt_at = None
                job_id = job.id
            return await session.get(HistoricalMergeJob, job_id)

    async def _assert_owned(
        self,
        *,
        job_id: int,
        worker_id: str,
        lease_token: str,
        session: Optional[AsyncSession] = None,
        lock: bool = False,
    ) -> None:
        statement = select(HistoricalMergeJob.id).where(
            HistoricalMergeJob.id == job_id,
            HistoricalMergeJob.status == HistoricalMergeStatus.PROCESSING,
            HistoricalMergeJob.worker_id == worker_id,
            HistoricalMergeJob.lease_token == lease_token,
            HistoricalMergeJob.lease_expires_at > datetime.now(timezone.utc),
        )
        if lock:
            statement = statement.with_for_update()
        if session is not None:
            result = await session.execute(statement)
        else:
            async with self.session_factory() as owned_session:
                result = await owned_session.execute(statement)
        if result.scalar_one_or_none() is None:
            raise LeaseLostError(f"Historical merge claim lost for job {job_id}")

    async def _heartbeat_loop(
        self,
        *,
        job_id: int,
        worker_id: str,
        lease_token: str,
        lease_duration: timedelta,
        ownership_lost: asyncio.Event,
        sleep_interval: Optional[float] = None,
    ) -> None:
        if sleep_interval is None:
            sleep_interval = max(0.5, lease_duration.total_seconds() / 2.0)
        while not ownership_lost.is_set():
            try:
                await asyncio.sleep(sleep_interval)
            except asyncio.CancelledError:
                return
            now = datetime.now(timezone.utc)
            try:
                async with self.session_factory() as session:
                    async with session.begin():
                        result = await session.execute(
                            update(HistoricalMergeJob)
                            .where(
                                HistoricalMergeJob.id == job_id,
                                HistoricalMergeJob.status == HistoricalMergeStatus.PROCESSING,
                                HistoricalMergeJob.worker_id == worker_id,
                                HistoricalMergeJob.lease_token == lease_token,
                                HistoricalMergeJob.lease_expires_at > now,
                            )
                            .values(lease_expires_at=now + lease_duration)
                        )
                        if result.rowcount != 1:
                            logger.warning("Historical merge worker %s lost job %s", worker_id, job_id)
                            ownership_lost.set()
                            return
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Historical merge heartbeat failed for job %s", job_id)
                ownership_lost.set()
                return

    async def _decompress_day(
        self,
        *,
        day: datetime,
        next_day: datetime,
        job_id: int,
        worker_id: str,
        lease_token: str,
        ownership_lost: asyncio.Event,
    ) -> None:
        if ownership_lost.is_set():
            raise LeaseLostError(f"Historical merge claim lost for job {job_id}")
        await self._assert_owned(job_id=job_id, worker_id=worker_id, lease_token=lease_token)
        async with self.session_factory() as session:
            # Timescale's if_compressed flag makes this safe when the selected
            # chunk is already writable or when no historical chunk exists.
            await session.execute(
                text(
                    """
                    SELECT decompress_chunk(c, if_compressed => true)
                    FROM show_chunks('raw_1m_candles', newer_than => :day, older_than => :next_day) c
                    """
                ),
                {"day": day, "next_day": next_day},
            )
            await session.commit()

    async def _merge_one_page(
        self,
        *,
        day: datetime,
        next_day: datetime,
        page_size: int,
        job_id: int,
        worker_id: str,
        lease_token: str,
        ownership_lost: asyncio.Event,
    ) -> int:
        """Copy/delete one bounded staging page under the current claim token."""
        if ownership_lost.is_set():
            raise LeaseLostError(f"Historical merge claim lost for job {job_id}")
        async with self.session_factory() as session:
            async with session.begin():
                await self._assert_owned(
                    job_id=job_id,
                    worker_id=worker_id,
                    lease_token=lease_token,
                    session=session,
                    lock=True,
                )
                rows = (
                    await session.execute(
                        select(GapStagingCandle)
                        .where(
                            GapStagingCandle.timestamp >= day,
                            GapStagingCandle.timestamp < next_day,
                        )
                        .order_by(GapStagingCandle.asset_id.asc(), GapStagingCandle.timestamp.asc())
                        .limit(page_size)
                        .with_for_update(skip_locked=True)
                    )
                ).scalars().all()
                if not rows:
                    return 0
                if ownership_lost.is_set():
                    raise LeaseLostError(f"Historical merge claim lost for job {job_id}")

                # Grouping keeps each ingestion call a sorted one-asset block,
                # while ``page_size`` bounds the total rows retained in memory.
                rows.sort(key=attrgetter("asset_id", "timestamp"))
                ingestion = IngestionService(session)
                for asset_id, asset_rows_iter in groupby(rows, key=attrgetter("asset_id")):
                    asset_rows = list(asset_rows_iter)
                    candles = [
                        {
                            "asset_id": row.asset_id,
                            "timestamp": row.timestamp,
                            "open": row.open,
                            "high": row.high,
                            "low": row.low,
                            "close": row.close,
                            "volume": row.volume,
                            "source": row.source,
                            "source_event_time": row.source_event_time,
                            "source_received_at": row.source_received_at,
                        }
                        for row in asset_rows
                    ]
                    await ingestion.commit_raw_batch_in_transaction(asset_id, candles)

                keys = [(row.asset_id, row.timestamp) for row in rows]
                await session.execute(
                    delete(GapStagingCandle).where(
                        tuple_(GapStagingCandle.asset_id, GapStagingCandle.timestamp).in_(keys)
                    )
                )
                return len(rows)

    async def _recompress_day(
        self,
        *,
        day: datetime,
        next_day: datetime,
        job_id: int,
        worker_id: str,
        lease_token: str,
        ownership_lost: asyncio.Event,
    ) -> None:
        if ownership_lost.is_set():
            raise LeaseLostError(f"Historical merge claim lost for job {job_id}")
        await self._assert_owned(job_id=job_id, worker_id=worker_id, lease_token=lease_token)
        async with self.session_factory() as session:
            await session.execute(
                text(
                    """
                    SELECT compress_chunk(c, if_not_compressed => true)
                    FROM show_chunks('raw_1m_candles', newer_than => :day, older_than => :next_day) c
                    """
                ),
                {"day": day, "next_day": next_day},
            )
            await session.commit()

    async def _complete_if_owned(
        self,
        *,
        job_id: int,
        worker_id: str,
        lease_token: str,
        day: datetime,
        next_day: datetime,
    ) -> bool:
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            async with session.begin():
                await self._assert_owned(
                    job_id=job_id,
                    worker_id=worker_id,
                    lease_token=lease_token,
                    session=session,
                    lock=True,
                )
                remaining = await session.execute(
                    select(GapStagingCandle.asset_id)
                    .where(
                        GapStagingCandle.timestamp >= day,
                        GapStagingCandle.timestamp < next_day,
                    )
                    .limit(1)
                )
                if remaining.scalar_one_or_none() is not None:
                    return False
                await schedule_cagg_refresh_job(session, day, next_day, timeframe="1d")
                result = await session.execute(
                    update(HistoricalMergeJob)
                    .where(
                        HistoricalMergeJob.id == job_id,
                        HistoricalMergeJob.status == HistoricalMergeStatus.PROCESSING,
                        HistoricalMergeJob.worker_id == worker_id,
                        HistoricalMergeJob.lease_token == lease_token,
                        HistoricalMergeJob.lease_expires_at > now,
                    )
                    .values(
                        status=HistoricalMergeStatus.COMPLETED,
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
        now = datetime.now(timezone.utc)
        message = f"{type(error).__name__}: {error}\n{traceback.format_exc()}"
        async with self.session_factory() as session:
            async with session.begin():
                job = await session.get(HistoricalMergeJob, job_id, with_for_update=True)
                if (
                    job is None
                    or job.status != HistoricalMergeStatus.PROCESSING
                    or job.worker_id != worker_id
                    or job.lease_token != lease_token
                    or job.lease_expires_at is None
                    or job.lease_expires_at <= now
                ):
                    return
                job.retry_count += 1
                job.error_message = message
                job.error_category = type(error).__name__
                job.worker_id = None
                job.lease_token = None
                job.claimed_at = None
                job.lease_expires_at = None
                if job.retry_count <= job.max_retries:
                    job.status = HistoricalMergeStatus.PENDING
                    job.next_attempt_at = now + self._retry_delay(job.retry_count)
                else:
                    job.status = HistoricalMergeStatus.FAILED
                    job.next_attempt_at = None

    async def _release_gap_jobs_waiting_for_merge(self, *, max_jobs: int = 100) -> int:
        """Requeue only staged-gap jobs whose entire staging window is drained.

        A requeued job still re-verifies raw coverage before it can complete.  If
        REST was partial, that verification safely causes a bounded re-repair;
        if all staged rows became raw, it completes without a second download.
        """
        if max_jobs <= 0:
            raise ValueError("max_jobs must be positive")
        released = 0
        async with self.session_factory() as session:
            async with session.begin():
                jobs = (
                    await session.execute(
                        select(GapRepairJob)
                        .where(GapRepairJob.status == GapRepairStatus.AWAITING_MERGE)
                        .order_by(GapRepairJob.created_at.asc(), GapRepairJob.id.asc())
                        .limit(max_jobs)
                        .with_for_update(skip_locked=True)
                    )
                ).scalars().all()
                for job in jobs:
                    staged = await session.execute(
                        select(GapStagingCandle.asset_id)
                        .where(
                            GapStagingCandle.asset_id == job.asset_id,
                            GapStagingCandle.timestamp >= job.start_time,
                            GapStagingCandle.timestamp <= job.end_time,
                        )
                        .limit(1)
                    )
                    if staged.scalar_one_or_none() is not None:
                        continue
                    job.status = GapRepairStatus.PENDING
                    job.next_attempt_at = datetime.now(timezone.utc)
                    released += 1
        return released

    async def process_next_job(
        self,
        *,
        worker_id: Optional[str] = None,
        lease_duration: timedelta = timedelta(minutes=5),
        heartbeat_interval: Optional[float] = None,
        page_size: int = 1000,
        respect_retry_schedule: bool = True,
        raise_on_attempt_error: bool = True,
    ) -> bool:
        """Claim and promote at most one day; page memory is strictly bounded."""
        if page_size <= 0:
            raise ValueError("page_size must be positive")
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
        day = self._normalise_timestamp(job.day_bucket)
        next_day = day + timedelta(days=1)
        ownership_lost = asyncio.Event()
        heartbeat = asyncio.create_task(
            self._heartbeat_loop(
                job_id=job.id,
                worker_id=worker_id,
                lease_token=lease_token,
                lease_duration=lease_duration,
                ownership_lost=ownership_lost,
                sleep_interval=heartbeat_interval,
            )
        )
        try:
            await self._decompress_day(
                day=day,
                next_day=next_day,
                job_id=job.id,
                worker_id=worker_id,
                lease_token=lease_token,
                ownership_lost=ownership_lost,
            )
            while True:
                if ownership_lost.is_set():
                    raise LeaseLostError(f"Historical merge claim lost for job {job.id}")
                page_count = await self._merge_one_page(
                    day=day,
                    next_day=next_day,
                    page_size=page_size,
                    job_id=job.id,
                    worker_id=worker_id,
                    lease_token=lease_token,
                    ownership_lost=ownership_lost,
                )
                if page_count == 0:
                    break
            await self._recompress_day(
                day=day,
                next_day=next_day,
                job_id=job.id,
                worker_id=worker_id,
                lease_token=lease_token,
                ownership_lost=ownership_lost,
            )
            if ownership_lost.is_set() or not await self._complete_if_owned(
                job_id=job.id,
                worker_id=worker_id,
                lease_token=lease_token,
                day=day,
                next_day=next_day,
            ):
                raise LeaseLostError(f"Historical merge claim lost for job {job.id}")
            await self._release_gap_jobs_waiting_for_merge()
            logger.info("Completed historical merge job %s for %s", job.id, day.date())
        except LeaseLostError:
            logger.warning("Stopped stale historical merge worker for job %s", job.id)
        except Exception as exc:
            logger.exception("Historical merge job %s failed", job.id)
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

    async def process_available_jobs(
        self,
        *,
        max_jobs: int = 8,
        max_schedule_days: int = 32,
        **process_kwargs: object,
    ) -> int:
        """Schedule and process a bounded number of daily jobs per actor run."""
        if max_jobs <= 0 or max_schedule_days <= 0:
            raise ValueError("historical merge bounds must be positive")
        await self.schedule_staged_days(max_days=max_schedule_days)
        processed = 0
        while processed < max_jobs:
            claimed = await self.process_next_job(**process_kwargs)
            if not claimed:
                break
            processed += 1
        return processed
