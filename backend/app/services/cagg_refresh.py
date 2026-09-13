"""Durable, fenced refresh work for Timescale continuous aggregates.

The refresh procedure itself is idempotent, but it is expensive.  The job table
therefore owns scheduling, retry backoff, and a per-claim fencing token so a
stale worker cannot overwrite a newer worker's state or start additional work
once it notices lost ownership.
"""

from __future__ import annotations

import asyncio
import logging
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, Sequence, Tuple

from sqlalchemy import and_, or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.cagg_refresh_jobs import CaggRefreshJob, RefreshStatus

logger = logging.getLogger(__name__)


class LeaseLostError(RuntimeError):
    """Raised when a worker's durable claim is no longer current."""


class CaggRefreshService:
    """Claim and execute one fenced CAGG refresh job at a time."""

    # This transaction-scoped lock serializes scheduling only.  It is never held
    # while TimescaleDB is refreshing an aggregate.
    _SCHEDULER_ADVISORY_LOCK = 731_415_021
    _ACTIVE_STATUSES: tuple[RefreshStatus, ...] = (
        RefreshStatus.PENDING,
        RefreshStatus.PROCESSING,
    )

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory
        self.cagg_names: Sequence[str] = (
            "candles_5m",
            "candles_15m",
            "candles_1h",
            "candles_4h",
            "candles_1d",
        )

    @staticmethod
    def _retry_delay(retry_count: int) -> timedelta:
        """Bound exponential backoff for durable job retries."""
        return timedelta(seconds=min(3600, 30 * (2 ** max(0, retry_count - 1))))

    @staticmethod
    def _normalise_timestamp(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _uncovered_intervals(
        start: datetime,
        end: datetime,
        existing: Iterable[CaggRefreshJob],
    ) -> list[tuple[datetime, datetime]]:
        """Return portions of ``[start, end)`` not already actively scheduled."""
        intervals = sorted(
            (
                max(start, CaggRefreshService._normalise_timestamp(job.window_start)),
                min(end, CaggRefreshService._normalise_timestamp(job.window_end)),
            )
            for job in existing
            if job.window_start < end and job.window_end > start
        )
        cursor = start
        uncovered: list[tuple[datetime, datetime]] = []
        for existing_start, existing_end in intervals:
            if existing_end <= cursor:
                continue
            if existing_start > cursor:
                uncovered.append((cursor, existing_start))
            cursor = max(cursor, existing_end)
            if cursor >= end:
                break
        if cursor < end:
            uncovered.append((cursor, end))
        return uncovered

    async def claim_next_job(
        self,
        *,
        worker_id: str,
        lease_duration: timedelta,
        respect_retry_schedule: bool = True,
    ) -> Optional[CaggRefreshJob]:
        """Atomically claim one ready or expired job and issue a fresh token."""
        now = datetime.now(timezone.utc)
        ready_pending = CaggRefreshJob.status == RefreshStatus.PENDING
        if respect_retry_schedule:
            ready_pending = and_(
                ready_pending,
                or_(
                    CaggRefreshJob.next_attempt_at.is_(None),
                    CaggRefreshJob.next_attempt_at <= now,
                ),
            )

        async with self.session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    select(CaggRefreshJob)
                    .where(
                        or_(
                            ready_pending,
                            and_(
                                CaggRefreshJob.status == RefreshStatus.PROCESSING,
                                CaggRefreshJob.lease_expires_at < now,
                            ),
                        )
                    )
                    .order_by(CaggRefreshJob.created_at.asc(), CaggRefreshJob.id.asc())
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                job = result.scalars().first()
                if job is None:
                    return None

                job.status = RefreshStatus.PROCESSING
                job.worker_id = worker_id
                job.lease_token = uuid.uuid4().hex
                job.claimed_at = now
                job.lease_expires_at = now + lease_duration
                job.next_attempt_at = None
                job_id = job.id

            # A separate read gives callers a fully materialized object after the
            # short claim transaction has committed.
            return await session.get(CaggRefreshJob, job_id)

    async def _assert_lease_owned(
        self,
        job_id: int,
        worker_id: str,
        lease_token: str,
    ) -> None:
        """Fail closed when a claim expired, changed owner, or changed token."""
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            result = await session.execute(
                select(CaggRefreshJob.id).where(
                    CaggRefreshJob.id == job_id,
                    CaggRefreshJob.status == RefreshStatus.PROCESSING,
                    CaggRefreshJob.worker_id == worker_id,
                    CaggRefreshJob.lease_token == lease_token,
                    CaggRefreshJob.lease_expires_at > now,
                )
            )
            if result.scalar_one_or_none() is None:
                raise LeaseLostError(f"CAGG refresh claim lost for job {job_id}")

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
        """Renew only a currently unexpired, token-matching claim.

        On any database error the worker fails closed.  Continuing a refresh after
        a missed heartbeat is worse than letting a new owner recover the work.
        """
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
                            CaggRefreshJob.id == job_id,
                            CaggRefreshJob.status == RefreshStatus.PROCESSING,
                            CaggRefreshJob.worker_id == worker_id,
                            CaggRefreshJob.lease_expires_at > now,
                        ]
                        if lease_token is not None:
                            conditions.append(CaggRefreshJob.lease_token == lease_token)
                        result = await session.execute(
                            update(CaggRefreshJob)
                            .where(*conditions)
                            .values(lease_expires_at=now + lease_duration)
                        )
                        if result.rowcount != 1:
                            logger.warning("CAGG worker %s lost job %s", worker_id, job_id)
                            ownership_lost.set()
                            return
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("CAGG heartbeat failed for job %s; fencing worker", job_id)
                ownership_lost.set()
                return

    async def _refresh_one(
        self,
        *,
        cagg_name: str,
        window_start: datetime,
        window_end: datetime,
        job_id: int,
        worker_id: str,
        lease_token: str,
        ownership_lost: asyncio.Event,
    ) -> None:
        if ownership_lost.is_set():
            raise LeaseLostError(f"CAGG refresh claim lost for job {job_id}")
        await self._assert_lease_owned(job_id, worker_id, lease_token)

        # The aggregate identifier is selected from this static allowlist, not
        # caller input.  Timestamp values remain bound parameters.
        if cagg_name not in self.cagg_names:
            raise ValueError(f"Unknown continuous aggregate {cagg_name!r}")
        statement = text(
            f"CALL refresh_continuous_aggregate('{cagg_name}', :window_start, :window_end)"
        )

        async with self.session_factory() as session:
            execute_task = asyncio.create_task(
                session.execute(
                    statement,
                    {"window_start": window_start, "window_end": window_end},
                )
            )
            lost_task = asyncio.create_task(ownership_lost.wait())
            try:
                done, _ = await asyncio.wait(
                    {execute_task, lost_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if lost_task in done and ownership_lost.is_set():
                    execute_task.cancel()
                    try:
                        await execute_task
                    except asyncio.CancelledError:
                        pass
                    raise LeaseLostError(f"CAGG refresh claim lost for job {job_id}")
                await execute_task
                await session.commit()
            finally:
                lost_task.cancel()
                try:
                    await lost_task
                except asyncio.CancelledError:
                    pass
                if not execute_task.done():
                    execute_task.cancel()
                    try:
                        await execute_task
                    except asyncio.CancelledError:
                        pass

    async def _complete_if_owned(
        self, job_id: int, worker_id: str, lease_token: str
    ) -> bool:
        now = datetime.now(timezone.utc)
        async with self.session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(CaggRefreshJob)
                    .where(
                        CaggRefreshJob.id == job_id,
                        CaggRefreshJob.status == RefreshStatus.PROCESSING,
                        CaggRefreshJob.worker_id == worker_id,
                        CaggRefreshJob.lease_token == lease_token,
                        CaggRefreshJob.lease_expires_at > now,
                    )
                    .values(
                        status=RefreshStatus.COMPLETED,
                        error_message=None,
                        error_category=None,
                        lease_expires_at=None,
                        lease_token=None,
                        next_attempt_at=None,
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
        """Persist bounded exponential retry state without a broker retry storm."""
        now = datetime.now(timezone.utc)
        message = f"{type(error).__name__}: {error}\n{traceback.format_exc()}"
        async with self.session_factory() as session:
            async with session.begin():
                job = await session.get(CaggRefreshJob, job_id, with_for_update=True)
                if (
                    job is None
                    or job.status != RefreshStatus.PROCESSING
                    or job.worker_id != worker_id
                    or job.lease_token != lease_token
                    or job.lease_expires_at is None
                    or job.lease_expires_at <= now
                ):
                    return
                job.retry_count += 1
                job.error_message = message
                job.error_category = type(error).__name__
                job.lease_expires_at = None
                job.lease_token = None
                job.claimed_at = None
                job.worker_id = None
                if job.retry_count <= job.max_retries:
                    job.status = RefreshStatus.PENDING
                    job.next_attempt_at = now + self._retry_delay(job.retry_count)
                else:
                    job.status = RefreshStatus.FAILED
                    job.next_attempt_at = None

    async def process_pending_jobs(
        self,
        lease_duration: timedelta = timedelta(minutes=5),
        heartbeat_interval: Optional[float] = None,
        *,
        worker_id: Optional[str] = None,
        respect_retry_schedule: bool = True,
        raise_on_attempt_error: bool = True,
    ) -> bool:
        """Claim and process at most one CAGG refresh job.

        A worker loop supplies ``respect_retry_schedule=True``.  The explicit
        switch exists for deterministic operator/test recovery, not for normal
        dispatch.
        """
        if lease_duration.total_seconds() <= 0:
            raise ValueError("lease_duration must be positive")
        worker_id = worker_id or str(uuid.uuid4())
        job = await self.claim_next_job(
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
            for cagg_name in self.cagg_names:
                await self._refresh_one(
                    cagg_name=cagg_name,
                    window_start=self._normalise_timestamp(job.window_start),
                    window_end=self._normalise_timestamp(job.window_end),
                    job_id=job.id,
                    worker_id=worker_id,
                    lease_token=lease_token,
                    ownership_lost=ownership_lost,
                )
            if ownership_lost.is_set() or not await self._complete_if_owned(
                job.id, worker_id, lease_token
            ):
                raise LeaseLostError(f"CAGG refresh claim lost for job {job.id}")
            logger.info("Completed CAGG refresh job %s", job.id)
        except LeaseLostError:
            # Do not let a stale actor overwrite/requeue a newer owner's job.
            logger.warning("Stopped stale CAGG refresh worker for job %s", job.id)
        except Exception as exc:
            logger.exception("CAGG refresh job %s failed", job.id)
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


def compute_cagg_bucket_alignment(
    start_time: datetime,
    end_time: datetime,
    timeframe: Optional[str] = None,
    *,
    end_inclusive: bool = False,
) -> Tuple[datetime, datetime]:
    """Align a refresh range to exact UTC bucket boundaries.

    CAGG procedures use a half-open refresh window.  Most scheduler callers
    therefore pass ``end_inclusive=False`` (the default).  Canonical candle
    ranges, however, are inclusive at both ends: a repair ending exactly on a
    bucket boundary still changed the candle beginning at that boundary.  Those
    callers must opt in so the affected bucket is not silently omitted.
    """
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)
    else:
        start_time = start_time.astimezone(timezone.utc)
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)
    else:
        end_time = end_time.astimezone(timezone.utc)
    if end_inclusive:
        if start_time > end_time:
            raise ValueError("inclusive CAGG refresh window must have start_time <= end_time")
        # Advance only the alignment calculation.  The returned value remains
        # an exact bucket boundary; this is not a microsecond refresh request.
        end_time += timedelta(microseconds=1)
    elif start_time >= end_time:
        raise ValueError("CAGG refresh window must have start_time < end_time")

    if timeframe == "5m":
        minutes = 5
    elif timeframe == "15m":
        minutes = 15
    elif timeframe == "1h":
        aligned_start = start_time.replace(minute=0, second=0, microsecond=0)
        aligned_end = end_time.replace(minute=0, second=0, microsecond=0)
        if aligned_end < end_time:
            aligned_end += timedelta(hours=1)
        return aligned_start, aligned_end
    elif timeframe == "4h":
        aligned_start = start_time.replace(
            hour=(start_time.hour // 4) * 4, minute=0, second=0, microsecond=0
        )
        aligned_end = end_time.replace(
            hour=(end_time.hour // 4) * 4, minute=0, second=0, microsecond=0
        )
        if aligned_end < end_time:
            aligned_end += timedelta(hours=4)
        return aligned_start, aligned_end
    else:
        # None deliberately covers the union of all CAGGs.  A day boundary is
        # also the safe alignment for historical promotion.
        aligned_start = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        aligned_end = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
        if aligned_end < end_time:
            aligned_end += timedelta(days=1)
        return aligned_start, aligned_end

    aligned_start = start_time.replace(
        minute=(start_time.minute // minutes) * minutes, second=0, microsecond=0
    )
    aligned_end = end_time.replace(
        minute=(end_time.minute // minutes) * minutes, second=0, microsecond=0
    )
    if aligned_end < end_time:
        aligned_end += timedelta(minutes=minutes)
    return aligned_start, aligned_end


async def schedule_cagg_refresh_jobs(
    db: AsyncSession,
    start_time: datetime,
    end_time: datetime,
    timeframe: Optional[str] = None,
    *,
    end_inclusive: bool = False,
) -> list[CaggRefreshJob]:
    """Schedule only uncovered portions of an aligned CAGG refresh window.

    The caller must already be inside a short database transaction.  An advisory
    transaction lock serializes scheduler races; a database exclusion constraint
    added by the worker hardening migration is the final backstop.
    """
    aligned_start, aligned_end = compute_cagg_bucket_alignment(
        start_time,
        end_time,
        timeframe,
        end_inclusive=end_inclusive,
    )
    await db.execute(
        text("SELECT pg_advisory_xact_lock(:lock_key)"),
        {"lock_key": CaggRefreshService._SCHEDULER_ADVISORY_LOCK},
    )
    active = (
        await db.execute(
            select(CaggRefreshJob)
            .where(
                CaggRefreshJob.status.in_(CaggRefreshService._ACTIVE_STATUSES),
                CaggRefreshJob.window_start < aligned_end,
                CaggRefreshJob.window_end > aligned_start,
            )
            .order_by(CaggRefreshJob.window_start.asc(), CaggRefreshJob.id.asc())
            .with_for_update()
        )
    ).scalars().all()

    uncovered = CaggRefreshService._uncovered_intervals(aligned_start, aligned_end, active)
    created: list[CaggRefreshJob] = []
    for window_start, window_end in uncovered:
        job = CaggRefreshJob(
            window_start=window_start,
            window_end=window_end,
            status=RefreshStatus.PENDING,
        )
        db.add(job)
        created.append(job)
    await db.flush()
    return created or active[:1]


async def schedule_cagg_refresh_job(
    db: AsyncSession,
    start_time: datetime,
    end_time: datetime,
    timeframe: Optional[str] = None,
    *,
    end_inclusive: bool = False,
) -> CaggRefreshJob:
    """Compatibility wrapper returning the created or already-active job."""
    jobs = await schedule_cagg_refresh_jobs(
        db,
        start_time,
        end_time,
        timeframe,
        end_inclusive=end_inclusive,
    )
    if not jobs:
        raise RuntimeError("CAGG scheduler did not produce a job")
    return jobs[0]
