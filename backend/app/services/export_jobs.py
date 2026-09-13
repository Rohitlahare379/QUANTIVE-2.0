"""Durable, fenced lifecycle for asynchronous export jobs.

The Parquet/S3 work itself belongs in the worker, but ownership and terminal
state transitions belong at a database boundary.  A status flag is not enough:
after a worker pauses beyond its lease, a later claimant must be able to finish
without the stale worker overwriting the result.
"""

from __future__ import annotations

import asyncio
import logging
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.export_jobs import ExportJob, ExportStatus


logger = logging.getLogger(__name__)


class ExportLeaseLostError(RuntimeError):
    """A worker tried to continue after its export claim was fenced."""


class ExportJobService:
    """Claim and transition export jobs with a per-claim fencing token."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    @staticmethod
    def retry_delay(attempt_count: int) -> timedelta:
        """Bound retry delay so a failing object-store dependency cannot hot-loop."""
        return timedelta(seconds=min(3600, 30 * (2 ** max(0, attempt_count - 1))))

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _ready_condition(now: datetime):
        return or_(
            and_(
                ExportJob.status == ExportStatus.PENDING,
                or_(ExportJob.next_attempt_at.is_(None), ExportJob.next_attempt_at <= now),
                ExportJob.attempt_count < ExportJob.max_attempts,
            ),
            and_(
                ExportJob.status == ExportStatus.PROCESSING,
                ExportJob.lease_expires_at.is_not(None),
                ExportJob.lease_expires_at <= now,
                ExportJob.attempt_count < ExportJob.max_attempts,
            ),
        )

    async def reap_expired_exhausted_jobs(
        self,
        *,
        max_jobs: int,
        job_id: Optional[uuid.UUID] = None,
    ) -> int:
        """Terminalize only a bounded set of expired claims with no attempts left.

        Without this cleanup a process that crashes exactly ``max_attempts``
        times would remain in PROCESSING forever because it is no longer
        claimable.  Each row is locked independently so this sweep cannot
        overwrite a concurrently renewed/reclaimed owner.
        """
        if max_jobs <= 0:
            raise ValueError("max_jobs must be positive")
        reaped = 0
        for _ in range(max_jobs):
            now = self._now()
            async with self.session_factory() as session:
                async with session.begin():
                    conditions = [
                        ExportJob.status == ExportStatus.PROCESSING,
                        ExportJob.lease_expires_at.is_not(None),
                        ExportJob.lease_expires_at <= now,
                        ExportJob.attempt_count >= ExportJob.max_attempts,
                    ]
                    if job_id is not None:
                        conditions.append(ExportJob.id == job_id)
                    result = await session.execute(
                        select(ExportJob)
                        .where(*conditions)
                        .order_by(ExportJob.created_at.asc(), ExportJob.id.asc())
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                    job = result.scalars().first()
                    if job is None:
                        break
                    job.status = ExportStatus.FAILED
                    job.worker_id = None
                    job.lease_token = None
                    job.claimed_at = None
                    job.lease_expires_at = None
                    job.next_attempt_at = None
                    job.error_category = "lease_expired"
                    job.error_message = (
                        "export lease expired after the maximum number of allowed attempts"
                    )
                    reaped += 1
        return reaped

    async def _claim(
        self,
        *,
        worker_id: str,
        lease_duration: timedelta,
        job_id: Optional[uuid.UUID] = None,
        respect_retry_schedule: bool = True,
    ) -> Optional[ExportJob]:
        if lease_duration.total_seconds() <= 0:
            raise ValueError("lease_duration must be positive")
        now = self._now()
        ready = self._ready_condition(now)
        if not respect_retry_schedule:
            ready = or_(
                and_(
                    ExportJob.status == ExportStatus.PENDING,
                    ExportJob.attempt_count < ExportJob.max_attempts,
                ),
                and_(
                    ExportJob.status == ExportStatus.PROCESSING,
                    ExportJob.lease_expires_at.is_not(None),
                    ExportJob.lease_expires_at <= now,
                    ExportJob.attempt_count < ExportJob.max_attempts,
                ),
            )

        async with self.session_factory() as session:
            async with session.begin():
                statement = select(ExportJob).where(ready)
                if job_id is not None:
                    statement = statement.where(ExportJob.id == job_id)
                statement = (
                    statement.order_by(ExportJob.created_at.asc(), ExportJob.id.asc())
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                result = await session.execute(statement)
                job = result.scalars().first()
                if job is None:
                    return None

                # The value is an opaque generation, not merely a worker name:
                # a restarted process can reuse the same worker id.
                job.status = ExportStatus.PROCESSING
                job.worker_id = worker_id
                job.lease_token = uuid.uuid4().hex
                job.claimed_at = now
                job.lease_expires_at = now + lease_duration
                job.next_attempt_at = None
                job.attempt_count += 1
                return job

    async def claim_job(
        self,
        job_id: uuid.UUID,
        *,
        worker_id: str,
        lease_duration: timedelta,
        respect_retry_schedule: bool = True,
    ) -> Optional[ExportJob]:
        """Atomically claim a known job or reclaim it after a finite lease."""
        await self.reap_expired_exhausted_jobs(max_jobs=1, job_id=job_id)
        return await self._claim(
            worker_id=worker_id,
            lease_duration=lease_duration,
            job_id=job_id,
            respect_retry_schedule=respect_retry_schedule,
        )

    async def claim_next_job(
        self,
        *,
        worker_id: str,
        lease_duration: timedelta,
        respect_retry_schedule: bool = True,
    ) -> Optional[ExportJob]:
        """Atomically claim one due or expired job for the bounded recovery sweep."""
        return await self._claim(
            worker_id=worker_id,
            lease_duration=lease_duration,
            respect_retry_schedule=respect_retry_schedule,
        )

    async def assert_owned(self, job_id: uuid.UUID, worker_id: str, lease_token: str) -> None:
        """Raise instead of allowing an expired owner to publish an artifact."""
        now = self._now()
        async with self.session_factory() as session:
            result = await session.execute(
                select(ExportJob.id).where(
                    ExportJob.id == job_id,
                    ExportJob.status == ExportStatus.PROCESSING,
                    ExportJob.worker_id == worker_id,
                    ExportJob.lease_token == lease_token,
                    ExportJob.lease_expires_at > now,
                )
            )
            if result.scalar_one_or_none() is None:
                raise ExportLeaseLostError(f"export claim lost for job {job_id}")

    async def heartbeat_loop(
        self,
        *,
        job_id: uuid.UUID,
        worker_id: str,
        lease_token: str,
        lease_duration: timedelta,
        ownership_lost: asyncio.Event,
        sleep_interval: float,
    ) -> None:
        """Renew only the unexpired matching claim on an independent session."""
        if sleep_interval <= 0:
            raise ValueError("sleep_interval must be positive")
        while not ownership_lost.is_set():
            try:
                await asyncio.sleep(sleep_interval)
            except asyncio.CancelledError:
                return
            now = self._now()
            try:
                async with self.session_factory() as session:
                    async with session.begin():
                        result = await session.execute(
                            update(ExportJob)
                            .where(
                                ExportJob.id == job_id,
                                ExportJob.status == ExportStatus.PROCESSING,
                                ExportJob.worker_id == worker_id,
                                ExportJob.lease_token == lease_token,
                                ExportJob.lease_expires_at > now,
                            )
                            .values(lease_expires_at=now + lease_duration)
                        )
                        if result.rowcount != 1:
                            ownership_lost.set()
                            return
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Export lease heartbeat failed for job %s", job_id)
                ownership_lost.set()
                return

    async def complete_if_owned(
        self,
        *,
        job_id: uuid.UUID,
        worker_id: str,
        lease_token: str,
        s3_key: str,
        expires_at: datetime,
    ) -> bool:
        """Publish an object key only if this claim is still the current owner."""
        now = self._now()
        async with self.session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(ExportJob)
                    .where(
                        ExportJob.id == job_id,
                        ExportJob.status == ExportStatus.PROCESSING,
                        ExportJob.worker_id == worker_id,
                        ExportJob.lease_token == lease_token,
                        ExportJob.lease_expires_at > now,
                    )
                    .values(
                        status=ExportStatus.COMPLETED,
                        s3_key=s3_key,
                        expires_at=expires_at,
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

    async def record_failure_if_owned(
        self,
        *,
        job_id: uuid.UUID,
        worker_id: str,
        lease_token: str,
        error: Exception,
    ) -> bool:
        """Record a bounded retry only while the original fenced claim is valid."""
        now = self._now()
        async with self.session_factory() as session:
            async with session.begin():
                job = await session.get(ExportJob, job_id, with_for_update=True)
                if (
                    job is None
                    or job.status != ExportStatus.PROCESSING
                    or job.worker_id != worker_id
                    or job.lease_token != lease_token
                    or job.lease_expires_at is None
                    or job.lease_expires_at <= now
                ):
                    return False

                job.error_category = type(error).__name__
                job.error_message = f"{type(error).__name__}: {error}\n{traceback.format_exc()}"
                job.worker_id = None
                job.lease_token = None
                job.claimed_at = None
                job.lease_expires_at = None
                if job.attempt_count < job.max_attempts:
                    job.status = ExportStatus.PENDING
                    job.next_attempt_at = now + self.retry_delay(job.attempt_count)
                else:
                    job.status = ExportStatus.FAILED
                    job.next_attempt_at = None
                return True
