import asyncio
import logging
import traceback
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List
from sqlalchemy import select, update, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.cagg_refresh_jobs import CaggRefreshJob, RefreshStatus

logger = logging.getLogger(__name__)

class CaggRefreshService:
    """
    Dedicated worker to process historically modified time windows and trigger targeted 
    refresh_continuous_aggregate calls to keep the historical CAGG layers perfectly synced.
    
    Architecture:
    Decouples the long-running CAGG refresh execution session from independent heartbeat
    sessions created via `session_factory` to guarantee lease extension liveness without
    session/connection blocking.
    """
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory
        self.cagg_names = [
            'candles_5m',
            'candles_15m',
            'candles_1h',
            'candles_4h',
            'candles_1d'
        ]

    async def _heartbeat_loop(
        self, 
        job_id: int, 
        worker_id: str, 
        lease_duration: timedelta, 
        sleep_interval: Optional[float] = None
    ):
        """
        Periodically extends the lease of the claimed job while it is being processed.
        Uses independent database sessions from session_factory so it never collides
        with or blocks on the long-running refresh session/connection.
        """
        if sleep_interval is None:
            sleep_interval = lease_duration.total_seconds() / 2.0
        
        while True:
            try:
                await asyncio.sleep(sleep_interval)
            except asyncio.CancelledError:
                break

            try:
                # Update lease atomically using an independent session from the pool
                async with self.session_factory() as session:
                    async with session.begin():
                        new_expiry = datetime.now(timezone.utc) + lease_duration
                        stmt = (
                            update(CaggRefreshJob)
                            .where(
                                CaggRefreshJob.id == job_id,
                                CaggRefreshJob.status == RefreshStatus.PROCESSING,
                                CaggRefreshJob.worker_id == worker_id
                            )
                            .values(lease_expires_at=new_expiry)
                        )
                        result = await session.execute(stmt)
                        if result.rowcount > 0:
                            logger.debug(f"Renewed lease for CAGG Refresh Job #{job_id} by worker {worker_id} until {new_expiry}")
                        else:
                            # Worker no longer owns the job (e.g. lease expired and reclaimed)
                            logger.warning(f"Worker {worker_id} lost ownership of job #{job_id} during heartbeat. Stopping heartbeat.")
                            break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error during heartbeat for job #{job_id}: {e}. Will retry on next cycle.")

    async def process_pending_jobs(
        self, 
        lease_duration: timedelta = timedelta(minutes=5), 
        heartbeat_interval: Optional[float] = None
    ) -> bool:
        """
        Fetches the oldest PENDING job (or stale PROCESSING job), claims it atomically via 
        FOR UPDATE SKIP LOCKED in a short transaction, executes the TimescaleDB refreshes
        on a dedicated refresh session while a decoupled heartbeat task extends the lease 
        on independent sessions, and safely marks completion or failure using ownership-aware updates.

        Returns True if a job was claimed and processed, False if no jobs were available.
        """
        job_id = None
        window_start = None
        window_end = None
        worker_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)

        # 1. Short claim transaction: fetch oldest pending job OR stale processing job WITH SKIP LOCKED
        async with self.session_factory() as claim_session:
            async with claim_session.begin():
                stmt = (
                    select(CaggRefreshJob)
                    .where(
                        (CaggRefreshJob.status == RefreshStatus.PENDING) |
                        ((CaggRefreshJob.status == RefreshStatus.PROCESSING) & (CaggRefreshJob.lease_expires_at < now))
                    )
                    .order_by(CaggRefreshJob.created_at.asc(), CaggRefreshJob.id.asc())
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                
                result = await claim_session.execute(stmt)
                job = result.scalars().first()
                
                if not job:
                    return False # No pending jobs
                    
                logger.info(f"Worker {worker_id} claiming CAGG Refresh Job #{job.id} for window: {job.window_start} to {job.window_end}")
                
                # Lock in the database with lease
                job.status = RefreshStatus.PROCESSING
                job.worker_id = worker_id
                job.claimed_at = now
                job.lease_expires_at = now + lease_duration
                
                job_id = job.id
                window_start = job.window_start
                window_end = job.window_end

        # Claim transaction committed, row lock released. Ownership maintained purely by the lease & worker_id.

        # Start independent heartbeat task
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(job_id, worker_id, lease_duration, sleep_interval=heartbeat_interval)
        )

        try:
            # 2. Execute refreshes sequentially on a dedicated refresh session
            async with self.session_factory() as refresh_session:
                for cagg_name in self.cagg_names:
                    logger.info(f"Refreshing {cagg_name} for window {window_start} -> {window_end}")
                    
                    refresh_stmt = text(f"""
                        CALL refresh_continuous_aggregate(
                            '{cagg_name}', 
                            '{window_start.isoformat()}'::timestamptz, 
                            '{window_end.isoformat()}'::timestamptz
                        );
                    """)
                    await refresh_session.execute(refresh_stmt)
                    await refresh_session.commit()

            # 3. Mark completed ONLY if we still own the job (atomic conditional update)
            async with self.session_factory() as complete_session:
                async with complete_session.begin():
                    stmt = (
                        update(CaggRefreshJob)
                        .where(
                            CaggRefreshJob.id == job_id,
                            CaggRefreshJob.status == RefreshStatus.PROCESSING,
                            CaggRefreshJob.worker_id == worker_id
                        )
                        .values(
                            status=RefreshStatus.COMPLETED,
                            error_message=None,
                            lease_expires_at=None
                        )
                    )
                    res = await complete_session.execute(stmt)
                    if res.rowcount > 0:
                        logger.info(f"Successfully completed CAGG Refresh Job #{job_id} by worker {worker_id}")
                    else:
                        logger.warning(f"Worker {worker_id} finished job #{job_id} but no longer owns it. Discarding state change.")

        except Exception as e:
            logger.error(f"Failed CAGG Refresh Job #{job_id} by worker {worker_id}: {e}")
            try:
                # Mark failed ONLY if we still own the job
                async with self.session_factory() as fail_session:
                    async with fail_session.begin():
                        stmt = (
                            update(CaggRefreshJob)
                            .where(
                                CaggRefreshJob.id == job_id,
                                CaggRefreshJob.status == RefreshStatus.PROCESSING,
                                CaggRefreshJob.worker_id == worker_id
                            )
                            .values(
                                status=RefreshStatus.FAILED,
                                error_message=str(e) + "\n" + traceback.format_exc(),
                                lease_expires_at=None
                            )
                        )
                        res = await fail_session.execute(stmt)
                        if res.rowcount > 0:
                            logger.info(f"Marked CAGG Refresh Job #{job_id} as FAILED by worker {worker_id}")
                        else:
                            logger.warning(f"Worker {worker_id} encountered failure on job #{job_id} but no longer owns it.")
            except Exception as fail_err:
                logger.error(f"Error while recording job #{job_id} failure status: {fail_err}")
            raise
        finally:
            # Heartbeat task cleanup: cancel and await cleanly
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

        return True


def compute_cagg_bucket_alignment(
    start_time: datetime,
    end_time: datetime,
    timeframe: Optional[str] = None
) -> Tuple[datetime, datetime]:
    """
    Aligns start_time and end_time to aggregate bucket boundaries.

    Supported timeframes:
    - '5m': 5-minute boundary
    - '15m': 15-minute boundary
    - '1h': 1-hour boundary
    - '4h': 4-hour boundary (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
    - '1d': 1-day boundary (midnight UTC)
    - None: union alignment covering all aggregate timeframes (1d boundary)
    """
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=timezone.utc)
    else:
        start_time = start_time.astimezone(timezone.utc)

    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)
    else:
        end_time = end_time.astimezone(timezone.utc)

    if timeframe == "5m":
        aligned_start = start_time.replace(
            minute=(start_time.minute // 5) * 5, second=0, microsecond=0
        )
        end_aligned_base = end_time.replace(second=0, microsecond=0)
        if end_time.minute % 5 == 0 and end_time.second == 0 and end_time.microsecond == 0:
            aligned_end = end_aligned_base
        else:
            aligned_end = end_aligned_base.replace(
                minute=(end_time.minute // 5) * 5
            ) + timedelta(minutes=5)
        return aligned_start, aligned_end

    elif timeframe == "15m":
        aligned_start = start_time.replace(
            minute=(start_time.minute // 15) * 15, second=0, microsecond=0
        )
        end_aligned_base = end_time.replace(second=0, microsecond=0)
        if end_time.minute % 15 == 0 and end_time.second == 0 and end_time.microsecond == 0:
            aligned_end = end_aligned_base
        else:
            aligned_end = end_aligned_base.replace(
                minute=(end_time.minute // 15) * 15
            ) + timedelta(minutes=15)
        return aligned_start, aligned_end

    elif timeframe == "1h":
        aligned_start = start_time.replace(minute=0, second=0, microsecond=0)
        if end_time.minute == 0 and end_time.second == 0 and end_time.microsecond == 0:
            aligned_end = end_time.replace(second=0, microsecond=0)
        else:
            aligned_end = end_time.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        return aligned_start, aligned_end

    elif timeframe == "4h":
        aligned_start = start_time.replace(
            hour=(start_time.hour // 4) * 4, minute=0, second=0, microsecond=0
        )
        if end_time.hour % 4 == 0 and end_time.minute == 0 and end_time.second == 0 and end_time.microsecond == 0:
            aligned_end = end_time.replace(minute=0, second=0, microsecond=0)
        else:
            aligned_end = end_time.replace(
                hour=(end_time.hour // 4) * 4, minute=0, second=0, microsecond=0
            ) + timedelta(hours=4)
        return aligned_start, aligned_end

    elif timeframe == "1d":
        aligned_start = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
        if end_time.hour == 0 and end_time.minute == 0 and end_time.second == 0 and end_time.microsecond == 0:
            aligned_end = end_time.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            aligned_end = end_time.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return aligned_start, aligned_end

    else:
        # Default: align to the day boundary to encompass all 5m, 15m, 1h, 4h, 1d continuous aggregates
        return compute_cagg_bucket_alignment(start_time, end_time, timeframe="1d")


async def schedule_cagg_refresh_job(
    db: AsyncSession,
    start_time: datetime,
    end_time: datetime,
    timeframe: Optional[str] = None
) -> CaggRefreshJob:
    """
    Schedules a CaggRefreshJob for the aligned time window.
    """
    aligned_start, aligned_end = compute_cagg_bucket_alignment(start_time, end_time, timeframe=timeframe)
    job = CaggRefreshJob(
        window_start=aligned_start,
        window_end=aligned_end,
        status=RefreshStatus.PENDING
    )
    db.add(job)
    await db.flush()
    return job

