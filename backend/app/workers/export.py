"""Fenced, bounded asynchronous Parquet export workers.

All database work for one Dramatiq invocation lives in one event loop and uses
``NullPool``.  A job lease is renewed from independent sessions while the worker
streams canonical data or waits for object storage, so a stale attempt can never
publish its artifact after recovery by another worker.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import dramatiq
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.models.export_jobs import ExportJob
from app.services.export_jobs import ExportJobService, ExportLeaseLostError
from app.services.query import CandleQueryService
from app.services.s3_storage import S3StorageService
from app.workers.config import broker, redis_client  # Configure Redis before actor registration.


logger = logging.getLogger(__name__)


CANDLE_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
])

_RECOVERY_DISPATCH_LEASE_KEY = "quantive:exports:recovery-dispatch-lease"
_RELEASE_DISPATCH_LEASE = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


def _session_factory_for_actor() -> tuple[async_sessionmaker[AsyncSession], object]:
    """Create a loop-local, non-pooling engine for one synchronous actor call."""
    engine = create_async_engine(
        settings.sqlalchemy_database_uri,
        poolclass=NullPool,
        pool_pre_ping=True,
    )
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False), engine


def _artifact_file_path(job_id: uuid.UUID, lease_token: str) -> str:
    """A stale/reclaimed attempt must never share a local file with its successor."""
    return f"/tmp/quantive-export-{job_id}-{lease_token}.parquet"


def _artifact_object_key(job: ExportJob, lease_token: str) -> str:
    """Only a fenced terminal update publishes this attempt-specific object key."""
    return (
        f"exports/{job.asset_id}/{job.timeframe}/{job.id}/"
        f"attempt-{job.attempt_count}-{lease_token}.parquet"
    )


async def _generate_parquet_export(
    *,
    job: ExportJob,
    file_path: str,
    session_factory: async_sessionmaker[AsyncSession],
    lease_service: ExportJobService,
    worker_id: str,
    lease_token: str,
    ownership_lost: asyncio.Event,
) -> None:
    """Stream one fenced job to disk without retaining a DB session for S3 I/O."""
    await lease_service.assert_owned(job.id, worker_id, lease_token)
    if ownership_lost.is_set():
        raise ExportLeaseLostError(f"export claim lost for job {job.id}")

    async with session_factory() as session:
        query_service = CandleQueryService(session)
        stream = query_service.get_candles(
            asset_id=job.asset_id,
            timeframe=job.timeframe,
            start_time=job.start_time,
            end_time=job.end_time,
        )
        with pq.ParquetWriter(file_path, CANDLE_SCHEMA, compression="gzip") as writer:
            chunk_columns = {column: [] for column in CANDLE_SCHEMA.names}
            chunk_size = 0
            async for candle in stream:
                if ownership_lost.is_set():
                    raise ExportLeaseLostError(f"export claim lost for job {job.id}")
                chunk_columns["timestamp"].append(candle["timestamp"])
                chunk_columns["open"].append(candle["open"])
                chunk_columns["high"].append(candle["high"])
                chunk_columns["low"].append(candle["low"])
                chunk_columns["close"].append(candle["close"])
                chunk_columns["volume"].append(candle["volume"])
                chunk_size += 1

                if chunk_size >= 10_000:
                    writer.write_table(pa.Table.from_pydict(chunk_columns, schema=CANDLE_SCHEMA))
                    # A long disk stream is bounded in memory.  Recheck ownership
                    # at each bounded chunk before accepting more canonical rows.
                    await lease_service.assert_owned(job.id, worker_id, lease_token)
                    if ownership_lost.is_set():
                        raise ExportLeaseLostError(f"export claim lost for job {job.id}")
                    chunk_columns = {column: [] for column in CANDLE_SCHEMA.names}
                    chunk_size = 0

            if chunk_size > 0:
                writer.write_table(pa.Table.from_pydict(chunk_columns, schema=CANDLE_SCHEMA))

    await lease_service.assert_owned(job.id, worker_id, lease_token)
    if ownership_lost.is_set():
        raise ExportLeaseLostError(f"export claim lost for job {job.id}")


def _upload_artifact_sync(file_path: str, object_key: str) -> bool:
    storage = S3StorageService()
    return storage.upload_file(file_path, object_key)


def _delete_artifact_sync(object_key: str) -> None:
    try:
        S3StorageService().delete_file(object_key)
    except Exception:
        logger.exception("Unable to delete unowned export artifact %s", object_key)


async def _upload_artifact_with_fence(
    *,
    file_path: str,
    object_key: str,
    ownership_lost: asyncio.Event,
) -> bool:
    """Run blocking object-storage I/O off-loop while the lease heartbeat runs.

    A Python thread cannot be safely killed during boto's upload.  Await it even
    after ownership loss before removing the local file, then delete only the
    stale attempt's unique object key.  Thus a late/stale upload cannot overwrite
    the artifact a new owner publishes.
    """
    if ownership_lost.is_set():
        raise ExportLeaseLostError("export claim lost before object upload")
    upload_task = asyncio.create_task(asyncio.to_thread(_upload_artifact_sync, file_path, object_key))
    lost_task = asyncio.create_task(ownership_lost.wait())
    try:
        done, _ = await asyncio.wait({upload_task, lost_task}, return_when=asyncio.FIRST_COMPLETED)
        if lost_task in done and ownership_lost.is_set():
            uploaded = False
            try:
                uploaded = await asyncio.shield(upload_task)
            except Exception:
                logger.exception("Stale export upload failed while waiting for cleanup")
            if uploaded:
                await asyncio.to_thread(_delete_artifact_sync, object_key)
            raise ExportLeaseLostError("export claim lost during object upload")

        uploaded = await upload_task
        if ownership_lost.is_set():
            if uploaded:
                await asyncio.to_thread(_delete_artifact_sync, object_key)
            raise ExportLeaseLostError("export claim lost after object upload")
        return uploaded
    finally:
        lost_task.cancel()
        try:
            await lost_task
        except asyncio.CancelledError:
            pass
        if not upload_task.done():
            # Do not remove the attempt-specific local file until the synchronous
            # uploader releases it.  Storage client timeouts bound this wait.
            try:
                await asyncio.shield(upload_task)
            except Exception:
                pass


async def _execute_claimed_export(
    *,
    job: ExportJob,
    session_factory: async_sessionmaker[AsyncSession],
    lease_service: ExportJobService,
    worker_id: str,
    lease_duration: timedelta,
    heartbeat_interval: float,
) -> bool:
    """Generate/upload one already-claimed job and make a fenced terminal write."""
    if job.lease_token is None:
        raise RuntimeError(f"claimed export job {job.id} has no lease token")
    lease_token = job.lease_token
    file_path = _artifact_file_path(job.id, lease_token)
    object_key = _artifact_object_key(job, lease_token)
    ownership_lost = asyncio.Event()
    heartbeat = asyncio.create_task(
        lease_service.heartbeat_loop(
            job_id=job.id,
            worker_id=worker_id,
            lease_token=lease_token,
            lease_duration=lease_duration,
            ownership_lost=ownership_lost,
            sleep_interval=heartbeat_interval,
        ),
        name=f"export-lease-heartbeat-{job.id}",
    )
    uploaded = False
    completed = False
    try:
        await _generate_parquet_export(
            job=job,
            file_path=file_path,
            session_factory=session_factory,
            lease_service=lease_service,
            worker_id=worker_id,
            lease_token=lease_token,
            ownership_lost=ownership_lost,
        )
        uploaded = await _upload_artifact_with_fence(
            file_path=file_path,
            object_key=object_key,
            ownership_lost=ownership_lost,
        )
        if not uploaded:
            raise RuntimeError("object storage upload returned false")
        if ownership_lost.is_set():
            raise ExportLeaseLostError(f"export claim lost for job {job.id}")
        completed = await lease_service.complete_if_owned(
            job_id=job.id,
            worker_id=worker_id,
            lease_token=lease_token,
            s3_key=object_key,
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=settings.S3_PRESIGNED_EXPIRY_SECONDS),
        )
        if not completed:
            raise ExportLeaseLostError(f"export claim lost before completion for job {job.id}")
        logger.info("Export job %s completed successfully", job.id)
        return True
    except ExportLeaseLostError:
        # A later claim owns the database result.  Do not convert its progress
        # into a failure, and do not leave our attempt-specific object visible.
        logger.warning("Stopped stale export worker for job %s", job.id)
        return False
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        logger.exception("Export job %s failed", job.id)
        await lease_service.record_failure_if_owned(
            job_id=job.id,
            worker_id=worker_id,
            lease_token=lease_token,
            error=exc,
        )
        raise
    finally:
        heartbeat.cancel()
        try:
            await heartbeat
        except asyncio.CancelledError:
            pass
        if uploaded and not completed:
            await asyncio.to_thread(_delete_artifact_sync, object_key)
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except OSError:
                logger.warning("Unable to remove export temporary file %s", file_path)


async def _run_export_job(
    job_id: uuid.UUID,
    *,
    worker_id: Optional[str] = None,
    lease_duration: Optional[timedelta] = None,
    heartbeat_interval: Optional[float] = None,
    session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
) -> bool:
    """Run one known export in a single event loop and dispose its engine."""
    owns_engine = session_factory is None
    engine = None
    if session_factory is None:
        session_factory, engine = _session_factory_for_actor()
    worker_id = worker_id or f"export-worker-{uuid.uuid4().hex}"
    lease_duration = lease_duration or timedelta(seconds=settings.EXPORT_LEASE_SECONDS)
    heartbeat_interval = (
        settings.EXPORT_HEARTBEAT_INTERVAL_SECONDS
        if heartbeat_interval is None
        else heartbeat_interval
    )
    try:
        lease_service = ExportJobService(session_factory)
        job = await lease_service.claim_job(
            job_id,
            worker_id=worker_id,
            lease_duration=lease_duration,
            respect_retry_schedule=True,
        )
        if job is None:
            logger.info("Export job %s is not currently claimable", job_id)
            return False
        return await _execute_claimed_export(
            job=job,
            session_factory=session_factory,
            lease_service=lease_service,
            worker_id=worker_id,
            lease_duration=lease_duration,
            heartbeat_interval=heartbeat_interval,
        )
    finally:
        if owns_engine and engine is not None:
            await engine.dispose()


async def _run_available_exports(
    *,
    max_jobs: Optional[int] = None,
    worker_id: Optional[str] = None,
    lease_duration: Optional[timedelta] = None,
    heartbeat_interval: Optional[float] = None,
    session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
) -> int:
    """Recover/execute a finite number of due or expired export jobs."""
    max_jobs = settings.EXPORT_MAX_JOBS_PER_RUN if max_jobs is None else max_jobs
    if max_jobs <= 0:
        raise ValueError("max_jobs must be positive")
    owns_engine = session_factory is None
    engine = None
    if session_factory is None:
        session_factory, engine = _session_factory_for_actor()
    worker_id = worker_id or f"export-recovery-{uuid.uuid4().hex}"
    lease_duration = lease_duration or timedelta(seconds=settings.EXPORT_LEASE_SECONDS)
    heartbeat_interval = (
        settings.EXPORT_HEARTBEAT_INTERVAL_SECONDS
        if heartbeat_interval is None
        else heartbeat_interval
    )
    try:
        lease_service = ExportJobService(session_factory)
        # The sweep itself is bounded so a database full of stale rows cannot
        # make a maintenance tick run without limit.
        await lease_service.reap_expired_exhausted_jobs(max_jobs=max_jobs)
        processed = 0
        for _ in range(max_jobs):
            job = await lease_service.claim_next_job(
                worker_id=worker_id,
                lease_duration=lease_duration,
                respect_retry_schedule=True,
            )
            if job is None:
                break
            processed += 1
            try:
                await _execute_claimed_export(
                    job=job,
                    session_factory=session_factory,
                    lease_service=lease_service,
                    worker_id=worker_id,
                    lease_duration=lease_duration,
                    heartbeat_interval=heartbeat_interval,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                # Failure state was recorded with the current token.  Continue
                # the bounded sweep so one bad export cannot starve recovery.
                logger.exception("Recovered export attempt failed for job %s", job.id)
        return processed
    finally:
        if owns_engine and engine is not None:
            await engine.dispose()


def _release_recovery_dispatch_lease(token: str) -> None:
    try:
        redis_client.eval(_RELEASE_DISPATCH_LEASE, 1, _RECOVERY_DISPATCH_LEASE_KEY, token)
    except Exception:
        # Expiry is crash recovery; never issue an unconditional delete that
        # could clear a newer scheduler's coalescing token.
        logger.exception("Unable to release export recovery dispatch lease")


def enqueue_export_recovery() -> bool:
    """Coalesce bounded recovery sweeps; Redis failure schedules no duplicate work."""
    token = uuid.uuid4().hex
    try:
        acquired = redis_client.set(
            _RECOVERY_DISPATCH_LEASE_KEY,
            token,
            nx=True,
            ex=settings.EXPORT_RECOVERY_DISPATCH_LEASE_SECONDS,
        )
    except Exception:
        logger.exception("Unable to acquire export recovery dispatch lease")
        return False
    if not acquired:
        return False
    try:
        process_available_exports.send(token)
    except Exception:
        _release_recovery_dispatch_lease(token)
        raise
    return True


@dramatiq.actor(queue_name="exports", max_retries=3)
def process_export_job(job_id_str: str) -> None:
    """Execute one explicitly requested job under a fresh loop-local engine."""
    job_id = uuid.UUID(job_id_str)
    logger.info("Starting export job %s", job_id)
    # Exactly one loop per synchronous actor invocation.  Do not split its DB
    # phases over several asyncio.run calls: asyncpg connections are loop-bound.
    asyncio.run(_run_export_job(job_id))


@dramatiq.actor(queue_name="exports", max_retries=3)
def process_available_exports(dispatch_token: str | None = None) -> None:
    """Run a finite crash-recovery/due-export sweep."""
    try:
        processed = asyncio.run(_run_available_exports())
        logger.info("Export recovery worker processed %s job(s)", processed)
    finally:
        if dispatch_token is not None:
            _release_recovery_dispatch_lease(dispatch_token)
