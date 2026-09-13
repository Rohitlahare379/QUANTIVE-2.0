"""Real PostgreSQL regressions for fenced export job ownership.

These tests require Quantive migrations through ``020_export_durability``.  They
exercise the database concurrency boundary; object storage is deliberately not
involved here.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.models.asset_registry import AssetRegistry
from app.models.export_jobs import ExportJob, ExportStatus
from app.services.export_jobs import ExportJobService


pytestmark = [pytest.mark.postgres, pytest.mark.timescaledb]

engine = create_async_engine(settings.sqlalchemy_database_uri, poolclass=NullPool)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


@pytest.fixture(autouse=True)
async def clean_export_tables():
    async with AsyncSessionLocal() as session:
        await session.execute(delete(ExportJob))
        await session.execute(delete(AssetRegistry))
        await session.commit()
    yield
    async with AsyncSessionLocal() as session:
        await session.execute(delete(ExportJob))
        await session.execute(delete(AssetRegistry))
        await session.commit()


async def _job(*, max_attempts: int = 5) -> ExportJob:
    async with AsyncSessionLocal() as session:
        asset = AssetRegistry(symbol=f"EXPORT{uuid.uuid4().hex[:12]}USDT", exchange="BINANCE", asset_type="SPOT", is_active=True)
        session.add(asset)
        await session.flush()
        job = ExportJob(
            asset_id=asset.id,
            timeframe="1m",
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 1, 1, 1, tzinfo=timezone.utc),
            status=ExportStatus.PENDING,
            max_attempts=max_attempts,
        )
        session.add(job)
        await session.commit()
        return job


@pytest.mark.asyncio
async def test_atomic_export_claim_allows_exactly_one_concurrent_owner():
    job = await _job()
    service_a = ExportJobService(AsyncSessionLocal)
    service_b = ExportJobService(AsyncSessionLocal)

    claim_a, claim_b = await asyncio.gather(
        service_a.claim_job(job.id, worker_id="export-a", lease_duration=timedelta(minutes=5)),
        service_b.claim_job(job.id, worker_id="export-b", lease_duration=timedelta(minutes=5)),
    )

    claims = [claim for claim in (claim_a, claim_b) if claim is not None]
    assert len(claims) == 1
    assert claims[0].lease_token is not None
    assert claims[0].attempt_count == 1


@pytest.mark.asyncio
async def test_expired_export_claim_is_recovered_with_a_new_token_and_attempt():
    job = await _job()
    service = ExportJobService(AsyncSessionLocal)
    first = await service.claim_job(job.id, worker_id="crashed", lease_duration=timedelta(minutes=5))
    assert first is not None

    async with AsyncSessionLocal() as session:
        await session.execute(
            update(ExportJob)
            .where(ExportJob.id == job.id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        await session.commit()

    recovered = await service.claim_job(job.id, worker_id="recovered", lease_duration=timedelta(minutes=5))
    assert recovered is not None
    assert recovered.lease_token != first.lease_token
    assert recovered.attempt_count == 2
    assert recovered.worker_id == "recovered"


@pytest.mark.asyncio
async def test_stale_export_owner_cannot_complete_or_fail_reclaimed_job():
    job = await _job()
    service = ExportJobService(AsyncSessionLocal)
    old = await service.claim_job(job.id, worker_id="old", lease_duration=timedelta(minutes=5))
    assert old is not None and old.lease_token is not None

    async with AsyncSessionLocal() as session:
        await session.execute(
            update(ExportJob)
            .where(ExportJob.id == job.id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        await session.commit()

    new = await service.claim_job(job.id, worker_id="new", lease_duration=timedelta(minutes=5))
    assert new is not None and new.lease_token is not None
    assert not await service.complete_if_owned(
        job_id=job.id,
        worker_id="old",
        lease_token=old.lease_token,
        s3_key="exports/stale.parquet",
        expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    assert not await service.record_failure_if_owned(
        job_id=job.id,
        worker_id="old",
        lease_token=old.lease_token,
        error=RuntimeError("stale"),
    )

    async with AsyncSessionLocal() as session:
        current = await session.get(ExportJob, job.id)
        assert current.status == ExportStatus.PROCESSING
        assert current.worker_id == "new"
        assert current.lease_token == new.lease_token


@pytest.mark.asyncio
async def test_expired_export_at_max_attempts_becomes_terminal_failure():
    job = await _job(max_attempts=1)
    service = ExportJobService(AsyncSessionLocal)
    claimed = await service.claim_job(job.id, worker_id="crashed", lease_duration=timedelta(minutes=5))
    assert claimed is not None

    async with AsyncSessionLocal() as session:
        await session.execute(
            update(ExportJob)
            .where(ExportJob.id == job.id)
            .values(lease_expires_at=datetime.now(timezone.utc) - timedelta(seconds=1))
        )
        await session.commit()

    assert await service.reap_expired_exhausted_jobs(max_jobs=1) == 1
    async with AsyncSessionLocal() as session:
        current = await session.get(ExportJob, job.id)
        assert current.status == ExportStatus.FAILED
        assert current.worker_id is None
        assert current.lease_token is None
        assert current.error_category == "lease_expired"


@pytest.mark.asyncio
async def test_export_heartbeat_renews_only_current_claim():
    job = await _job()
    service = ExportJobService(AsyncSessionLocal)
    claimed = await service.claim_job(job.id, worker_id="heartbeat", lease_duration=timedelta(seconds=2))
    assert claimed is not None and claimed.lease_token is not None
    initial_expiry = claimed.lease_expires_at
    ownership_lost = asyncio.Event()
    heartbeat = asyncio.create_task(
        service.heartbeat_loop(
            job_id=job.id,
            worker_id="heartbeat",
            lease_token=claimed.lease_token,
            lease_duration=timedelta(seconds=2),
            ownership_lost=ownership_lost,
            sleep_interval=0.1,
        )
    )
    try:
        await asyncio.sleep(0.3)
    finally:
        heartbeat.cancel()
        await heartbeat

    async with AsyncSessionLocal() as session:
        current = await session.get(ExportJob, job.id)
        assert current.lease_expires_at > initial_expiry
        assert not ownership_lost.is_set()
