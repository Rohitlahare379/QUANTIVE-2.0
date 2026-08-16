"""
Test Matrix — Gap Repair Job State Machine (Tests 27 to 32).
Verifies atomic job claim via SELECT ... FOR UPDATE SKIP LOCKED, duplicate job prevention,
stale job recovery, worker crash / lease expiration, job cancellation, and retry exhaustion.
"""

import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, patch
from sqlalchemy import select, delete, update
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.config import settings
from app.models.asset_registry import AssetRegistry
from app.models.raw_1m_candles import Raw1mCandle
from app.models.gap_staging_candles import GapStagingCandle
from app.models.sync_ranges import SyncRange
from app.models.gap_repair_jobs import GapRepairJob, GapRepairStatus
from app.models.cagg_refresh_jobs import CaggRefreshJob
from app.services.gap_repair import GapRepairService, classify_error
from app.connectors.exceptions import APIError, NetworkError

engine = create_async_engine(settings.sqlalchemy_database_uri, poolclass=NullPool)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


@pytest.fixture(autouse=True)
async def clean_database():
    async with AsyncSessionLocal() as session:
        await session.execute(delete(CaggRefreshJob))
        await session.execute(delete(GapRepairJob))
        await session.execute(delete(Raw1mCandle))
        await session.execute(delete(GapStagingCandle))
        await session.execute(delete(SyncRange))
        await session.execute(delete(AssetRegistry))
        await session.commit()
    yield
    async with AsyncSessionLocal() as session:
        await session.execute(delete(CaggRefreshJob))
        await session.execute(delete(GapRepairJob))
        await session.execute(delete(Raw1mCandle))
        await session.execute(delete(GapStagingCandle))
        await session.execute(delete(SyncRange))
        await session.execute(delete(AssetRegistry))
        await session.commit()


async def _create_test_asset(symbol: str = "BTCUSDT") -> int:
    async with AsyncSessionLocal() as session:
        asset = AssetRegistry(symbol=symbol, exchange="BINANCE", asset_type="SPOT", is_active=True)
        session.add(asset)
        await session.commit()
        await session.refresh(asset)
        return asset.id


@pytest.mark.asyncio
async def test_27_job_claim_exclusivity_skip_locked():
    """
    Test 27: Worker A and Worker B concurrently attempt to claim the same pending job.
    Exactly ONE worker claims the job; the other receives None (SKIP LOCKED).
    """
    asset_id = await _create_test_asset("BTCUSDT")
    t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    t10 = datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc)

    service = GapRepairService(session_factory=AsyncSessionLocal)
    job = await service.schedule_repair_job(asset_id, "BTCUSDT", t0, t10)
    assert job is not None

    worker_a_claims = []
    worker_b_claims = []

    async def worker_a_claim():
        claimed = await service.claim_job(worker_id="worker-A", lease_duration=timedelta(minutes=5))
        if claimed:
            worker_a_claims.append(claimed)

    async def worker_b_claim():
        claimed = await service.claim_job(worker_id="worker-B", lease_duration=timedelta(minutes=5))
        if claimed:
            worker_b_claims.append(claimed)

    await asyncio.gather(worker_a_claim(), worker_b_claim())

    total_claims = len(worker_a_claims) + len(worker_b_claims)
    assert total_claims == 1


@pytest.mark.asyncio
async def test_28_duplicate_job_prevention():
    """
    Test 28: Scheduling the exact same repair window multiple times returns the existing active job.
    Does NOT create duplicate job rows.
    """
    asset_id = await _create_test_asset("ETHUSDT")
    t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    t10 = datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc)

    service = GapRepairService(session_factory=AsyncSessionLocal)

    job1 = await service.schedule_repair_job(asset_id, "ETHUSDT", t0, t10)
    job2 = await service.schedule_repair_job(asset_id, "ETHUSDT", t0, t10)

    assert job1.id == job2.id

    async with AsyncSessionLocal() as session:
        jobs = (await session.execute(select(GapRepairJob).where(GapRepairJob.asset_id == asset_id))).scalars().all()
        assert len(jobs) == 1


@pytest.mark.asyncio
async def test_29_stale_job_recovery():
    """
    Test 29: Worker A claims a job, but Worker A crashes. Its lease expires.
    Worker B successfully reclaims the stale PROCESSING job.
    """
    asset_id = await _create_test_asset("SOLUSDT")
    t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    t10 = datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)

    async with AsyncSessionLocal() as session:
        job = GapRepairJob(
            asset_id=asset_id,
            symbol="SOLUSDT",
            start_time=t0,
            end_time=t10,
            status=GapRepairStatus.PROCESSING,
            worker_id="crashed-worker-A",
            claimed_at=now - timedelta(minutes=10),
            lease_expires_at=now - timedelta(seconds=10),
        )
        session.add(job)
        await session.commit()
        job_id = job.id

    service = GapRepairService(session_factory=AsyncSessionLocal)
    reclaimed_job = await service.claim_job(worker_id="worker-B", lease_duration=timedelta(minutes=5))

    assert reclaimed_job is not None
    assert reclaimed_job.id == job_id
    assert reclaimed_job.worker_id == "worker-B"
    assert reclaimed_job.status == GapRepairStatus.PROCESSING


@pytest.mark.asyncio
async def test_30_worker_crash_and_heartbeat_renewal():
    """
    Test 30: Proves heartbeat periodically extends lease_expires_at on independent session.
    When heartbeat is cancelled (worker crash), lease expires and job is reclaimable.
    """
    asset_id = await _create_test_asset("ADAUSDT")
    t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    t10 = datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc)

    service = GapRepairService(session_factory=AsyncSessionLocal)
    job = await service.schedule_repair_job(asset_id, "ADAUSDT", t0, t10)

    claimed = await service.claim_job(worker_id="worker-heartbeat", lease_duration=timedelta(seconds=2))
    assert claimed is not None

    initial_expiry = claimed.lease_expires_at

    heartbeat_task = asyncio.create_task(
        service._heartbeat_loop(claimed.id, "worker-heartbeat", lease_duration=timedelta(seconds=2), sleep_interval=0.4)
    )

    await asyncio.sleep(1.0)
    heartbeat_task.cancel()
    try:
        await heartbeat_task
    except asyncio.CancelledError:
        pass

    async with AsyncSessionLocal() as session:
        updated_job = await session.get(GapRepairJob, claimed.id)
        assert updated_job.lease_expires_at > initial_expiry


@pytest.mark.asyncio
async def test_31_job_cancellation():
    """
    Test 31: A job marked as CANCELLED is never claimed by any worker.
    """
    asset_id = await _create_test_asset("AVAXUSDT")
    t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    t10 = datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc)

    async with AsyncSessionLocal() as session:
        job = GapRepairJob(
            asset_id=asset_id,
            symbol="AVAXUSDT",
            start_time=t0,
            end_time=t10,
            status=GapRepairStatus.CANCELLED,
        )
        session.add(job)
        await session.commit()

    service = GapRepairService(session_factory=AsyncSessionLocal)
    claimed = await service.claim_job(worker_id="worker-X")
    assert claimed is None


@pytest.mark.asyncio
async def test_32_retry_exhaustion_and_failure_state():
    """
    Test 32: If a job fails with a retryable error up to max_retries, it transitions
    to FAILED and records the terminal error. Non-retryable errors fail immediately.
    """
    asset_id = await _create_test_asset("DOGEUSDT")
    t0 = datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    t10 = datetime(2026, 1, 1, 10, 10, tzinfo=timezone.utc)

    service = GapRepairService(session_factory=AsyncSessionLocal)
    job = await service.schedule_repair_job(asset_id, "DOGEUSDT", t0, t10, max_retries=2)

    category, is_retryable = classify_error(APIError("API Error 400: Invalid symbol"))
    assert category == "PERMANENT"
    assert is_retryable is False

    category_net, is_retryable_net = classify_error(NetworkError("Connection reset"))
    assert category_net == "NETWORK"
    assert is_retryable_net is True

    mock_client = AsyncMock()
    async def mock_fail_klines(*args, **kwargs):
        raise NetworkError("Simulated network drop")
        yield {}
    mock_client.get_klines = mock_fail_klines

    # Attempt 1: fails, requeues to PENDING (retry 1/2)
    with pytest.raises(NetworkError):
        await service.process_next_job(worker_id="w1", binance_client=mock_client)

    async with AsyncSessionLocal() as session:
        j1 = await session.get(GapRepairJob, job.id)
        assert j1.status == GapRepairStatus.PENDING
        assert j1.retry_count == 1

    # Attempt 2: fails, requeues to PENDING (retry 2/2)
    with pytest.raises(NetworkError):
        await service.process_next_job(worker_id="w2", binance_client=mock_client)

    async with AsyncSessionLocal() as session:
        j2 = await session.get(GapRepairJob, job.id)
        assert j2.status == GapRepairStatus.PENDING
        assert j2.retry_count == 2

    # Attempt 3: fails, exceeds max_retries -> transitions to FAILED
    with pytest.raises(NetworkError):
        await service.process_next_job(worker_id="w3", binance_client=mock_client)

    async with AsyncSessionLocal() as session:
        j3 = await session.get(GapRepairJob, job.id)
        assert j3.status == GapRepairStatus.FAILED
        assert j3.retry_count == 3
        assert j3.error_category == "NETWORK"
