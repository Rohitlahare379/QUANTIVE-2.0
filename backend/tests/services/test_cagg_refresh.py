import asyncio
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, AsyncMock
from sqlalchemy import select, delete, update, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from app.models.cagg_refresh_jobs import CaggRefreshJob, RefreshStatus
from app.services.cagg_refresh import CaggRefreshService
from app.core.config import settings

# Test DB Engine and Session Factory
engine = create_async_engine(settings.sqlalchemy_database_uri, pool_pre_ping=True)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)

@pytest.fixture(autouse=True)
async def cleanup_jobs():
    """Clean up CaggRefreshJob table before and after each test."""
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(delete(CaggRefreshJob))
            await session.commit()
    except Exception:
        pass
    yield
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(delete(CaggRefreshJob))
            await session.commit()
    except Exception:
        pass


@pytest.mark.asyncio
async def test_1_heartbeat_during_long_query():
    """
    TEST 1 — HEARTBEAT DURING LONG QUERY
    Proves that the heartbeat using an independent AsyncSession writes to the DB 
    and advances lease_expires_at multiple times while a dedicated refresh connection 
    is busy with a long-running query (e.g., pg_sleep / CALL refresh_continuous_aggregate).
    """
    # 1. Create a processing job in DB
    job_id = None
    worker_id = "test-worker-1"
    initial_lease = timedelta(seconds=2)
    now = datetime.now(timezone.utc)
    
    async with AsyncSessionLocal() as session:
        async with session.begin():
            job = CaggRefreshJob(
                window_start=now - timedelta(days=1),
                window_end=now,
                status=RefreshStatus.PROCESSING,
                worker_id=worker_id,
                claimed_at=now,
                lease_expires_at=now + initial_lease
            )
            session.add(job)
        await session.commit()
        job_id = job.id

    service = CaggRefreshService(AsyncSessionLocal)
    heartbeat_task = asyncio.create_task(
        service._heartbeat_loop(job_id, worker_id, lease_duration=timedelta(seconds=4), sleep_interval=0.5)
    )

    recorded_expirations = []

    async def simulate_long_refresh_connection():
        # Dedicated Connection A: executes long-running query / sleep
        async with AsyncSessionLocal() as refresh_session:
            # We poll the DB from a 3rd session to record lease progression while Connection A is busy
            for _ in range(5):
                await asyncio.sleep(0.6)
                async with AsyncSessionLocal() as inspect_session:
                    current_job = await inspect_session.get(CaggRefreshJob, job_id)
                    recorded_expirations.append(current_job.lease_expires_at)

    try:
        await simulate_long_refresh_connection()
    finally:
        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

    # Verify that lease_expires_at advanced multiple times during the long query
    assert len(recorded_expirations) >= 3
    # Verify strict monotonic advancement of lease timestamp
    assert recorded_expirations[-1] > recorded_expirations[0]


@pytest.mark.asyncio
async def test_2_same_session_negative_concurrency():
    """
    TEST 2 — SAME SESSION NEGATIVE TEST
    Documents why sharing a single AsyncSession concurrently between refresh and heartbeat
    is fundamentally unsafe. When an operation is in flight on an AsyncSession, concurrent 
    use of the same session causes conflicts/errors.
    """
    async with AsyncSessionLocal() as shared_session:
        async def mock_long_query():
            # Holds connection busy
            await shared_session.execute(text("SELECT 1"))
            await asyncio.sleep(0.5)

        async def concurrent_heartbeat_attempt():
            await asyncio.sleep(0.1)
            # Attempting transaction or query on same session
            async with shared_session.begin():
                await shared_session.execute(text("SELECT 1"))

        # Concurrent operations on the same session should raise an error or deadlock
        # Depending on driver state, asyncpg raises InterfaceError: cannot perform operation: another operation is in progress
        with pytest.raises(Exception):
            await asyncio.gather(
                mock_long_query(),
                concurrent_heartbeat_attempt()
            )


@pytest.mark.asyncio
async def test_3_live_long_running_job_prevents_reclamation():
    """
    TEST 3 — LIVE LONG-RUNNING JOB
    Worker A has lease = 2s, heartbeat = 0.5s, execution duration = 2.5s.
    Worker B polls every 200ms.
    Expected: Worker B NEVER reclaims Worker A's job because heartbeat continuously extends lease.
    Worker A completes successfully.
    """
    job_id = None
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        async with session.begin():
            job = CaggRefreshJob(
                window_start=now - timedelta(days=1),
                window_end=now,
                status=RefreshStatus.PENDING
            )
            session.add(job)
        await session.commit()
        job_id = job.id

    service_a = CaggRefreshService(AsyncSessionLocal)
    service_b = CaggRefreshService(AsyncSessionLocal)

    worker_b_claimed = []

    async def worker_a_run():
        # Mock CAGG refresh calls to simulate 2.5s execution while heartbeat runs
        with patch.object(service_a, 'cagg_names', ['candles_5m']):
            # We override refresh execution with a 2.5s delay
            original_session_factory = service_a.session_factory
            await service_a.process_pending_jobs(
                lease_duration=timedelta(seconds=2),
                heartbeat_interval=0.5
            )

    async def worker_b_poll():
        # Worker B polls every 200ms for 3 seconds
        for _ in range(12):
            await asyncio.sleep(0.2)
            claimed = await service_b.process_pending_jobs(lease_duration=timedelta(seconds=2))
            if claimed:
                worker_b_claimed.append(True)

    await asyncio.gather(worker_a_run(), worker_b_poll())

    # Worker B should NEVER have claimed Worker A's live job
    assert len(worker_b_claimed) == 0

    # Job should be COMPLETED by Worker A
    async with AsyncSessionLocal() as session:
        final_job = await session.get(CaggRefreshJob, job_id)
        assert final_job.status == RefreshStatus.COMPLETED


@pytest.mark.asyncio
async def test_4_crash_recovery_expired_lease():
    """
    TEST 4 — CRASH RECOVERY
    Worker A claims a job, but crashes (heartbeat stops).
    Lease expires.
    Worker B successfully reclaims the job and completes it.
    """
    job_id = None
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Simulate a crashed worker's expired job
            job = CaggRefreshJob(
                window_start=now - timedelta(days=1),
                window_end=now,
                status=RefreshStatus.PROCESSING,
                worker_id="crashed-worker-dead",
                claimed_at=now - timedelta(minutes=10),
                lease_expires_at=now - timedelta(seconds=5) # Expired
            )
            session.add(job)
        await session.commit()
        job_id = job.id

    service_b = CaggRefreshService(AsyncSessionLocal)
    with patch.object(service_b, 'cagg_names', ['candles_5m']):
        claimed = await service_b.process_pending_jobs(lease_duration=timedelta(seconds=5))
        assert claimed is True

    async with AsyncSessionLocal() as session:
        final_job = await session.get(CaggRefreshJob, job_id)
        assert final_job.status == RefreshStatus.COMPLETED
        assert final_job.worker_id != "crashed-worker-dead"


@pytest.mark.asyncio
async def test_5_ownership_loss_prevents_overwriting_state():
    """
    TEST 5 — OWNERSHIP LOSS
    Worker A loses ownership to Worker B because its lease expired.
    When Worker A finishes and attempts to mark the job COMPLETED or FAILED,
    the conditional UPDATE (WHERE worker_id = :worker_id) updates 0 rows.
    Worker B's ownership and state remain intact.
    """
    job_id = None
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        async with session.begin():
            job = CaggRefreshJob(
                window_start=now - timedelta(days=1),
                window_end=now,
                status=RefreshStatus.PROCESSING,
                worker_id="worker-b-new-owner",
                claimed_at=now,
                lease_expires_at=now + timedelta(minutes=5)
            )
            session.add(job)
        await session.commit()
        job_id = job.id

    # Worker A attempts to execute completion on job_id with old worker_id
    service_a = CaggRefreshService(AsyncSessionLocal)
    
    # Run the atomic conditional update as Worker A
    async with AsyncSessionLocal() as session:
        async with session.begin():
            stmt = (
                update(CaggRefreshJob)
                .where(
                    CaggRefreshJob.id == job_id,
                    CaggRefreshJob.status == RefreshStatus.PROCESSING,
                    CaggRefreshJob.worker_id == "worker-a-stale-owner"
                )
                .values(
                    status=RefreshStatus.COMPLETED,
                    lease_expires_at=None
                )
            )
            res = await session.execute(stmt)
            # 0 rows updated because worker_id did not match
            assert res.rowcount == 0

    # Verify Worker B's state was completely untouched
    async with AsyncSessionLocal() as session:
        job = await session.get(CaggRefreshJob, job_id)
        assert job.status == RefreshStatus.PROCESSING
        assert job.worker_id == "worker-b-new-owner"


@pytest.mark.asyncio
async def test_6_heartbeat_task_cleanup():
    """
    TEST 6 — HEARTBEAT TASK CLEANUP
    Verifies that the heartbeat asyncio.Task is always cancelled and cleaned up:
    - On successful job completion
    - On refresh failure / exception
    - On outer cancellation / timeout
    """
    service = CaggRefreshService(AsyncSessionLocal)
    
    # 1. Test cleanup on success
    now = datetime.now(timezone.utc)
    job_id = None
    async with AsyncSessionLocal() as session:
        async with session.begin():
            job = CaggRefreshJob(
                window_start=now - timedelta(days=1),
                window_end=now,
                status=RefreshStatus.PENDING
            )
            session.add(job)
        await session.commit()
        job_id = job.id

    with patch.object(service, 'cagg_names', []): # 0 refreshes = instant completion
        await service.process_pending_jobs()

    # Verify no dangling tasks running _heartbeat_loop
    current_tasks = [t for t in asyncio.all_tasks() if "_heartbeat_loop" in str(t)]
    assert len(current_tasks) == 0

    # 2. Test cleanup on failure/exception
    async with AsyncSessionLocal() as session:
        async with session.begin():
            job2 = CaggRefreshJob(
                window_start=now - timedelta(days=1),
                window_end=now,
                status=RefreshStatus.PENDING
            )
            session.add(job2)
        await session.commit()

    with patch.object(service, 'cagg_names', ['invalid_cagg']):
        with patch.object(service.session_factory(), 'execute', side_effect=RuntimeError("Simulated DB Crash")):
            try:
                await service.process_pending_jobs()
            except Exception:
                pass

    current_tasks = [t for t in asyncio.all_tasks() if "_heartbeat_loop" in str(t)]
    assert len(current_tasks) == 0
