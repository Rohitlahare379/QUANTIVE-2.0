"""Real-PostgreSQL concurrency regressions for durable maintenance workers.

These tests intentionally use the configured PostgreSQL database.  They are not
mock evidence: run them against the Docker Timescale/PostgreSQL service after
``alembic upgrade head``.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.models.asset_registry import AssetRegistry
from app.models.cagg_refresh_jobs import CaggRefreshJob, RefreshStatus
from app.models.gap_repair_jobs import GapRepairJob, GapRepairStatus
from app.models.historical_merge_jobs import HistoricalMergeJob
from app.services.gap_repair import GapRepairService
from app.services.cagg_refresh import schedule_cagg_refresh_jobs


pytestmark = [pytest.mark.postgres, pytest.mark.timescaledb]


engine = create_async_engine(settings.sqlalchemy_database_uri, poolclass=NullPool)
Session = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


@pytest.fixture(autouse=True)
async def clean_durable_jobs():
    async with Session() as session:
        await session.execute(delete(CaggRefreshJob))
        await session.execute(delete(GapRepairJob))
        await session.execute(delete(HistoricalMergeJob))
        await session.execute(delete(AssetRegistry))
        await session.commit()
    yield
    async with Session() as session:
        await session.execute(delete(CaggRefreshJob))
        await session.execute(delete(GapRepairJob))
        await session.execute(delete(HistoricalMergeJob))
        await session.execute(delete(AssetRegistry))
        await session.commit()


async def _asset_id() -> int:
    async with Session() as session:
        asset = AssetRegistry(symbol="BTCUSDT", exchange="BINANCE", asset_type="SPOT", is_active=True)
        session.add(asset)
        await session.commit()
        return asset.id


@pytest.mark.asyncio
async def test_concurrent_claim_after_crash_issues_one_fresh_fencing_token():
    asset_id = await _asset_id()
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(minutes=10)
    service = GapRepairService(Session)
    job = await service.schedule_repair_job(asset_id, "BTCUSDT", start, end)
    assert job is not None
    old = await service.claim_job("crashed", timedelta(milliseconds=25))
    assert old is not None and old.lease_token
    await asyncio.sleep(0.04)

    claims = await asyncio.gather(
        service.claim_job("recovery-a", timedelta(minutes=1)),
        service.claim_job("recovery-b", timedelta(minutes=1)),
    )
    reclaimed = [claim for claim in claims if claim is not None]

    assert len(reclaimed) == 1
    assert reclaimed[0].lease_token != old.lease_token
    assert reclaimed[0].worker_id in {"recovery-a", "recovery-b"}


@pytest.mark.asyncio
async def test_concurrent_overlapping_schedule_has_no_duplicate_active_work():
    asset_id = await _asset_id()
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    first_end = start + timedelta(minutes=30)
    final_end = start + timedelta(minutes=40)
    service_a = GapRepairService(Session)
    service_b = GapRepairService(Session)

    await asyncio.gather(
        service_a.schedule_repair_jobs(asset_id, "BTCUSDT", start, first_end),
        service_b.schedule_repair_jobs(
            asset_id, "BTCUSDT", start + timedelta(minutes=10), final_end
        ),
    )

    async with Session() as session:
        jobs = (
            await session.execute(
                select(GapRepairJob)
                .where(GapRepairJob.status.in_((GapRepairStatus.PENDING, GapRepairStatus.PROCESSING)))
                .order_by(GapRepairJob.start_time)
            )
        ).scalars().all()

    assert jobs
    assert min(job.start_time for job in jobs) == start
    assert max(job.end_time for job in jobs) == final_end
    for previous, current in zip(jobs, jobs[1:]):
        assert previous.end_time <= current.start_time


@pytest.mark.asyncio
async def test_concurrent_cagg_schedule_coalesces_overlapping_windows():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    first_end = start + timedelta(days=1)
    final_end = start + timedelta(days=2)

    async def schedule(window_start, window_end):
        async with Session() as session:
            async with session.begin():
                return await schedule_cagg_refresh_jobs(session, window_start, window_end)

    await asyncio.gather(
        schedule(start, first_end),
        schedule(start + timedelta(hours=12), final_end),
    )

    async with Session() as session:
        jobs = (
            await session.execute(
                select(CaggRefreshJob)
                .where(CaggRefreshJob.status.in_((RefreshStatus.PENDING, RefreshStatus.PROCESSING)))
                .order_by(CaggRefreshJob.window_start)
            )
        ).scalars().all()

    assert jobs
    assert min(job.window_start for job in jobs) == start
    assert max(job.window_end for job in jobs) == final_end
    for previous, current in zip(jobs, jobs[1:]):
        assert previous.window_end <= current.window_start
