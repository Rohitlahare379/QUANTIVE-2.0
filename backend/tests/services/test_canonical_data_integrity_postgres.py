"""Infrastructure-backed invariants for canonical candle data.

These tests intentionally require PostgreSQL + TimescaleDB and migration 014.  They
are not mocked proof: run them against the Docker database before release.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.config import settings
from app.models.asset_registry import AssetRegistry
from app.models.candle_revision import CandleRevision
from app.models.gap_staging_candles import GapStagingCandle
from app.models.raw_1m_candles import Raw1mCandle
from app.models.sync_ranges import SyncRange
from app.services.ingestion import IngestionService


pytestmark = [pytest.mark.postgres, pytest.mark.timescaledb]

engine = create_async_engine(settings.sqlalchemy_database_uri, poolclass=NullPool)
Session = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


def candle(asset_id: int, timestamp: datetime, *, source: str, close: float = 100.5) -> dict:
    return {
        "asset_id": asset_id,
        "timestamp": timestamp,
        "open": 100.0,
        "high": max(101.0, close),
        "low": 99.0,
        "close": close,
        "volume": 10.0,
        "source": source,
        "source_event_time": timestamp + timedelta(seconds=59),
        "source_received_at": datetime.now(timezone.utc),
    }


@pytest.fixture(autouse=True)
async def clean_canonical_tables():
    async with Session() as session:
        await session.execute(delete(Raw1mCandle))
        await session.execute(delete(GapStagingCandle))
        await session.execute(delete(SyncRange))
        await session.execute(delete(AssetRegistry))
        await session.commit()
    yield
    async with Session() as session:
        await session.execute(delete(Raw1mCandle))
        await session.execute(delete(GapStagingCandle))
        await session.execute(delete(SyncRange))
        await session.execute(delete(AssetRegistry))
        await session.commit()


async def create_asset(symbol: str = "BTCUSDT", exchange: str = "BINANCE") -> int:
    async with Session() as session:
        asset = AssetRegistry(symbol=symbol, exchange=exchange, asset_type="SPOT", is_active=True)
        session.add(asset)
        await session.commit()
        await session.refresh(asset)
        return asset.id


@pytest.mark.asyncio
async def test_staged_history_never_claims_coverage_until_promoted_to_raw():
    asset_id = await create_asset()
    timestamp = (datetime.now(timezone.utc) - timedelta(days=8)).replace(second=0, microsecond=0)
    staged = candle(asset_id, timestamp, source="binance_rest")

    async with Session() as session:
        await IngestionService(session)._commit_batch(asset_id, [staged])
        await session.commit()

    async with Session() as session:
        assert (await session.execute(select(func.count()).select_from(GapStagingCandle))).scalar_one() == 1
        assert (await session.execute(select(func.count()).select_from(Raw1mCandle))).scalar_one() == 0
        assert (await session.execute(select(func.count()).select_from(SyncRange))).scalar_one() == 0

    # This is the bounded historical-merge write primitive.  Promotion and its
    # coverage assertion occur in one transaction.
    async with Session() as session:
        async with session.begin():
            await IngestionService(session).commit_raw_batch_in_transaction(asset_id, [staged])

    async with Session() as session:
        assert (await session.execute(select(func.count()).select_from(Raw1mCandle))).scalar_one() == 1
        assert (await session.execute(select(func.count()).select_from(SyncRange))).scalar_one() == 1
        assert (await session.execute(select(func.count()).select_from(CandleRevision))).scalar_one() == 1


@pytest.mark.asyncio
async def test_rest_correction_is_versioned_and_late_websocket_replay_cannot_erase_it():
    asset_id = await create_asset()
    timestamp = (datetime.now(timezone.utc) - timedelta(minutes=5)).replace(second=0, microsecond=0)

    async with Session() as session:
        await IngestionService(session)._commit_batch(
            asset_id, [candle(asset_id, timestamp, source="binance_ws", close=100.5)]
        )
        await session.commit()

    async with Session() as session:
        await IngestionService(session)._commit_batch(
            asset_id, [candle(asset_id, timestamp, source="binance_rest", close=101.0)]
        )
        await session.commit()

    # A delayed lower-precedence websocket value must be a no-op.
    async with Session() as session:
        await IngestionService(session)._commit_batch(
            asset_id, [candle(asset_id, timestamp, source="binance_ws", close=100.25)]
        )
        await session.commit()

    async with Session() as session:
        raw = await session.get(Raw1mCandle, {"asset_id": asset_id, "timestamp": timestamp})
        revisions = (
            await session.execute(
                select(CandleRevision)
                .where(CandleRevision.asset_id == asset_id, CandleRevision.timestamp == timestamp)
                .order_by(CandleRevision.data_revision)
            )
        ).scalars().all()
        assert raw is not None
        assert raw.source == "binance_rest"
        assert raw.close == 101.0
        assert raw.data_revision == 2
        assert [(row.data_revision, row.source, row.close) for row in revisions] == [
            (1, "binance_ws", 100.5),
            (2, "binance_rest", 101.0),
        ]


@pytest.mark.asyncio
async def test_concurrent_websocket_and_rest_correction_has_deterministic_canonical_winner():
    """Real concurrent sessions prove source precedence independently of commit order."""
    asset_id = await create_asset()
    timestamp = (datetime.now(timezone.utc) - timedelta(minutes=6)).replace(second=0, microsecond=0)

    async def persist(source: str, close: float) -> None:
        async with Session() as session:
            await IngestionService(session)._commit_batch(
                asset_id, [candle(asset_id, timestamp, source=source, close=close)]
            )
            await session.commit()

    await asyncio.gather(
        persist("binance_ws", 100.5),
        persist("binance_rest", 101.0),
    )

    async with Session() as session:
        raw = await session.get(Raw1mCandle, {"asset_id": asset_id, "timestamp": timestamp})
        assert raw is not None
        assert raw.source == "binance_rest"
        assert raw.close == 101.0
        # Depending on which transaction won the initial insert, there is either
        # one REST revision or a WS insert followed by REST correction.
        assert raw.data_revision in (1, 2)


@pytest.mark.asyncio
async def test_database_constraints_and_raw_delete_cannot_leave_false_coverage():
    asset_id = await create_asset()
    timestamp = (datetime.now(timezone.utc) - timedelta(minutes=4)).replace(second=0, microsecond=0)

    async with Session() as session:
        await IngestionService(session)._commit_batch(
            asset_id, [candle(asset_id, timestamp, source="binance_ws")]
        )
        await session.commit()

    # No ORM/direct-SQL caller can manufacture a coverage assertion with a
    # missing minute; the trigger independently checks canonical raw storage.
    async with Session() as session:
        session.add(
            SyncRange(
                asset_id=asset_id,
                start_timestamp=timestamp - timedelta(minutes=1),
                end_timestamp=timestamp + timedelta(minutes=1),
            )
        )
        with pytest.raises(DBAPIError, match="sync_ranges can only claim complete"):
            await session.commit()
        await session.rollback()

    # The database, not only the service validator, rejects a non-minute raw row.
    async with Session() as session:
        session.add(
            Raw1mCandle(
                asset_id=asset_id,
                timestamp=timestamp + timedelta(seconds=1),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1.0,
                source="binance_ws",
                data_revision=1,
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

    async with Session() as session:
        session.add(
            Raw1mCandle(
                asset_id=asset_id,
                timestamp=timestamp + timedelta(minutes=1),
                open=100.0,
                high=101.0,
                low=99.0,
                close=100.5,
                volume=1.0,
                source="binance_ws",
                data_revision=2,
            )
        )
        with pytest.raises(DBAPIError, match="initial canonical candle revision"):
            await session.commit()
        await session.rollback()

    # Direct writers cannot falsify accepted provenance without a higher- or
    # equal-priority revision.  This reaches the migration trigger rather than
    # the service-level validator.
    async with Session() as session:
        with pytest.raises(DBAPIError, match="lower-priority"):
            await session.execute(
                update(Raw1mCandle)
                .where(Raw1mCandle.asset_id == asset_id, Raw1mCandle.timestamp == timestamp)
                .values(source="legacy", data_revision=2)
            )
            await session.commit()
        await session.rollback()

    # Moving a canonical primary-key row would invalidate an existing coverage
    # assertion without firing DELETE, so raw identity is immutable.
    async with Session() as session:
        with pytest.raises(DBAPIError, match="identity is immutable"):
            await session.execute(
                update(Raw1mCandle)
                .where(Raw1mCandle.asset_id == asset_id, Raw1mCandle.timestamp == timestamp)
                .values(timestamp=timestamp + timedelta(minutes=2))
            )
            await session.commit()
        await session.rollback()

    # Any raw deletion invalidates the affected sync claim rather than leaving
    # the query layer with a false statement of data availability.
    async with Session() as session:
        raw = await session.get(Raw1mCandle, {"asset_id": asset_id, "timestamp": timestamp})
        await session.delete(raw)
        await session.commit()

    async with Session() as session:
        assert (await session.execute(select(func.count()).select_from(Raw1mCandle))).scalar_one() == 0
        assert (await session.execute(select(func.count()).select_from(SyncRange))).scalar_one() == 0


@pytest.mark.asyncio
async def test_concurrent_raw_delete_waits_for_coverage_assertion_then_invalidates_it():
    """A raw delete cannot race past an uncommitted verified sync range.

    This is deliberately a real PostgreSQL/TimescaleDB concurrency regression.
    The range insert holds the per-asset trigger advisory lock.  A concurrent
    delete reaches its row trigger but cannot return until the range transaction
    commits; it then sees and removes the just-committed coverage assertion.
    """
    asset_id = await create_asset("SERIALBTC")
    timestamp = (datetime.now(timezone.utc) - timedelta(minutes=11)).replace(
        second=0, microsecond=0
    )

    async with Session() as session:
        async with session.begin():
            await IngestionService(session).commit_raw_batch_in_transaction(
                asset_id, [candle(asset_id, timestamp, source="binance_ws")]
            )
            # Establish the raw row without retaining the service-created range;
            # the concurrently held range below is the assertion under test.
            await session.execute(delete(SyncRange).where(SyncRange.asset_id == asset_id))

    range_created = asyncio.Event()
    allow_range_commit = asyncio.Event()
    delete_attempted = asyncio.Event()
    delete_completed = asyncio.Event()

    async def create_coverage_assertion() -> None:
        async with Session() as session:
            async with session.begin():
                session.add(
                    SyncRange(
                        asset_id=asset_id,
                        start_timestamp=timestamp,
                        end_timestamp=timestamp,
                    )
                )
                await session.flush()
                range_created.set()
                await allow_range_commit.wait()

    async def delete_raw_candle() -> None:
        async with Session() as session:
            async with session.begin():
                delete_attempted.set()
                await session.execute(
                    delete(Raw1mCandle).where(
                        Raw1mCandle.asset_id == asset_id,
                        Raw1mCandle.timestamp == timestamp,
                    )
                )
                delete_completed.set()

    range_task = asyncio.create_task(create_coverage_assertion())
    await asyncio.wait_for(range_created.wait(), timeout=5)
    delete_task = asyncio.create_task(delete_raw_candle())
    await asyncio.wait_for(delete_attempted.wait(), timeout=5)
    # The delete has acquired its raw-row lock, but its row trigger must block
    # on the coverage transaction's advisory lock rather than committing a
    # delete that cannot yet see the new range.
    await asyncio.sleep(0.1)
    assert not delete_completed.is_set()

    allow_range_commit.set()
    await asyncio.wait_for(asyncio.gather(range_task, delete_task), timeout=10)

    async with Session() as session:
        raw_count = (
            await session.execute(select(func.count()).select_from(Raw1mCandle))
        ).scalar_one()
        range_count = (
            await session.execute(select(func.count()).select_from(SyncRange))
        ).scalar_one()
        assert raw_count == 0
        assert range_count == 0


@pytest.mark.asyncio
async def test_candle_revision_ledger_rejects_direct_mutation_but_allows_asset_cleanup():
    """A forensic revision cannot be rewritten or erased by direct SQL."""
    asset_id = await create_asset()
    timestamp = (datetime.now(timezone.utc) - timedelta(minutes=7)).replace(second=0, microsecond=0)

    async with Session() as session:
        await IngestionService(session)._commit_batch(
            asset_id, [candle(asset_id, timestamp, source="binance_ws")]
        )
        await session.commit()

    async with Session() as session:
        with pytest.raises(DBAPIError, match="candle_revisions is append-only"):
            await session.execute(
                update(CandleRevision)
                .where(CandleRevision.asset_id == asset_id)
                .values(close=101.0)
            )
            await session.commit()
        await session.rollback()

    async with Session() as session:
        with pytest.raises(DBAPIError, match="candle_revisions is append-only"):
            await session.execute(delete(CandleRevision).where(CandleRevision.asset_id == asset_id))
            await session.commit()
        await session.rollback()

    # The append-only trigger intentionally permits only a parent asset's FK
    # cascade, so normal lifecycle cleanup cannot strand a foreign-key row.
    async with Session() as session:
        await session.execute(delete(AssetRegistry).where(AssetRegistry.id == asset_id))
        await session.commit()

    async with Session() as session:
        assert (await session.execute(select(func.count()).select_from(CandleRevision))).scalar_one() == 0


@pytest.mark.asyncio
async def test_reingesting_an_operationally_deleted_raw_candle_advances_the_ledger():
    """Raw deletion invalidates coverage but cannot cause a revision-number reset."""
    asset_id = await create_asset()
    timestamp = (datetime.now(timezone.utc) - timedelta(minutes=9)).replace(second=0, microsecond=0)

    async with Session() as session:
        await IngestionService(session)._commit_batch(
            asset_id, [candle(asset_id, timestamp, source="binance_ws", close=100.5)]
        )
        await session.commit()

    async with Session() as session:
        raw = await session.get(Raw1mCandle, {"asset_id": asset_id, "timestamp": timestamp})
        assert raw is not None
        await session.delete(raw)
        await session.commit()

    async with Session() as session:
        await IngestionService(session)._commit_batch(
            asset_id, [candle(asset_id, timestamp, source="binance_rest", close=101.0)]
        )
        await session.commit()

    async with Session() as session:
        raw = await session.get(Raw1mCandle, {"asset_id": asset_id, "timestamp": timestamp})
        revisions = (
            await session.execute(
                select(CandleRevision.data_revision, CandleRevision.source, CandleRevision.close)
                .where(CandleRevision.asset_id == asset_id, CandleRevision.timestamp == timestamp)
                .order_by(CandleRevision.data_revision)
            )
        ).all()
        assert raw is not None
        assert raw.data_revision == 2
        assert (raw.source, raw.close) == ("binance_rest", 101.0)
        assert revisions == [(1, "binance_ws", 100.5), (2, "binance_rest", 101.0)]
