import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch
from app.services.ingestion import IngestionService
from app.services.ws_sharding.persistence_fence import ShardPersistenceFenceLostError
from app.models.sync_ranges import SyncRange
from app.connectors.exceptions import PayloadCorruptionError

@pytest.fixture
def mock_db():
    db = AsyncMock()
    # Mock commit, rollback, execute, flush
    db.commit = AsyncMock()
    db.rollback = AsyncMock()
    db.execute = AsyncMock()
    db.flush = AsyncMock()
    db.add = MagicMock()
    
    # Mock begin() context manager (synchronous method returning async context manager)
    begin_mock = AsyncMock()
    begin_mock.__aenter__.return_value = None
    begin_mock.__aexit__.return_value = None
    db.begin = MagicMock(return_value=begin_mock)
    
    return db

@pytest.fixture
def mock_binance():
    client = AsyncMock()
    
    # Create an async generator mock for get_klines
    async def mock_get_klines(*args, **kwargs):
        yield {
            "timestamp": datetime(2023, 1, 15, tzinfo=timezone.utc),
            "open": 100.0,
            "high": 105.0,
            "low": 95.0,
            "close": 101.0,
            "volume": 1000.0
        }
    
    client.get_klines = mock_get_klines
    return client

@pytest.fixture
def service(mock_db, mock_binance):
    return IngestionService(mock_db, mock_binance)

@pytest.mark.asyncio
async def test_detect_missing_ranges_empty(service, mock_db):
    # Mock empty result from DB
    mock_result = MagicMock()
    mock_result.scalars().all.return_value = []
    mock_db.execute.return_value = mock_result
    
    req_start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    req_end = datetime(2023, 1, 31, tzinfo=timezone.utc)
    
    gaps = await service.detect_missing_ranges(1, req_start, req_end)
    assert len(gaps) == 1
    assert gaps[0] == (req_start, req_end)


@pytest.mark.asyncio
async def test_commit_batch_runs_ownership_fence_inside_transaction_before_any_persistence(mock_binance):
    """A stale WebSocket owner cannot reach canonical persistence after its fence is lost."""
    entered_transaction = False

    class Transaction:
        async def __aenter__(self):
            nonlocal entered_transaction
            entered_transaction = True

        async def __aexit__(self, exc_type, exc, tb):
            return False

    db = AsyncMock()
    db.begin = MagicMock(return_value=Transaction())
    service = IngestionService(db, mock_binance)

    async def stale_owner_check():
        assert entered_transaction is True
        raise ShardPersistenceFenceLostError("successor generation is current")

    with pytest.raises(ShardPersistenceFenceLostError, match="successor generation"):
        await service._commit_batch(
            1,
            [{"asset_id": 1}],
            ownership_check=stale_owner_check,
        )

    # The ownership check fails before validation/upsert/coverage code can run.
    db.execute.assert_not_awaited()

@pytest.mark.asyncio
async def test_detect_missing_ranges_partial(service, mock_db):
    mock_range = SyncRange(
        asset_id=1,
        start_timestamp=datetime(2023, 1, 10, tzinfo=timezone.utc),
        end_timestamp=datetime(2023, 1, 20, tzinfo=timezone.utc)
    )
    
    mock_result = MagicMock()
    mock_result.scalars().all.return_value = [mock_range]
    mock_db.execute.return_value = mock_result
    
    req_start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    req_end = datetime(2023, 1, 31, tzinfo=timezone.utc)
    
    gaps = await service.detect_missing_ranges(1, req_start, req_end)
    
    assert len(gaps) == 2
    assert gaps[0] == (req_start, mock_range.start_timestamp)
    assert gaps[1] == (mock_range.end_timestamp, req_end)


@pytest.mark.asyncio
async def test_detect_missing_ranges_bounds_fragmented_metadata_conservatively(service, mock_db):
    """A pathological metadata fragment count cannot make detection unbounded.

    The uninspected suffix must be requested again, rather than being treated
    as healthy coverage merely because the query cap was reached.
    """
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc)
    ranges = [
        SyncRange(asset_id=1, start_timestamp=datetime(2026, 1, 1, 0, 1, tzinfo=timezone.utc), end_timestamp=datetime(2026, 1, 1, 0, 2, tzinfo=timezone.utc)),
        SyncRange(asset_id=1, start_timestamp=datetime(2026, 1, 1, 0, 3, tzinfo=timezone.utc), end_timestamp=datetime(2026, 1, 1, 0, 4, tzinfo=timezone.utc)),
        # Sentinel row: it proves the query was truncated at max_ranges=2.
        SyncRange(asset_id=1, start_timestamp=datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), end_timestamp=datetime(2026, 1, 1, 0, 6, tzinfo=timezone.utc)),
    ]
    result = MagicMock()
    result.scalars().all.return_value = ranges
    mock_db.execute.return_value = result

    gaps = await service.detect_missing_ranges(1, start, end, max_ranges=2, max_gaps=5)

    # Only three rows were ever materialized (the two-row cap plus sentinel),
    # and the uninspected suffix is conservatively returned as repair work.
    stmt = mock_db.execute.call_args.args[0]
    assert int(stmt._limit_clause.value) == 3
    assert gaps == [
        (start, ranges[0].start_timestamp),
        (ranges[0].end_timestamp, ranges[1].start_timestamp),
        (ranges[1].end_timestamp, end),
    ]


@pytest.mark.asyncio
async def test_detect_missing_ranges_coalesces_after_gap_cap_without_false_health(service, mock_db):
    start = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 1, 1, 0, 10, tzinfo=timezone.utc)
    ranges = [
        SyncRange(asset_id=1, start_timestamp=datetime(2026, 1, 1, 0, 2, tzinfo=timezone.utc), end_timestamp=datetime(2026, 1, 1, 0, 3, tzinfo=timezone.utc)),
        SyncRange(asset_id=1, start_timestamp=datetime(2026, 1, 1, 0, 5, tzinfo=timezone.utc), end_timestamp=datetime(2026, 1, 1, 0, 6, tzinfo=timezone.utc)),
        SyncRange(asset_id=1, start_timestamp=datetime(2026, 1, 1, 0, 8, tzinfo=timezone.utc), end_timestamp=datetime(2026, 1, 1, 0, 9, tzinfo=timezone.utc)),
    ]
    result = MagicMock()
    result.scalars().all.return_value = ranges
    mock_db.execute.return_value = result

    gaps = await service.detect_missing_ranges(1, start, end, max_ranges=3, max_gaps=2)

    # The detector never returns more than the configured bound.  Its last
    # interval intentionally expands through the requested end, which may
    # re-fetch known data but cannot suppress an unobserved gap.
    assert len(gaps) == 2
    assert gaps == [(start, ranges[0].start_timestamp), (ranges[0].end_timestamp, end)]

@pytest.mark.asyncio
async def test_update_sync_ranges_merge(service, mock_db):
    # Case A: 1 Jan to 10 Jan exists. New is 11 Jan to 20 Jan.
    mock_range = SyncRange(
        id=1,
        asset_id=1,
        start_timestamp=datetime(2023, 1, 1, tzinfo=timezone.utc),
        end_timestamp=datetime(2023, 1, 10, tzinfo=timezone.utc)
    )
    
    mock_result = MagicMock()
    mock_result.scalars().all.return_value = [mock_range]
    mock_db.execute.return_value = mock_result
    
    new_start = datetime(2023, 1, 11, tzinfo=timezone.utc)
    new_end = datetime(2023, 1, 20, tzinfo=timezone.utc)
    
    with patch.object(service, "_raw_block_is_complete", new_callable=AsyncMock, return_value=True):
        await service.update_sync_ranges(1, new_start, new_end)
    
    # Verify the new range added encompasses both
    service.db.add.assert_called_once()
    added_range = service.db.add.call_args[0][0]
    assert added_range.start_timestamp == mock_range.start_timestamp
    assert added_range.end_timestamp == new_end

from app.models.gap_staging_candles import GapStagingCandle

@pytest.mark.asyncio
async def test_commit_batch_fragments_gaps(service, mock_db):
    # Create payload with a gap
    # 10:00, 10:01, (gap 10:02), 10:03, 10:04
    t0 = datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc)
    t1 = datetime(2023, 1, 1, 10, 1, tzinfo=timezone.utc)
    t3 = datetime(2023, 1, 1, 10, 3, tzinfo=timezone.utc)
    t4 = datetime(2023, 1, 1, 10, 4, tzinfo=timezone.utc)
    
    candles = [
        {"timestamp": t0},
        {"timestamp": t1},
        {"timestamp": t3},
        {"timestamp": t4},
    ]
    
    with patch.object(service, 'insert_candle_batch', new_callable=AsyncMock) as mock_insert:
        with patch.object(service, 'update_sync_ranges', new_callable=AsyncMock) as mock_update:
            await service._commit_batch(1, candles)
            
            # Should bulk insert exactly once with all 4 candles routed to staging for historical timestamps
            mock_insert.assert_awaited_once_with(candles, target_model=GapStagingCandle)
            
            # Historical rows are staging-only.  They must not claim canonical
            # coverage before the bounded historical merge promotes them to raw.
            mock_update.assert_not_awaited()

@pytest.mark.asyncio
async def test_commit_batch_duplicate_timestamp(service, mock_db):
    t0 = datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc)
    
    candles = [
        {"timestamp": t0},
        {"timestamp": t0},  # Duplicate!
    ]
    
    with pytest.raises(PayloadCorruptionError, match="Payload corruption detected"):
        await service._commit_batch(1, candles)

@pytest.mark.asyncio
async def test_commit_batch_out_of_order(service, mock_db):
    t0 = datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc)
    t1 = datetime(2023, 1, 1, 10, 1, tzinfo=timezone.utc)
    t2 = datetime(2023, 1, 1, 10, 2, tzinfo=timezone.utc)
    
    candles = [
        {"timestamp": t0},
        {"timestamp": t2},  # 10:02
        {"timestamp": t1},  # 10:01 (Out of order!)
    ]
    
    with pytest.raises(PayloadCorruptionError, match="Payload corruption detected"):
        await service._commit_batch(1, candles)

@pytest.mark.asyncio
async def test_commit_batch_normal_contiguous(service, mock_db):
    t0 = datetime(2023, 1, 1, 10, 0, tzinfo=timezone.utc)
    t1 = datetime(2023, 1, 1, 10, 1, tzinfo=timezone.utc)
    t2 = datetime(2023, 1, 1, 10, 2, tzinfo=timezone.utc)
    
    candles = [
        {"timestamp": t0},
        {"timestamp": t1},
        {"timestamp": t2},
    ]
    
    with patch.object(service, 'insert_candle_batch', new_callable=AsyncMock) as mock_insert:
        with patch.object(service, 'update_sync_ranges', new_callable=AsyncMock) as mock_update:
            await service._commit_batch(1, candles)
            
            mock_insert.assert_awaited_once_with(candles, target_model=GapStagingCandle)
            # Old rows route to staging and never create sync coverage directly.
            mock_update.assert_not_awaited()


@pytest.mark.asyncio
async def test_sync_asset_releases_database_transaction_before_binance_iteration(service, mock_db):
    """No pooled PostgreSQL transaction is retained while waiting on Binance."""
    result = MagicMock()
    result.scalars().all.return_value = []
    mock_db.execute.return_value = result
    rollback_count_observed_during_network = []

    async def network_stream(*args, **kwargs):
        rollback_count_observed_during_network.append(mock_db.rollback.await_count)
        yield {
            "timestamp": datetime.now(timezone.utc).replace(second=0, microsecond=0),
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1.0,
        }

    service.client.get_klines = network_stream
    with patch.object(service, "_validate_binance_asset", new_callable=AsyncMock), \
         patch.object(service, "_commit_batch", new_callable=AsyncMock) as commit_batch:
        await service.sync_asset(
            asset_id=1,
            symbol="BTCUSDT",
            start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
            end_time=datetime(2026, 1, 2, tzinfo=timezone.utc),
        )

    assert rollback_count_observed_during_network == [1]
    commit_batch.assert_awaited_once()


@pytest.mark.asyncio
async def test_sync_asset_rejects_cross_exchange_or_unregistered_asset_before_network(service, mock_db):
    result = MagicMock()
    result.scalar_one_or_none.return_value = None
    mock_db.execute.return_value = result

    with patch.object(service.client, "get_klines", new_callable=AsyncMock) as get_klines:
        with pytest.raises(ValueError, match="not an active BINANCE asset"):
            await service.sync_asset(
                asset_id=99,
                symbol="BTCUSDT",
                start_time=datetime(2026, 1, 1, tzinfo=timezone.utc),
                end_time=datetime(2026, 1, 2, tzinfo=timezone.utc),
            )

    get_klines.assert_not_called()


@pytest.mark.asyncio
async def test_commit_batch_rejects_nonfinite_ohlcv_before_sync_metadata(service):
    candle = {
        "asset_id": 1,
        "timestamp": datetime.now(timezone.utc).replace(second=0, microsecond=0),
        "open": float("nan"),
        "high": 101.0,
        "low": 99.0,
        "close": 100.0,
        "volume": 1.0,
    }

    with pytest.raises(PayloadCorruptionError, match="finite"):
        await service._commit_batch(1, [candle])


@pytest.mark.asyncio
async def test_commit_failure_never_records_sync_coverage(service):
    """Coverage metadata is written only after the candle insert succeeds."""
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    candle = {
        "asset_id": 1,
        "timestamp": now,
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1.0,
    }

    with patch.object(service, "insert_candle_batch", new_callable=AsyncMock) as insert_batch, \
         patch.object(service, "update_sync_ranges", new_callable=AsyncMock) as update_ranges:
        insert_batch.side_effect = RuntimeError("database write failed")
        with pytest.raises(RuntimeError, match="database write failed"):
            await service._commit_batch(1, [candle])

    update_ranges.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_sync_range_update_rejects_unpersisted_coverage(service, mock_db):
    """No service caller can manufacture coverage metadata from intent alone."""
    start = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    end = datetime(2026, 9, 1, 12, 1, tzinfo=timezone.utc)

    with patch.object(service, "_raw_block_is_complete", new_callable=AsyncMock, return_value=False):
        with pytest.raises(RuntimeError, match="not fully present"):
            await service.update_sync_ranges(1, start, end)

    mock_db.add.assert_not_called()


@pytest.mark.asyncio
async def test_live_raw_batch_with_incomplete_database_block_never_claims_coverage(service):
    """An insert acknowledgement is not sufficient evidence of complete raw coverage."""
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    candle = {
        "asset_id": 1,
        "timestamp": now,
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1.0,
        "source": "binance_ws",
    }

    with patch.object(service, "insert_candle_batch", new_callable=AsyncMock), \
         patch.object(service, "_raw_block_is_complete", new_callable=AsyncMock, return_value=False), \
         patch.object(service, "_merge_verified_sync_ranges", new_callable=AsyncMock) as merge_range:
        await service._commit_batch(1, [candle])

    merge_range.assert_not_awaited()


@pytest.mark.asyncio
async def test_commit_batch_rejects_non_minute_timestamp_before_any_write(service, mock_db):
    candle = {
        "asset_id": 1,
        "timestamp": datetime(2026, 9, 1, 12, 0, 1, tzinfo=timezone.utc),
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1.0,
    }

    with pytest.raises(PayloadCorruptionError, match="align exactly"):
        await service._commit_batch(1, [candle])

    mock_db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_commit_raw_batch_is_available_to_fenced_workers_without_opening_nested_transaction(service):
    """The worker-facing API is bounded-page compatible and transaction-neutral."""
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    candle = {
        "asset_id": 1,
        "timestamp": now,
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 1.0,
        "source": "binance_rest",
    }

    with patch.object(service, "insert_candle_batch", new_callable=AsyncMock) as insert_batch, \
         patch.object(service, "_raw_block_is_complete", new_callable=AsyncMock, return_value=True), \
         patch.object(service, "_merge_verified_sync_ranges", new_callable=AsyncMock) as merge_range:
        await service.commit_raw_batch_in_transaction(1, [candle])

    service.db.begin.assert_not_called()
    insert_batch.assert_awaited_once()
    merge_range.assert_awaited_once_with(1, now, now)
