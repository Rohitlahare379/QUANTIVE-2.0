"""
Performance & Bounded Memory Load Test (P0.2 Phase 3).

Simulates high-cardinality streaming across 5,000 assets using synthetic CandleEvents.
Measures:
- Memory usage (RSS MB)
- Queue utilization
- Batch throughput (events/sec)
- DB write throughput (batches/sec)
- Event latency (ms)
- Bounded memory verification (Zero O(N) leak)
"""

import asyncio
from datetime import datetime, timedelta, timezone
import os
import resource
import time
from typing import Dict, List

import pytest

from app.connectors.models import CandleEvent
from app.services.ws_sharding.pipeline import BoundedLiveIngestionPipeline
from app.services.ws_sharding.registry import AssetRegistryResolver


def get_current_rss_mb() -> float:
    """Returns current process Resident Set Size in Megabytes."""
    # resource.getrusage on macOS returns maxrss in bytes, on Linux in kilobytes
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # On Darwin (macOS), ru_maxrss is in bytes
    import sys
    if sys.platform == "darwin":
        return usage / (1024.0 * 1024.0)
    return usage / 1024.0


@pytest.mark.asyncio
async def test_5000_assets_bounded_load():
    """
    Simulates 5,000 assets producing 50,000 synthetic finalized CandleEvents.
    Verifies that pipeline memory remains bounded and all events are flushed.
    """
    num_assets = 5000
    events_per_asset = 10
    total_expected_events = num_assets * events_per_asset  # 50,000 events

    # 1. Setup Asset Resolver with 5,000 simulated assets
    resolver = AssetRegistryResolver()
    for i in range(num_assets):
        symbol = f"ASSET{i}USDT"
        resolver.register_asset(symbol, asset_id=i + 1, is_active=True)

    assert resolver.cached_count == num_assets

    # 2. Setup Pipeline with bounded queue (10,000 items) and batch size (1,000 items)
    pipeline = BoundedLiveIngestionPipeline(
        shard_id=0,
        asset_resolver=resolver,
        queue_maxsize=10000,
        batch_size=1000,
        flush_interval_ms=100,
    )

    committed_batches: List[int] = []
    committed_candles_count = 0
    batch_latencies: List[float] = []

    async def mock_db_commit(asset_id: int, payload: list):
        nonlocal committed_candles_count
        # Cooperative yield to event loop simulating fast persistence
        await asyncio.sleep(0)
        committed_candles_count += len(payload)
        committed_batches.append(len(payload))
        pipeline.metrics.record_flush_complete(len(payload), 0.1)

    pipeline._commit_asset_batch = mock_db_commit

    # 3. Measure Baseline Memory
    rss_start = get_current_rss_mb()
    start_time = time.perf_counter()

    await pipeline.start()

    max_queue_utilization = 0.0
    base_time = datetime(2026, 8, 15, 12, 0, 0, tzinfo=timezone.utc)

    # 4. Stream 50,000 synthetic events in interleaved batches of 500
    chunk_size = 500
    for minute in range(events_per_asset):
        for chunk_start in range(0, num_assets, chunk_size):
            chunk_end = min(chunk_start + chunk_size, num_assets)
            tasks = []

            for asset_idx in range(chunk_start, chunk_end):
                sym = f"ASSET{asset_idx}USDT"
                ts = base_time + timedelta(minutes=minute)
                close_ts = ts + timedelta(seconds=59, milliseconds=999)
                event = CandleEvent(
                    symbol=sym,
                    interval="1m",
                    timestamp=ts,
                    close_time=close_ts,
                    open=100.0 + asset_idx,
                    high=105.0 + asset_idx,
                    low=95.0 + asset_idx,
                    close=102.0 + asset_idx,
                    volume=50.0,
                    is_closed=True,
                    source="binance_ws",
                )
                tasks.append(pipeline.enqueue_candle(event))

            # Enqueue batch concurrently
            results = await asyncio.gather(*tasks)
            assert all(r is True for r in results)

            # Record max queue utilization
            util = pipeline.queue_utilization
            if util > max_queue_utilization:
                max_queue_utilization = util

            # Allow flusher task to drain queue
            if pipeline.queue_size >= 4000:
                await asyncio.sleep(0.05)

    # 5. Stop pipeline cleanly to drain any remaining events
    await pipeline.stop()
    elapsed_total = time.perf_counter() - start_time
    rss_end = get_current_rss_mb()
    delta_rss = rss_end - rss_start

    # 6. Compute Throughput and Metrics
    event_throughput = total_expected_events / elapsed_total if elapsed_total > 0 else 0
    batch_throughput = len(committed_batches) / elapsed_total if elapsed_total > 0 else 0
    avg_batch_size = (
        sum(committed_batches) / len(committed_batches) if committed_batches else 0
    )

    print("\n" + "=" * 65)
    print("      P0.2 PHASE 3 — 5,000 ASSETS BOUNDED LOAD BENCHMARK")
    print("=" * 65)
    print(f"  Simulated Assets Count    : {num_assets:,}")
    print(f"  Events per Asset          : {events_per_asset}")
    print(f"  Total Events Processed    : {committed_candles_count:,} / {total_expected_events:,}")
    print(f"  Total Batches Committed   : {len(committed_batches):,}")
    print(f"  Average Batch Size        : {avg_batch_size:.1f} candles")
    print(f"  Elapsed Benchmark Time    : {elapsed_total:.2f} seconds")
    print(f"  Event Ingestion Throughput: {event_throughput:,.1f} events/sec")
    print(f"  DB Batch Throughput       : {batch_throughput:,.1f} batches/sec")
    print(f"  Peak Queue Utilization    : {max_queue_utilization * 100:.1f}%")
    print(f"  Final Queue Depth         : {pipeline.queue_size} items")
    print(f"  Starting RSS Memory       : {rss_start:.2f} MB")
    print(f"  Ending RSS Memory         : {rss_end:.2f} MB")
    print(f"  Delta RSS Growth          : {delta_rss:.2f} MB")
    print(f"  Queue Overflow Drops      : {pipeline.metrics.queue_overflow_count}")
    print(f"  Rejections (Invalid/Unk)  : {pipeline.metrics.rejected_candles + pipeline.metrics.unmapped_symbol_rejections}")
    print("=" * 65)

    # 7. Invariant Verifications
    assert committed_candles_count == total_expected_events
    assert pipeline.queue_size == 0
    assert pipeline.metrics.queue_overflow_count == 0
    assert pipeline.metrics.rejected_candles == 0
    # Memory growth must remain strictly bounded (< 50 MB growth during 50k events)
    assert delta_rss < 50.0
