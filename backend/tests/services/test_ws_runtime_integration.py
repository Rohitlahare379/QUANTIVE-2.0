"""Runtime-level tests for the live WebSocket-to-persistence bridge."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from app.connectors.models import CandleEvent
from app.services.ws_sharding.lease import ShardLeaseClaim
from app.services.ws_sharding.registry import (
    AssetRegistryResolver,
    AssetRegistryUnavailableError,
)
from app.services.ws_sharding.runtime import ShardRuntime


def _final_candle() -> CandleEvent:
    timestamp = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    return CandleEvent(
        symbol="BTCUSDT",
        interval="1m",
        timestamp=timestamp,
        close_time=timestamp + timedelta(minutes=1) - timedelta(milliseconds=1),
        open=100.0,
        high=102.0,
        low=99.0,
        close=101.0,
        volume=5.0,
        is_closed=True,
    )


class FakeWebSocketClient:
    def __init__(self, events):
        self.events = events
        self.disconnect_called = False
        self._hold_open = asyncio.Event()

    async def stream_final_candles(self):
        for event in self.events:
            yield event
        await self._hold_open.wait()

    async def disconnect(self):
        self.disconnect_called = True
        self._hold_open.set()


class FailingSessionContext:
    async def __aenter__(self):
        raise RuntimeError("database unavailable")

    async def __aexit__(self, exc_type, exc, tb):
        return False


class UnavailableResolver:
    async def resolve_symbol(self, symbol, exchange="BINANCE"):
        raise AssetRegistryUnavailableError("registry refresh failed")


@pytest.mark.asyncio
async def test_owned_runtime_bridges_final_ws_candles_to_bounded_pipeline():
    """A shard-owned final candle reaches the existing persistence path exactly once."""
    resolver = AssetRegistryResolver()
    resolver.register_asset("BTCUSDT", asset_id=7)
    client = FakeWebSocketClient([_final_candle()])
    claim = ShardLeaseClaim(
        shard_id=2,
        worker_id="worker-a",
        claim_token="claim-a",
        claimed_at=datetime.now(timezone.utc),
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    runtime = ShardRuntime(
        shard_id=2,
        symbols=["BTCUSDT"],
        claim=claim,
        asset_resolver=resolver,
        websocket_client_factory=lambda **_: client,
    )

    await runtime.start()
    assert runtime.pipeline is not None
    runtime.pipeline.batch_size = 1
    runtime.pipeline.flush_interval_seconds = 0.01

    try:
        for _ in range(50):
            if runtime.pipeline.metrics.candles_persisted == 1:
                break
            await asyncio.sleep(0.01)

        assert runtime.pipeline.metrics.candles_received == 1
        assert runtime.pipeline.metrics.candles_persisted == 1
        assert runtime.pipeline.metrics.duplicate_candles == 0
    finally:
        await runtime.stop()

    assert client.disconnect_called is True


@pytest.mark.asyncio
async def test_fenced_runtime_cancels_ws_consumer_and_never_flushes_stale_data():
    """Lease loss tears down both the source stream and bounded pipeline immediately."""
    resolver = AssetRegistryResolver()
    resolver.register_asset("BTCUSDT", asset_id=7)
    client = FakeWebSocketClient([])
    claim = ShardLeaseClaim(
        shard_id=2,
        worker_id="worker-a",
        claim_token="claim-a",
        claimed_at=datetime.now(timezone.utc),
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    runtime = ShardRuntime(
        shard_id=2,
        symbols=["BTCUSDT"],
        claim=claim,
        asset_resolver=resolver,
        websocket_client_factory=lambda **_: client,
    )
    await runtime.start()
    assert runtime._stream_task is not None

    runtime.fence("lease lost")
    await runtime.stop()

    assert runtime.is_fenced is True
    assert runtime.pipeline.queue_size == 0
    assert runtime._stream_task is None
    assert client.disconnect_called is True


@pytest.mark.asyncio
async def test_expired_registry_cache_fails_closed_instead_of_reusing_stale_mapping():
    """A registry outage cannot keep a disabled/reassigned cached asset live."""
    resolver = AssetRegistryResolver(session_factory=lambda: FailingSessionContext())
    resolver.register_asset("BTCUSDT", asset_id=7)
    resolver.invalidate()

    with pytest.raises(AssetRegistryUnavailableError, match="refusing stale symbol attribution"):
        await resolver.resolve_symbol("BTCUSDT")


@pytest.mark.asyncio
async def test_registry_failure_fences_owned_runtime_and_stops_new_ingestion():
    """The registry boundary is fatal: it stops the WS source instead of writing stale attribution."""
    claim = ShardLeaseClaim(
        shard_id=2,
        worker_id="worker-a",
        claim_token="claim-a",
        claimed_at=datetime.now(timezone.utc),
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    runtime = ShardRuntime(
        shard_id=2,
        symbols=["BTCUSDT"],
        claim=claim,
        asset_resolver=UnavailableResolver(),
    )
    await runtime.start()
    assert runtime.pipeline is not None
    runtime.pipeline.batch_size = 1
    runtime.pipeline.flush_interval_seconds = 0.01

    try:
        assert await runtime.enqueue_candle(_final_candle()) is True
        for _ in range(50):
            if runtime.is_fenced:
                break
            await asyncio.sleep(0.01)

        assert runtime.is_fenced is True
        assert runtime.pipeline.metrics.persistence_errors == 1
        assert await runtime.enqueue_candle(_final_candle()) is False
    finally:
        await runtime.stop()
