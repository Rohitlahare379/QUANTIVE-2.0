"""
Tests for WebSocket Shard Manager & Distributed Lease (P0.2 Phase 1).

Covers all 15 required verification scenarios:
1. Deterministic symbol → shard assignment
2. Same symbol produces same shard across calls
3. Different symbols distribute reasonably
4. Worker A acquires free shard
5. Worker B cannot acquire owned shard
6. Worker A heartbeat successfully renews lease
7. Worker B cannot renew Worker A's lease
8. Worker A loses ownership after TTL expiration
9. Worker B acquires expired shard
10. Old Worker A cannot delete Worker B's lease
11. Old Worker A cannot renew Worker B's lease
12. Redis unavailable causes shard shutdown/fail-closed
13. Clean shutdown releases only the owner's lease
14. Heartbeat task cleanup
15. Supervisor stops runtime after ownership loss
"""

import asyncio
from datetime import datetime, timezone, timedelta
import fakeredis.aioredis
import pytest
import pytest_asyncio

from app.services.ws_sharding.assignment import (
    assign_symbols_to_shards,
    get_shard_for_symbol,
    get_symbols_for_shard,
    normalize_symbol,
)
from app.services.ws_sharding.lease import (
    RedisUnavailableError,
    ShardLeaseClaim,
    ShardLeaseManager,
    generate_worker_id,
)
from app.services.ws_sharding.runtime import ShardRuntime, ShardRuntimeState
from app.services.ws_sharding.supervisor import ShardSupervisor


@pytest_asyncio.fixture
async def fake_redis():
    """Provides an isolated in-memory Redis instance with Lua engine support."""
    client = fakeredis.aioredis.FakeRedis()
    yield client
    await client.flushall()
    await client.aclose()


# ============================================================================
# 1. Deterministic Symbol → Shard Assignment Tests
# ============================================================================

def test_1_deterministic_symbol_to_shard_assignment():
    """1. Deterministic symbol → shard assignment behaves as expected."""
    num_shards = 8
    # BTCUSDT maps to a stable shard
    shard = get_shard_for_symbol("BTCUSDT", num_shards=num_shards)
    assert 0 <= shard < num_shards
    assert isinstance(shard, int)

    # Edge cases
    with pytest.raises(ValueError, match="num_shards must be a positive integer"):
        get_shard_for_symbol("BTCUSDT", num_shards=0)

    with pytest.raises(ValueError, match="Symbol cannot be empty"):
        get_shard_for_symbol("   ", num_shards=num_shards)


def test_2_same_symbol_produces_same_shard_across_calls():
    """2. Same symbol produces same shard across calls regardless of casing and spacing."""
    num_shards = 16
    expected = get_shard_for_symbol("ETHUSDT", num_shards=num_shards)

    assert get_shard_for_symbol("ethusdt", num_shards=num_shards) == expected
    assert get_shard_for_symbol("  ETHUSDT  ", num_shards=num_shards) == expected
    assert get_shard_for_symbol("EthUsdt", num_shards=num_shards) == expected

    # 100 repeated calls verify determinism without state mutation
    for _ in range(100):
        assert get_shard_for_symbol("ETHUSDT", num_shards=num_shards) == expected


def test_3_different_symbols_distribute_reasonably():
    """3. Different symbols distribute reasonably across shards."""
    num_shards = 8
    test_symbols = [
        f"COIN_{i}_USDT" for i in range(200)
    ]
    distribution = assign_symbols_to_shards(test_symbols, num_shards=num_shards)

    # Every shard must have at least some symbols assigned (uniform hashing)
    assert len(distribution) == num_shards
    for shard_id, syms in distribution.items():
        assert len(syms) > 0, f"Shard {shard_id} received 0 symbols"

    # Verify get_symbols_for_shard filter matches distribution
    for shard_id in range(num_shards):
        filtered = get_symbols_for_shard(test_symbols, shard_id, num_shards=num_shards)
        assert filtered == distribution[shard_id]


# ============================================================================
# 4 - 11. Distributed Lease & Atomic Operations Tests
# ============================================================================

@pytest.mark.asyncio
async def test_4_worker_a_acquires_free_shard(fake_redis):
    """4. Worker A acquires free shard."""
    mgr = ShardLeaseManager(redis_client=fake_redis, lease_ttl_seconds=10.0)
    claim = await mgr.acquire_shard_lease(shard_id=0, worker_id="worker_a")

    assert claim is not None
    assert claim.shard_id == 0
    assert claim.worker_id == "worker_a"
    assert claim.claim_token is not None

    # Verify Redis key contents
    owner = await mgr.get_current_owner(shard_id=0)
    assert owner is not None
    assert owner.worker_id == "worker_a"
    assert owner.claim_token == claim.claim_token


@pytest.mark.asyncio
async def test_5_worker_b_cannot_acquire_owned_shard(fake_redis):
    """5. Worker B cannot acquire owned shard."""
    mgr = ShardLeaseManager(redis_client=fake_redis, lease_ttl_seconds=10.0)
    claim_a = await mgr.acquire_shard_lease(shard_id=0, worker_id="worker_a")
    assert claim_a is not None

    claim_b = await mgr.acquire_shard_lease(shard_id=0, worker_id="worker_b")
    assert claim_b is None

    # Verify Worker A remains sole owner
    owner = await mgr.get_current_owner(shard_id=0)
    assert owner.worker_id == "worker_a"
    assert owner.claim_token == claim_a.claim_token


@pytest.mark.asyncio
async def test_6_worker_a_heartbeat_successfully_renews_lease(fake_redis):
    """6. Worker A heartbeat successfully renews lease."""
    mgr = ShardLeaseManager(redis_client=fake_redis, lease_ttl_seconds=5.0)
    claim_a = await mgr.acquire_shard_lease(shard_id=1, worker_id="worker_a")
    assert claim_a is not None

    # Renew lease
    renewed = await mgr.renew_shard_lease(shard_id=1, claim=claim_a, ttl_seconds=10.0)
    assert renewed is True

    # Check TTL in Redis is extended
    ttl_ms = await fake_redis.pttl("quantive:lock:ws_shard:1")
    assert ttl_ms > 5000 # Extended past initial 5s


@pytest.mark.asyncio
async def test_7_worker_b_cannot_renew_worker_a_lease(fake_redis):
    """7. Worker B cannot renew Worker A's lease."""
    mgr = ShardLeaseManager(redis_client=fake_redis, lease_ttl_seconds=10.0)
    claim_a = await mgr.acquire_shard_lease(shard_id=2, worker_id="worker_a")
    assert claim_a is not None

    # Construct a forged / separate claim from Worker B
    forged_claim_b = ShardLeaseClaim(
        shard_id=2,
        worker_id="worker_b",
        claim_token="forged_token",
        claimed_at=datetime.now(timezone.utc),
        lease_expires_at=datetime.now(timezone.utc) + timedelta(seconds=10),
    )

    renewed = await mgr.renew_shard_lease(shard_id=2, claim=forged_claim_b)
    assert renewed is False

    # Owner remains Worker A
    owner = await mgr.get_current_owner(shard_id=2)
    assert owner.worker_id == "worker_a"
    assert owner.claim_token == claim_a.claim_token


@pytest.mark.asyncio
async def test_8_and_9_worker_a_loses_ownership_after_ttl_and_worker_b_acquires(fake_redis):
    """8. Worker A loses ownership after TTL expiration & 9. Worker B acquires expired shard."""
    # Set short TTL
    mgr = ShardLeaseManager(redis_client=fake_redis, lease_ttl_seconds=0.1)
    claim_a = await mgr.acquire_shard_lease(shard_id=3, worker_id="worker_a")
    assert claim_a is not None

    # Wait for TTL expiration
    await asyncio.sleep(0.15)

    # Worker A attempts renewal on expired key -> returns False (lost ownership)
    renewed_a = await mgr.renew_shard_lease(shard_id=3, claim=claim_a)
    assert renewed_a is False

    # 9. Worker B can now acquire the expired shard
    claim_b = await mgr.acquire_shard_lease(shard_id=3, worker_id="worker_b")
    assert claim_b is not None
    assert claim_b.worker_id == "worker_b"


@pytest.mark.asyncio
async def test_10_and_11_old_worker_a_cannot_delete_or_renew_worker_b_lease(fake_redis):
    """10. Old Worker A cannot delete Worker B's lease & 11. Old Worker A cannot renew Worker B's lease."""
    mgr = ShardLeaseManager(redis_client=fake_redis, lease_ttl_seconds=0.1)
    claim_a = await mgr.acquire_shard_lease(shard_id=4, worker_id="worker_a")
    assert claim_a is not None

    # Shard expires
    await asyncio.sleep(0.15)

    # Worker B acquires shard
    claim_b = await mgr.acquire_shard_lease(shard_id=4, worker_id="worker_b", ttl_seconds=10.0)
    assert claim_b is not None
    assert claim_b.worker_id == "worker_b"

    # 10. Stale Worker A attempts to release (DEL) Worker B's lease -> must fail
    released_a = await mgr.release_shard_lease(shard_id=4, claim=claim_a)
    assert released_a is False

    # Worker B's lease is still intact
    owner = await mgr.get_current_owner(shard_id=4)
    assert owner is not None
    assert owner.worker_id == "worker_b"
    assert owner.claim_token == claim_b.claim_token

    # 11. Stale Worker A attempts to renew Worker B's lease -> must fail
    renewed_a = await mgr.renew_shard_lease(shard_id=4, claim=claim_a)
    assert renewed_a is False

    # Worker B remains owner
    owner = await mgr.get_current_owner(shard_id=4)
    assert owner.worker_id == "worker_b"


# ============================================================================
# 12 - 15. Runtime, Supervisor, Fencing, and Lifecycle Tests
# ============================================================================

@pytest.mark.asyncio
async def test_12_redis_unavailable_causes_shard_shutdown_fail_closed(fake_redis):
    """12. Redis unavailable causes shard shutdown/fail-closed."""
    import redis.exceptions
    mgr = ShardLeaseManager(redis_client=fake_redis, lease_ttl_seconds=2.0)
    supervisor = ShardSupervisor(
        worker_id="test_worker_fail_closed",
        candidate_shards=[0],
        lease_manager=mgr,
        heartbeat_interval_seconds=0.05,
        lease_ttl_seconds=0.5
    )
    await supervisor.start()
    assert 0 in supervisor.owned_shard_ids

    runtime = supervisor.get_shard_runtime(0)
    assert runtime.is_running is True
    runtime.add_uncommitted_buffer({"candle": "BTC_1m"})
    assert runtime.buffer_count == 1

    # Simulate Redis connection failure during heartbeat renewal
    original_script = mgr._renew_script

    async def mock_failing_script(*args, **kwargs):
        raise redis.exceptions.ConnectionError("Simulated Redis connection drop")

    mgr._renew_script = mock_failing_script

    # Wait for heartbeat cycle to encounter Redis communication failure
    await asyncio.sleep(0.15)

    # Runtime must be hard-fenced and failed-closed
    assert runtime.is_fenced is True
    assert runtime.is_running is False
    assert runtime.buffer_count == 0  # Discarded uncommitted buffers
    assert runtime.is_accepting_work is False

    # Restore script for clean teardown
    mgr._renew_script = original_script
    await supervisor.shutdown()


@pytest.mark.asyncio
async def test_13_clean_shutdown_releases_only_owner_lease(fake_redis):
    """13. Clean shutdown releases only the owner's lease."""
    mgr = ShardLeaseManager(redis_client=fake_redis, lease_ttl_seconds=10.0)
    supervisor_a = ShardSupervisor(
        worker_id="supervisor_a",
        candidate_shards=[0, 1],
        lease_manager=mgr,
        heartbeat_interval_seconds=1.0
    )
    await supervisor_a.start()
    assert sorted(supervisor_a.owned_shard_ids) == [0, 1]

    # Clean shutdown
    await supervisor_a.shutdown()
    assert len(supervisor_a.owned_shard_ids) == 0

    # Shards 0 and 1 are now immediately released in Redis
    assert await mgr.get_current_owner(0) is None
    assert await mgr.get_current_owner(1) is None

    # Another supervisor can immediately acquire them
    supervisor_b = ShardSupervisor(
        worker_id="supervisor_b",
        candidate_shards=[0, 1],
        lease_manager=mgr,
        heartbeat_interval_seconds=1.0
    )
    await supervisor_b.start()
    assert sorted(supervisor_b.owned_shard_ids) == [0, 1]
    await supervisor_b.shutdown()


@pytest.mark.asyncio
async def test_14_heartbeat_task_cleanup(fake_redis):
    """14. Heartbeat task cleanup when supervisor is stopped or shard fenced."""
    mgr = ShardLeaseManager(redis_client=fake_redis, lease_ttl_seconds=5.0)
    supervisor = ShardSupervisor(
        worker_id="test_worker_task_cleanup",
        candidate_shards=[0],
        lease_manager=mgr,
        heartbeat_interval_seconds=0.05
    )
    await supervisor.start()
    assert len(supervisor._heartbeat_tasks) == 1
    task = supervisor._heartbeat_tasks[0]
    assert not task.done()

    # Graceful shutdown cancels and cleans up heartbeat tasks
    await supervisor.shutdown()
    assert task.done() or task.cancelled()
    assert len(supervisor._heartbeat_tasks) == 0


@pytest.mark.asyncio
async def test_15_supervisor_stops_runtime_after_ownership_loss(fake_redis):
    """15. Supervisor stops/fences runtime after ownership loss."""
    mgr = ShardLeaseManager(redis_client=fake_redis, lease_ttl_seconds=0.1)
    supervisor = ShardSupervisor(
        worker_id="test_worker_ownership_loss",
        candidate_shards=[5],
        lease_manager=mgr,
        heartbeat_interval_seconds=0.05,
        lease_ttl_seconds=0.1
    )
    await supervisor.start()
    assert 5 in supervisor.owned_shard_ids
    runtime = supervisor.get_shard_runtime(5)
    assert runtime.is_running is True

    # Simulate another external entity wiping/overwriting the key in Redis
    await fake_redis.delete("quantive:lock:ws_shard:5")

    # Wait for heartbeat cycle
    await asyncio.sleep(0.12)

    # Runtime must be fenced upon ownership loss
    assert runtime.is_fenced is True
    assert runtime.state == ShardRuntimeState.FENCED
    assert 5 not in supervisor.owned_shard_ids

    await supervisor.shutdown()
