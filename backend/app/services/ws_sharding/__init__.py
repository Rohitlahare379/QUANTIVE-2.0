"""
WebSocket Shard Management & Distributed Lease Package (P0.2 Phase 1).
"""

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
from app.services.ws_sharding.metrics import (
    PipelineMetrics,
)
from app.services.ws_sharding.pipeline import (
    BoundedLiveIngestionPipeline,
    validate_candle_payload,
)
from app.services.ws_sharding.registry import (
    AssetRegistryResolver,
)
from app.services.ws_sharding.runtime import (
    ShardRuntime,
    ShardRuntimeState,
)
from app.services.ws_sharding.supervisor import (
    ShardSupervisor,
)

__all__ = [
    "assign_symbols_to_shards",
    "get_shard_for_symbol",
    "get_symbols_for_shard",
    "normalize_symbol",
    "generate_worker_id",
    "RedisUnavailableError",
    "ShardLeaseClaim",
    "ShardLeaseManager",
    "ShardRuntime",
    "ShardRuntimeState",
    "ShardSupervisor",
    "BoundedLiveIngestionPipeline",
    "PipelineMetrics",
    "AssetRegistryResolver",
    "validate_candle_payload",
]
