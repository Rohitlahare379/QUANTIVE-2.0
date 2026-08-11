"""
Distributed Shard Lease Management with Atomic Redis Operations.

Coordinates distributed shard ownership with atomic acquisition, safe Lua heartbeat renewal,
and atomic release to prevent split-brain, zombie worker overwrite, and stale release bugs.
"""

import json
import logging
import os
import socket
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

import redis.asyncio as redis_async
from app.core.config import settings
from app.workers.config import get_async_redis

logger = logging.getLogger(__name__)

# Atomic Heartbeat Renewal Lua Script
# Verifies the stored payload matches ARGV[1] before extending TTL via PEXPIRE.
# Returns 1 if renewed, 0 if ownership lost / key missing / owned by another claim.
RENEW_LEASE_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
"""

# Atomic Safe Release Lua Script
# Only deletes the key if the current stored payload exactly matches ARGV[1].
# Prevents stale workers from deleting a newly acquired lease of another worker.
RELEASE_LEASE_LUA = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""


class RedisUnavailableError(Exception):
    """Raised when Redis operations fail due to connectivity, timeout, or broker errors."""
    pass


def generate_worker_id() -> str:
    """
    Generates a unique process-level worker identifier.
    Format: hostname:pid:instance_nonce
    """
    hostname = socket.gethostname()
    pid = os.getpid()
    nonce = uuid.uuid4().hex[:8]
    return f"{hostname}:{pid}:{nonce}"


@dataclass(frozen=True)
class ShardLeaseClaim:
    """
    Immutable representation of an active shard lease claim.
    """
    shard_id: int
    worker_id: str
    claim_token: str
    claimed_at: datetime
    lease_expires_at: datetime

    def to_json(self) -> str:
        """Serializes the lease claim to a deterministic JSON string."""
        data = {
            "shard_id": self.shard_id,
            "worker_id": self.worker_id,
            "claim_token": self.claim_token,
            "claimed_at": self.claimed_at.isoformat(),
            "lease_expires_at": self.lease_expires_at.isoformat(),
        }
        return json.dumps(data, sort_keys=True)

    @classmethod
    def from_json(cls, json_str: str) -> "ShardLeaseClaim":
        """Deserializes a JSON string into a ShardLeaseClaim."""
        data = json.loads(json_str)
        return cls(
            shard_id=int(data["shard_id"]),
            worker_id=str(data["worker_id"]),
            claim_token=str(data["claim_token"]),
            claimed_at=datetime.fromisoformat(data["claimed_at"]),
            lease_expires_at=datetime.fromisoformat(data["lease_expires_at"]),
        )


class ShardLeaseManager:
    """
    Manages atomic acquisition, heartbeat extension, and release of WebSocket shard leases in Redis.
    """

    def __init__(
        self,
        redis_client: Optional[redis_async.Redis] = None,
        key_prefix: str = "quantive:lock:ws_shard",
        lease_ttl_seconds: Optional[float] = None
    ):
        self.redis = redis_client if redis_client is not None else get_async_redis()
        self.key_prefix = key_prefix
        self.lease_ttl_seconds = lease_ttl_seconds or settings.WS_LEASE_TTL_SECONDS
        
        # Pre-register Lua scripts for atomic operations
        self._renew_script = self.redis.register_script(RENEW_LEASE_LUA)
        self._release_script = self.redis.register_script(RELEASE_LEASE_LUA)

    def _get_key(self, shard_id: int) -> str:
        return f"{self.key_prefix}:{shard_id}"

    async def acquire_shard_lease(
        self,
        shard_id: int,
        worker_id: str,
        ttl_seconds: Optional[float] = None
    ) -> Optional[ShardLeaseClaim]:
        """
        Attempts atomic acquisition of a shard lease using Redis SET NX EX.

        Returns:
            ShardLeaseClaim if acquired, None if the shard is currently owned by another worker.

        Raises:
            RedisUnavailableError: If Redis communication fails (fail-closed).
        """
        ttl = ttl_seconds or self.lease_ttl_seconds
        ttl_ms = int(ttl * 1000)
        key = self._get_key(shard_id)
        
        now = datetime.now(timezone.utc)
        claim_token = uuid.uuid4().hex
        expires_at = now + timedelta(seconds=ttl)
        
        claim = ShardLeaseClaim(
            shard_id=shard_id,
            worker_id=worker_id,
            claim_token=claim_token,
            claimed_at=now,
            lease_expires_at=expires_at,
        )
        payload = claim.to_json()

        try:
            # SET key payload PX ttl_ms NX -> atomic acquire with millisecond precision
            acquired = await self.redis.set(key, payload, px=ttl_ms, nx=True)
            if acquired:
                logger.info(
                    f"Successfully acquired lease for shard {shard_id} (claim: {claim_token}) by worker {worker_id}",
                    extra={
                        "shard_id": shard_id,
                        "worker_id": worker_id,
                        "claim_token": claim_token,
                        "event": "shard_acquired"
                    }
                )
                return claim
            else:
                logger.debug(
                    f"Failed to acquire lease for shard {shard_id}: shard is already owned",
                    extra={
                        "shard_id": shard_id,
                        "worker_id": worker_id,
                        "event": "acquisition_failed"
                    }
                )
                return None
        except Exception as e:
            logger.error(
                f"Redis error while acquiring lease for shard {shard_id}: {e}",
                extra={"shard_id": shard_id, "worker_id": worker_id, "event": "redis_error"}
            )
            raise RedisUnavailableError(f"Failed to communicate with Redis during shard acquire: {e}") from e

    async def renew_shard_lease(
        self,
        shard_id: int,
        claim: ShardLeaseClaim,
        ttl_seconds: Optional[float] = None
    ) -> bool:
        """
        Atomically verifies ownership and extends the TTL of an active shard lease via Lua script.

        Returns:
            bool: True if renewed, False if ownership was lost (key expired or owned by another worker).

        Raises:
            RedisUnavailableError: If Redis communication fails (fail-closed).
        """
        ttl = ttl_seconds or self.lease_ttl_seconds
        ttl_ms = int(ttl * 1000)
        key = self._get_key(shard_id)
        payload = claim.to_json()

        try:
            result = await self._renew_script(
                keys=[key],
                args=[payload, ttl_ms]
            )
            renewed = bool(result == 1)
            if renewed:
                logger.debug(
                    f"Renewed lease for shard {shard_id} (claim: {claim.claim_token}) by worker {claim.worker_id}",
                    extra={
                        "shard_id": shard_id,
                        "worker_id": claim.worker_id,
                        "claim_token": claim.claim_token,
                        "event": "heartbeat_renewed"
                    }
                )
            else:
                logger.warning(
                    f"Ownership lost during heartbeat renewal for shard {shard_id} (claim: {claim.claim_token})",
                    extra={
                        "shard_id": shard_id,
                        "worker_id": claim.worker_id,
                        "claim_token": claim.claim_token,
                        "event": "ownership_lost"
                    }
                )
            return renewed
        except Exception as e:
            logger.error(
                f"Redis error during heartbeat renewal for shard {shard_id}: {e}",
                extra={
                    "shard_id": shard_id,
                    "worker_id": claim.worker_id,
                    "claim_token": claim.claim_token,
                    "event": "redis_error"
                }
            )
            raise RedisUnavailableError(f"Failed to communicate with Redis during lease renewal: {e}") from e

    async def release_shard_lease(
        self,
        shard_id: int,
        claim: ShardLeaseClaim
    ) -> bool:
        """
        Safely and atomically releases a shard lease if and only if the current owner matches `claim`.

        Returns:
            bool: True if the key was deleted, False if already deleted/expired or claimed by another worker.
        """
        key = self._get_key(shard_id)
        payload = claim.to_json()

        try:
            result = await self._release_script(
                keys=[key],
                args=[payload]
            )
            released = bool(result == 1)
            if released:
                logger.info(
                    f"Cleanly released lease for shard {shard_id} (claim: {claim.claim_token})",
                    extra={
                        "shard_id": shard_id,
                        "worker_id": claim.worker_id,
                        "claim_token": claim.claim_token,
                        "event": "lease_released"
                    }
                )
            else:
                logger.warning(
                    f"Attempted to release lease for shard {shard_id}, but ownership token did not match or key was already gone",
                    extra={
                        "shard_id": shard_id,
                        "worker_id": claim.worker_id,
                        "claim_token": claim.claim_token,
                        "event": "release_skipped"
                    }
                )
            return released
        except Exception as e:
            logger.error(
                f"Redis error during lease release for shard {shard_id}: {e}",
                extra={
                    "shard_id": shard_id,
                    "worker_id": claim.worker_id,
                    "claim_token": claim.claim_token,
                    "event": "redis_error"
                }
            )
            # Release is best-effort during shutdown; log and return False
            return False

    async def get_current_owner(self, shard_id: int) -> Optional[ShardLeaseClaim]:
        """
        Reads and deserializes the current active lease claim for a shard, if any.
        """
        key = self._get_key(shard_id)
        try:
            raw_val = await self.redis.get(key)
            if raw_val is None:
                return None
            if isinstance(raw_val, bytes):
                raw_val = raw_val.decode("utf-8")
            return ShardLeaseClaim.from_json(raw_val)
        except Exception as e:
            logger.error(f"Redis error reading current owner for shard {shard_id}: {e}")
            raise RedisUnavailableError(f"Failed to read current owner: {e}") from e
