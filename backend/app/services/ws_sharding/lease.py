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
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

import redis.asyncio as redis_async
from app.core.config import settings
from app.workers.config import get_async_redis

logger = logging.getLogger(__name__)

# Atomic acquire gives every successful ownership generation a monotonically
# increasing fencing token.  Redis TTLs decide *liveness*, but they cannot
# stop a paused owner that resumes after another worker has acquired the same
# shard.  The token is registered with PostgreSQL before a runtime starts and
# is checked in the same transaction as candle persistence.
ACQUIRE_LEASE_LUA = """
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
end
local fencing_token = redis.call("INCR", KEYS[2])
local claim = cjson.decode(ARGV[1])
claim["fencing_token"] = fencing_token
redis.call("SET", KEYS[1], cjson.encode(claim), "PX", ARGV[2])
return fencing_token
"""

# Atomic heartbeat renewal verifies the immutable acquisition identity instead
# of comparing a raw JSON payload (Lua JSON key ordering is not stable).
RENEW_LEASE_LUA = """
local raw = redis.call("GET", KEYS[1])
if raw == false then return 0 end
local claim = cjson.decode(raw)
if claim["claim_token"] == ARGV[1]
   and tonumber(claim["fencing_token"]) == tonumber(ARGV[2]) then
    return redis.call("PEXPIRE", KEYS[1], ARGV[3])
end
return 0
"""

# Atomic release uses the same ownership generation predicate.  A stale owner
# must never delete a later owner after lease expiry/reacquisition.
RELEASE_LEASE_LUA = """
local raw = redis.call("GET", KEYS[1])
if raw == false then return 0 end
local claim = cjson.decode(raw)
if claim["claim_token"] == ARGV[1]
   and tonumber(claim["fencing_token"]) == tonumber(ARGV[2]) then
    return redis.call("DEL", KEYS[1])
end
return 0
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
    # A Redis-monotonic ownership generation.  ``0`` preserves backward
    # compatibility for hand-built test claims, but those claims cannot enter
    # a persistence-fenced production runtime.
    fencing_token: int = 0

    def to_json(self) -> str:
        """Serializes the lease claim to a deterministic JSON string."""
        data = {
            "shard_id": self.shard_id,
            "worker_id": self.worker_id,
            "claim_token": self.claim_token,
            "claimed_at": self.claimed_at.isoformat(),
            "lease_expires_at": self.lease_expires_at.isoformat(),
            "fencing_token": self.fencing_token,
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
            fencing_token=int(data.get("fencing_token", 0)),
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
        self._acquire_script = self.redis.register_script(ACQUIRE_LEASE_LUA)
        self._renew_script = self.redis.register_script(RENEW_LEASE_LUA)
        self._release_script = self.redis.register_script(RELEASE_LEASE_LUA)

    def _get_key(self, shard_id: int) -> str:
        return f"{self.key_prefix}:{shard_id}"

    def _get_fencing_key(self, shard_id: int) -> str:
        """Return the durable Redis counter key for a shard generation."""
        return f"{self.key_prefix}:fence:{shard_id}"

    async def acquire_shard_lease(
        self,
        shard_id: int,
        worker_id: str,
        ttl_seconds: Optional[float] = None
    ) -> Optional[ShardLeaseClaim]:
        """
        Attempts atomic acquisition of a shard lease and fencing generation.

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
        
        # Lua adds the fencing token after atomically confirming the lease key
        # is free.  Do not increment it speculatively for unsuccessful claims:
        # a generation represents an actual owner, not contention.
        provisional_claim = ShardLeaseClaim(
            shard_id=shard_id,
            worker_id=worker_id,
            claim_token=claim_token,
            claimed_at=now,
            lease_expires_at=expires_at,
        )

        try:
            fencing_token = await self._acquire_script(
                keys=[key, self._get_fencing_key(shard_id)],
                args=[provisional_claim.to_json(), ttl_ms],
            )
            if fencing_token:
                claim = ShardLeaseClaim(
                    shard_id=shard_id,
                    worker_id=worker_id,
                    claim_token=claim_token,
                    claimed_at=now,
                    lease_expires_at=expires_at,
                    fencing_token=int(fencing_token),
                )
                logger.info(
                    f"Successfully acquired lease for shard {shard_id} (claim: {claim_token}, fence: {claim.fencing_token}) by worker {worker_id}",
                    extra={
                        "shard_id": shard_id,
                        "worker_id": worker_id,
                        "claim_token": claim_token,
                        "fencing_token": claim.fencing_token,
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
        try:
            result = await self._renew_script(
                keys=[key],
                args=[claim.claim_token, claim.fencing_token, ttl_ms]
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
        try:
            result = await self._release_script(
                keys=[key],
                args=[claim.claim_token, claim.fencing_token]
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
