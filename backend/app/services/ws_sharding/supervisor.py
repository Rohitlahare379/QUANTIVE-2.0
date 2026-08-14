"""
WebSocket Shard Supervisor.

Coordinates candidate shard discovery, distributed atomic lease acquisition,
heartbeat liveness monitoring, hard fencing on ownership loss or Redis failure,
and clean lease release during graceful shutdown.
"""

import asyncio
import logging
from typing import Callable, Dict, List, Optional, Set
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.services.ws_sharding.assignment import get_symbols_for_shard
from app.services.ws_sharding.lease import (
    RedisUnavailableError,
    ShardLeaseClaim,
    ShardLeaseManager,
    generate_worker_id,
)
from app.services.ws_sharding.registry import AssetRegistryResolver
from app.services.ws_sharding.runtime import ShardRuntime, ShardRuntimeState

logger = logging.getLogger(__name__)


class ShardSupervisor:
    """
    Supervises a set of WebSocket shards for a worker process.
    """

    def __init__(
        self,
        worker_id: Optional[str] = None,
        num_shards: Optional[int] = None,
        candidate_shards: Optional[List[int]] = None,
        symbols: Optional[List[str]] = None,
        lease_manager: Optional[ShardLeaseManager] = None,
        heartbeat_interval_seconds: Optional[float] = None,
        lease_ttl_seconds: Optional[float] = None,
        session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
        asset_resolver: Optional[AssetRegistryResolver] = None,
    ):
        self.worker_id = worker_id or generate_worker_id()
        self.num_shards = num_shards or settings.WS_NUM_SHARDS
        self.candidate_shards = (
            sorted(candidate_shards) if candidate_shards is not None else list(range(self.num_shards))
        )
        self.symbols = symbols or []
        self.lease_ttl_seconds = lease_ttl_seconds or settings.WS_LEASE_TTL_SECONDS
        self.heartbeat_interval_seconds = (
            heartbeat_interval_seconds or settings.WS_HEARTBEAT_INTERVAL_SECONDS
        )
        self.session_factory = session_factory
        self.asset_resolver = asset_resolver

        self.lease_manager = lease_manager or ShardLeaseManager(
            lease_ttl_seconds=self.lease_ttl_seconds
        )

        # Active state tracked per shard_id
        self._active_shards: Dict[int, ShardRuntime] = {}
        self._active_claims: Dict[int, ShardLeaseClaim] = {}
        self._heartbeat_tasks: Dict[int, asyncio.Task] = {}
        self._is_running = False
        self._lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def owned_shard_ids(self) -> List[int]:
        """Returns the list of currently active, non-fenced owned shard IDs."""
        return [
            shard_id
            for shard_id, runtime in self._active_shards.items()
            if runtime.is_running and not runtime.is_fenced
        ]

    def get_shard_runtime(self, shard_id: int) -> Optional[ShardRuntime]:
        """Returns the active ShardRuntime for a given shard ID, if owned."""
        return self._active_shards.get(shard_id)

    async def attempt_acquire_shard(self, shard_id: int) -> bool:
        """
        Attempts to acquire lease ownership for a specific shard and start its runtime.
        """
        async with self._lock:
            # Check if already actively owned
            existing_runtime = self._active_shards.get(shard_id)
            if existing_runtime and existing_runtime.is_running:
                return True

            # Attempt atomic Redis acquisition
            try:
                claim = await self.lease_manager.acquire_shard_lease(
                    shard_id=shard_id,
                    worker_id=self.worker_id,
                    ttl_seconds=self.lease_ttl_seconds
                )
            except RedisUnavailableError as e:
                logger.error(
                    f"Redis unavailable while supervisor {self.worker_id} attempted to acquire shard {shard_id}: {e}. Skipping.",
                    extra={"shard_id": shard_id, "worker_id": self.worker_id, "event": "redis_error"}
                )
                return False

            if not claim:
                return False # Owned by another worker

            # Filter symbols for this shard
            shard_symbols = get_symbols_for_shard(self.symbols, shard_id, self.num_shards)

            # Instantiate and start ShardRuntime
            runtime = ShardRuntime(
                shard_id=shard_id,
                symbols=shard_symbols,
                claim=claim,
                session_factory=self.session_factory,
                asset_resolver=self.asset_resolver,
            )
            await runtime.start()

            # Store active mappings
            self._active_shards[shard_id] = runtime
            self._active_claims[shard_id] = claim

            # Spawn dedicated heartbeat task
            heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(shard_id, claim, runtime),
                name=f"ws-shard-heartbeat-{shard_id}"
            )
            self._heartbeat_tasks[shard_id] = heartbeat_task

            return True

    async def attempt_acquire_all_candidates(self) -> Dict[int, bool]:
        """
        Iterates over all candidate shards and attempts acquisition for unowned shards.
        Returns a mapping of shard_id -> acquisition_success.
        """
        results: Dict[int, bool] = {}
        for shard_id in self.candidate_shards:
            if shard_id in self._active_shards and self._active_shards[shard_id].is_running:
                results[shard_id] = True
            else:
                results[shard_id] = await self.attempt_acquire_shard(shard_id)
        return results

    async def _heartbeat_loop(
        self,
        shard_id: int,
        claim: ShardLeaseClaim,
        runtime: ShardRuntime
    ) -> None:
        """
        Periodically extends lease TTL in Redis.
        If renewal returns 0 or Redis is unreachable, triggers hard fencing immediately.
        """
        logger.debug(f"Starting heartbeat loop for shard {shard_id} (claim {claim.claim_token})")
        
        while self._is_running and runtime.is_running:
            try:
                await asyncio.sleep(self.heartbeat_interval_seconds)
            except asyncio.CancelledError:
                break

            if not self._is_running or not runtime.is_running:
                break

            try:
                renewed = await self.lease_manager.renew_shard_lease(
                    shard_id=shard_id,
                    claim=claim,
                    ttl_seconds=self.lease_ttl_seconds
                )
                if not renewed:
                    logger.warning(
                        f"Ownership lost for shard {shard_id} (claim {claim.claim_token}) during heartbeat renewal. Fencing runtime.",
                        extra={"shard_id": shard_id, "worker_id": self.worker_id, "event": "ownership_lost"}
                    )
                    runtime.fence(reason="Heartbeat renewal returned 0 (lease expired or reclaimed)")
                    self._cleanup_fenced_shard(shard_id)
                    break
            except RedisUnavailableError as e:
                logger.error(
                    f"Redis unavailable during heartbeat renewal for shard {shard_id}: {e}. Fencing runtime (fail-closed).",
                    extra={"shard_id": shard_id, "worker_id": self.worker_id, "event": "redis_error"}
                )
                runtime.fence(reason=f"Redis unavailable during heartbeat: {e}")
                self._cleanup_fenced_shard(shard_id)
                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    f"Unexpected error in heartbeat for shard {shard_id}: {e}. Fencing runtime.",
                    extra={"shard_id": shard_id, "worker_id": self.worker_id, "event": "heartbeat_error"}
                )
                runtime.fence(reason=f"Unexpected heartbeat exception: {e}")
                self._cleanup_fenced_shard(shard_id)
                break

    def _cleanup_fenced_shard(self, shard_id: int) -> None:
        """Removes an active claim mapping for a fenced shard."""
        self._active_claims.pop(shard_id, None)

    async def start(self) -> None:
        """
        Starts the supervisor and attempts initial candidate shard acquisition.
        """
        self._is_running = True
        logger.info(
            f"Starting ShardSupervisor {self.worker_id} supervising candidates {self.candidate_shards} out of {self.num_shards} total shards",
            extra={"worker_id": self.worker_id, "candidates": self.candidate_shards, "event": "supervisor_started"}
        )
        await self.attempt_acquire_all_candidates()

    async def shutdown(self) -> None:
        """
        Gracefully shuts down the supervisor:
        1. Stops the supervisor loop.
        2. Cancels and awaits all background heartbeat tasks.
        3. Stops all active ShardRuntimes.
        4. Safely releases all held Redis leases using atomic release tokens.
        """
        if not self._is_running and not self._active_shards:
            return

        self._is_running = False
        logger.info(
            f"Shutting down ShardSupervisor {self.worker_id} holding shards {list(self._active_shards.keys())}",
            extra={"worker_id": self.worker_id, "event": "supervisor_shutdown_start"}
        )

        async with self._lock:
            # 1. Cancel and await all heartbeat tasks
            tasks_to_cancel = list(self._heartbeat_tasks.values())
            for task in tasks_to_cancel:
                task.cancel()
            
            if tasks_to_cancel:
                await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            self._heartbeat_tasks.clear()

            # 2. Stop all active runtimes
            for shard_id, runtime in list(self._active_shards.items()):
                await runtime.stop()

            # 3. Safely release all active leases
            for shard_id, claim in list(self._active_claims.items()):
                await self.lease_manager.release_shard_lease(shard_id, claim)

            self._active_shards.clear()
            self._active_claims.clear()

        logger.info(
            f"Successfully shut down ShardSupervisor {self.worker_id}",
            extra={"worker_id": self.worker_id, "event": "supervisor_shutdown_complete"}
        )
