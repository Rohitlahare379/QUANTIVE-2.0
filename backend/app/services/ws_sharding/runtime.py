"""
WebSocket Shard Runtime Lifecycle Component.

Provides the runtime abstraction for an individual owned WebSocket shard.
Coordinates WebSocket client streaming, bounded live ingestion pipeline,
and strict fencing semantics on ownership loss or shutdown.
"""

import asyncio
import enum
import logging
from typing import Any, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.connectors.models import CandleEvent
from app.services.ws_sharding.lease import ShardLeaseClaim
from app.services.ws_sharding.pipeline import BoundedLiveIngestionPipeline
from app.services.ws_sharding.registry import AssetRegistryResolver

logger = logging.getLogger(__name__)


class ShardRuntimeState(str, enum.Enum):
    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    FENCED = "FENCED"
    STOPPED = "STOPPED"


class ShardRuntime:
    """
    Manages the lifecycle of an individual owned shard.
    Enforces strict fencing semantics: once fenced, all buffers are discarded and
    no further work or persistence is permitted within this ownership context.
    """

    def __init__(
        self,
        shard_id: int,
        symbols: List[str],
        claim: ShardLeaseClaim,
        session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
        asset_resolver: Optional[AssetRegistryResolver] = None,
    ):
        self.shard_id = shard_id
        self.symbols = sorted(list(set(symbols)))
        self.claim = claim
        self.session_factory = session_factory
        self.asset_resolver = asset_resolver
        self._state = ShardRuntimeState.INITIALIZING
        self._is_accepting_work = False
        self._uncommitted_buffers: List[Any] = []
        self._stop_event = asyncio.Event()

        # Phase 3 Bounded Ingestion Pipeline
        self.pipeline: Optional[BoundedLiveIngestionPipeline] = None

    @property
    def state(self) -> ShardRuntimeState:
        return self._state

    @property
    def is_running(self) -> bool:
        return self._state == ShardRuntimeState.RUNNING

    @property
    def is_fenced(self) -> bool:
        return self._state == ShardRuntimeState.FENCED

    @property
    def is_stopped(self) -> bool:
        return self._state in (ShardRuntimeState.STOPPED, ShardRuntimeState.FENCED)

    @property
    def is_accepting_work(self) -> bool:
        return self._is_accepting_work and self.is_running

    @property
    def buffer_count(self) -> int:
        pipeline_count = self.pipeline.queue_size if self.pipeline else 0
        return len(self._uncommitted_buffers) + pipeline_count

    def add_uncommitted_buffer(self, item: Any) -> bool:
        """
        Adds a candle / buffer item to the uncommitted in-memory queue.
        Returns False if the shard is fenced or not accepting work.
        """
        if not self.is_accepting_work:
            return False
        self._uncommitted_buffers.append(item)
        return True

    async def enqueue_candle(self, event: CandleEvent) -> bool:
        """
        Enqueues a finalized CandleEvent to the live ingestion pipeline.
        Returns False if shard is fenced, not accepting work, or event rejected.
        """
        if not self.is_accepting_work or not self.pipeline:
            return False
        return await self.pipeline.enqueue_candle(event)

    def clear_uncommitted_buffers(self) -> int:
        """
        Discards all uncommitted in-memory buffers during fencing or teardown.
        Returns the total number of discarded items.
        """
        count = len(self._uncommitted_buffers)
        self._uncommitted_buffers.clear()
        if self.pipeline:
            count += self.pipeline.discard_uncommitted_buffers()
        return count

    async def start(self) -> None:
        """
        Starts the shard runtime upon verified lease acquisition.
        """
        if self._state == ShardRuntimeState.FENCED:
            raise RuntimeError(f"Cannot start a fenced shard runtime for shard {self.shard_id}")

        self._state = ShardRuntimeState.RUNNING
        self._is_accepting_work = True
        self._stop_event.clear()

        # Initialize and start live ingestion pipeline
        self.pipeline = BoundedLiveIngestionPipeline(
            shard_id=self.shard_id,
            session_factory=self.session_factory,
            asset_resolver=self.asset_resolver,
            fencing_check=lambda: self.is_running and not self.is_fenced,
        )
        await self.pipeline.start()
        
        logger.info(
            f"Started ShardRuntime for shard {self.shard_id} ({len(self.symbols)} symbols) with claim {self.claim.claim_token}",
            extra={
                "shard_id": self.shard_id,
                "worker_id": self.claim.worker_id,
                "claim_token": self.claim.claim_token,
                "symbols_count": len(self.symbols),
                "event": "shard_runtime_started"
            }
        )

    def fence(self, reason: str) -> None:
        """
        Fences the shard runtime immediately due to ownership loss or Redis failure.
        Enforces hard fencing:
        1. Stops accepting new WebSocket work for the shard.
        2. Discards uncommitted in-memory shard buffers and pipeline queues.
        3. Stops shard processing and flush loops.
        4. Prohibits database persistence and reacquisition from within this instance.
        """
        if self._state == ShardRuntimeState.FENCED:
            return # Already fenced

        discarded = self.clear_uncommitted_buffers()
        self._state = ShardRuntimeState.FENCED
        self._is_accepting_work = False
        self._stop_event.set()

        logger.warning(
            f"FENCED ShardRuntime for shard {self.shard_id} (claim {self.claim.claim_token}). Reason: {reason}. Discarded {discarded} uncommitted items.",
            extra={
                "shard_id": self.shard_id,
                "worker_id": self.claim.worker_id,
                "claim_token": self.claim.claim_token,
                "discarded_buffers": discarded,
                "reason": reason,
                "event": "shard_fenced"
            }
        )

    async def stop(self) -> None:
        """
        Performs clean graceful shutdown of the shard runtime.
        """
        if self._state in (ShardRuntimeState.STOPPED, ShardRuntimeState.FENCED):
            return

        self._state = ShardRuntimeState.STOPPED
        self._is_accepting_work = False
        self._stop_event.set()

        if self.pipeline:
            await self.pipeline.stop()

        discarded = len(self._uncommitted_buffers)
        self._uncommitted_buffers.clear()

        logger.info(
            f"Stopped ShardRuntime for shard {self.shard_id} (claim {self.claim.claim_token}). Cleaned {discarded} buffers.",
            extra={
                "shard_id": self.shard_id,
                "worker_id": self.claim.worker_id,
                "claim_token": self.claim.claim_token,
                "event": "shard_shutdown"
            }
        )
