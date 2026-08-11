"""
WebSocket Shard Runtime Lifecycle Component.

Provides the runtime abstraction for an individual owned WebSocket shard.
In Phase 1, this acts as the testable lifecycle and fencing component, ready
to receive the Binance WebSocket client and bounded ingestion buffers in Phase 2.
"""

import asyncio
import enum
import logging
from typing import Any, List, Optional
from app.services.ws_sharding.lease import ShardLeaseClaim

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
    no further work or reconnection is permitted within this ownership context.
    """

    def __init__(
        self,
        shard_id: int,
        symbols: List[str],
        claim: ShardLeaseClaim
    ):
        self.shard_id = shard_id
        self.symbols = sorted(list(set(symbols)))
        self.claim = claim
        self._state = ShardRuntimeState.INITIALIZING
        self._is_accepting_work = False
        self._uncommitted_buffers: List[Any] = []
        self._stop_event = asyncio.Event()

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
        return len(self._uncommitted_buffers)

    def add_uncommitted_buffer(self, item: Any) -> bool:
        """
        Adds a candle / buffer item to the uncommitted in-memory queue.
        Returns False if the shard is fenced or not accepting work.
        """
        if not self.is_accepting_work:
            return False
        self._uncommitted_buffers.append(item)
        return True

    def clear_uncommitted_buffers(self) -> int:
        """
        Discards all uncommitted in-memory buffers during fencing or teardown.
        Returns the number of discarded items.
        """
        count = len(self._uncommitted_buffers)
        self._uncommitted_buffers.clear()
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
        2. Closes connections / triggers stop event.
        3. Stops shard processing and flush loops.
        4. Discards uncommitted in-memory shard buffers.
        5. Prohibits reacquisition from within this instance.
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
        discarded = self.clear_uncommitted_buffers()

        logger.info(
            f"Stopped ShardRuntime for shard {self.shard_id} (claim {self.claim.claim_token}). Cleaned {discarded} buffers.",
            extra={
                "shard_id": self.shard_id,
                "worker_id": self.claim.worker_id,
                "claim_token": self.claim.claim_token,
                "event": "shard_shutdown"
            }
        )
