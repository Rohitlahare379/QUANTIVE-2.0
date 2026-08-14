"""
Bounded Live Ingestion Pipeline for WebSocket Shard Events (P0.2 Phase 3).

Provides bounded buffering, backpressure threshold management, fair per-asset grouping,
chronological sorting & in-batch deduplication, strict validation, shard lease fencing checks,
and atomic persistence via existing IngestionService.
"""

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.connectors.models import CandleEvent
from app.core.config import settings
from app.services.ingestion import IngestionService
from app.services.ws_sharding.metrics import PipelineMetrics
from app.services.ws_sharding.registry import AssetRegistryResolver

logger = logging.getLogger(__name__)


def validate_candle_payload(event: CandleEvent) -> bool:
    """
    Validates candle OHLCV invariants defensively before queueing/persistence.
    """
    if not event.is_closed:
        return False

    if (
        event.open <= 0
        or event.high <= 0
        or event.low <= 0
        or event.close <= 0
        or event.volume < 0
    ):
        return False

    eps = 1e-9
    if event.high < (event.low - eps):
        return False
    if event.open < (event.low - eps) or event.open > (event.high + eps):
        return False
    if event.close < (event.low - eps) or event.close > (event.high + eps):
        return False

    if event.timestamp.tzinfo is None:
        return False

    return True


class BoundedLiveIngestionPipeline:
    """
    Bounded Live Ingestion Pipeline for an individual WebSocket Shard.
    """

    def __init__(
        self,
        shard_id: int,
        session_factory: Optional[async_sessionmaker[AsyncSession]] = None,
        asset_resolver: Optional[AssetRegistryResolver] = None,
        queue_maxsize: Optional[int] = None,
        batch_size: Optional[int] = None,
        flush_interval_ms: Optional[int] = None,
        fencing_check: Optional[Callable[[], bool]] = None,
    ):
        self.shard_id = shard_id
        self.session_factory = session_factory
        self.asset_resolver = asset_resolver or AssetRegistryResolver(session_factory=session_factory)
        self.queue_maxsize = queue_maxsize or settings.WS_QUEUE_MAXSIZE
        self.batch_size = batch_size or settings.WS_BATCH_SIZE
        self.flush_interval_seconds = float(flush_interval_ms or settings.WS_BATCH_FLUSH_INTERVAL_MS) / 1000.0
        self.fencing_check = fencing_check

        # Bounded asyncio Queue
        self._queue: asyncio.Queue[CandleEvent] = asyncio.Queue(maxsize=self.queue_maxsize)
        
        # Operational State
        self.metrics = PipelineMetrics()
        self._is_running = False
        self._flusher_task: Optional[asyncio.Task] = None
        self._flush_trigger_event = asyncio.Event()
        self._lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def queue_size(self) -> int:
        return self._queue.qsize()

    @property
    def queue_utilization(self) -> float:
        return float(self._queue.qsize()) / float(self.queue_maxsize) if self.queue_maxsize > 0 else 0.0

    async def enqueue_candle(self, event: CandleEvent) -> bool:
        """
        Enqueues a finalized CandleEvent with strict validation and backpressure threshold management.
        Returns True if successfully queued, False if rejected or dropped.
        """
        if not self._is_running:
            return False

        # 1. Defensive Validation
        if not validate_candle_payload(event):
            self.metrics.rejected_candles += 1
            logger.debug(f"[Shard {self.shard_id}] Rejected invalid candle: {event}")
            return False

        # 2. Check Fencing
        if self.fencing_check and not self.fencing_check():
            self.metrics.fenced_events_discarded += 1
            return False

        # 3. Utilization Threshold Evaluation
        current_size = self._queue.qsize()
        self.metrics.update_queue_stats(
            current_size=current_size,
            max_size=self.queue_maxsize,
            degraded_threshold=settings.WS_QUEUE_DEGRADED_THRESHOLD,
        )

        # Degraded Mode (90% capacity) -> Trigger immediate flush
        if self.metrics.is_degraded:
            self._flush_trigger_event.set()

        # 4. Enqueue with Backpressure (100% capacity)
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            # Apply controlled backpressure by waiting with bounded timeout
            try:
                self._flush_trigger_event.set()
                await asyncio.wait_for(self._queue.put(event), timeout=2.0)
            except (asyncio.TimeoutError, asyncio.QueueFull):
                self.metrics.queue_overflow_count += 1
                logger.warning(
                    f"[Shard {self.shard_id}] Queue full ({self.queue_maxsize}). Dropping event under backpressure.",
                    extra={"shard_id": self.shard_id, "event": "queue_overflow"}
                )
                return False

        self.metrics.candles_received += 1
        self.metrics.update_queue_stats(
            current_size=self._queue.qsize(),
            max_size=self.queue_maxsize,
            degraded_threshold=settings.WS_QUEUE_DEGRADED_THRESHOLD,
        )

        # Flush trigger if batch size reached
        if self._queue.qsize() >= self.batch_size:
            self._flush_trigger_event.set()

        return True

    async def start(self) -> None:
        """Starts the background batch flusher task."""
        if self._is_running:
            return
        self._is_running = True
        self._flush_trigger_event.clear()
        self._flusher_task = asyncio.create_task(
            self._flusher_loop(),
            name=f"ws-pipeline-flusher-shard-{self.shard_id}"
        )
        logger.info(f"Started BoundedLiveIngestionPipeline for shard {self.shard_id}")

    async def stop(self) -> None:
        """Gracefully stops the pipeline and flushes remaining queue items."""
        if not self._is_running and self._flusher_task is None:
            return

        self._is_running = False
        self._flush_trigger_event.set()

        if self._flusher_task:
            try:
                # Allow flusher loop to complete current iteration cleanly
                await asyncio.wait_for(asyncio.shield(self._flusher_task), timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._flusher_task.cancel()
                try:
                    await self._flusher_task
                except asyncio.CancelledError:
                    pass
            self._flusher_task = None

        # Clean drain if not fenced
        if not (self.fencing_check and not self.fencing_check()):
            await self.drain_and_flush()
        else:
            self.discard_uncommitted_buffers()

        logger.info(f"Stopped BoundedLiveIngestionPipeline for shard {self.shard_id}")

    def discard_uncommitted_buffers(self) -> int:
        """
        Discards all uncommitted in-memory queue items on fencing.
        Returns the number of discarded items.
        """
        count = 0
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                count += 1
            except (asyncio.QueueEmpty, ValueError):
                break

        self.metrics.fenced_events_discarded += count
        self.metrics.update_queue_stats(0, self.queue_maxsize)
        if count > 0:
            logger.warning(f"[Shard {self.shard_id}] Discarded {count} in-memory queue items due to fencing.")
        return count

    async def _flusher_loop(self) -> None:
        """
        Continuous flusher loop: wakes up either on timer expiration or trigger event.
        """
        while self._is_running:
            try:
                # Wait for batch trigger event or timeout
                try:
                    await asyncio.wait_for(
                        self._flush_trigger_event.wait(),
                        timeout=self.flush_interval_seconds
                    )
                except asyncio.TimeoutError:
                    pass

                self._flush_trigger_event.clear()

                if not self._is_running:
                    break

                # Drain available items up to batch_size
                batch = self._drain_batch_items(self.batch_size)
                if batch:
                    await self._process_and_flush_batch(batch)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Shard {self.shard_id}] Error in flusher loop: {e}", exc_info=True)
                await asyncio.sleep(0.1)

    def _drain_batch_items(self, max_items: int) -> List[CandleEvent]:
        """Drains up to max_items from the bounded queue without blocking."""
        batch: List[CandleEvent] = []
        while len(batch) < max_items and not self._queue.empty():
            try:
                item = self._queue.get_nowait()
                self._queue.task_done()
                batch.append(item)
            except (asyncio.QueueEmpty, ValueError):
                break

        self.metrics.update_queue_stats(
            current_size=self._queue.qsize(),
            max_size=self.queue_maxsize,
            degraded_threshold=settings.WS_QUEUE_DEGRADED_THRESHOLD,
        )
        return batch

    async def drain_and_flush(self) -> int:
        """
        Drains all remaining items from the queue and flushes them to the database.
        Returns total number of flushed items.
        """
        total_flushed = 0
        while not self._queue.empty():
            batch = self._drain_batch_items(self.batch_size)
            if not batch:
                break
            await self._process_and_flush_batch(batch)
            total_flushed += len(batch)
        return total_flushed

    async def _process_and_flush_batch(self, batch: List[CandleEvent]) -> None:
        """
        Processes a drained batch:
        1. Fencing check.
        2. Partition/Group by asset_id (via AssetRegistryResolver).
        3. Fair iteration across all assets in batch.
        4. Independent per-asset sorting, in-batch deduplication, and validation.
        5. Atomic commit via existing IngestionService._commit_batch.
        """
        if not batch:
            return

        # 1. Fencing Check
        if self.fencing_check and not self.fencing_check():
            self.metrics.fenced_events_discarded += len(batch)
            logger.warning(f"[Shard {self.shard_id}] Fenced: aborted batch flush of {len(batch)} items.")
            return

        # 2. Partition by Asset
        asset_groups: Dict[int, List[CandleEvent]] = defaultdict(list)
        for event in batch:
            asset_info = await self.asset_resolver.resolve_symbol(event.symbol)
            if asset_info is None:
                self.metrics.unmapped_symbol_rejections += 1
                logger.debug(f"[Shard {self.shard_id}] Unknown symbol rejected: {event.symbol}")
                continue

            asset_id, is_active = asset_info
            if not is_active:
                self.metrics.inactive_asset_rejections += 1
                logger.debug(f"[Shard {self.shard_id}] Inactive asset rejected: {event.symbol} (id={asset_id})")
                continue

            asset_groups[asset_id].append(event)

        if not asset_groups:
            return

        # 3. Fair Per-Asset Processing
        for asset_id, events in asset_groups.items():
            # Check fencing between assets
            if self.fencing_check and not self.fencing_check():
                self.metrics.fenced_events_discarded += len(events)
                logger.warning(f"[Shard {self.shard_id}] Fenced: stopped multi-asset flush at asset {asset_id}.")
                return

            # Independent Chronological Ordering & In-Batch Deduplication
            sorted_events = sorted(events, key=lambda e: e.timestamp)
            deduped_events: List[CandleEvent] = []
            seen_timestamps: Set[datetime] = set()

            for e in sorted_events:
                if e.timestamp in seen_timestamps:
                    self.metrics.duplicate_candles += 1
                    logger.debug(f"[Shard {self.shard_id}] Deduplicated duplicate candle for asset {asset_id} at {e.timestamp}")
                else:
                    seen_timestamps.add(e.timestamp)
                    deduped_events.append(e)

            if not deduped_events:
                continue

            # Format payload for IngestionService
            candles_payload = [
                {
                    "asset_id": asset_id,
                    "timestamp": e.timestamp,
                    "open": e.open,
                    "high": e.high,
                    "low": e.low,
                    "close": e.close,
                    "volume": e.volume,
                }
                for e in deduped_events
            ]

            # 4. Commit via existing IngestionService
            await self._commit_asset_batch(asset_id, candles_payload)

    async def _commit_asset_batch(self, asset_id: int, candles_payload: List[Dict[str, Any]]) -> None:
        """
        Commits an asset's candle batch to PostgreSQL using IngestionService._commit_batch.
        """
        start_time = time.perf_counter()

        if self.session_factory is None:
            # If running in mock/offline mode, record completion
            latency_ms = (time.perf_counter() - start_time) * 1000.0
            self.metrics.record_flush_complete(len(candles_payload), latency_ms)
            return

        try:
            async with self.session_factory() as session:
                service = IngestionService(db_session=session)
                await service._commit_batch(asset_id, candles_payload)

            latency_ms = (time.perf_counter() - start_time) * 1000.0
            self.metrics.record_flush_complete(len(candles_payload), latency_ms)

        except Exception as e:
            self.metrics.persistence_errors += 1
            logger.error(
                f"[Shard {self.shard_id}] Failed to commit {len(candles_payload)} candles for asset {asset_id}: {e}",
                extra={"shard_id": self.shard_id, "asset_id": asset_id, "error": str(e), "event": "persistence_error"}
            )
            # Transaction is cleanly rolled back by async with session
