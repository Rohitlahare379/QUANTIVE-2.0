import logging
import math
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Iterable, List, Optional, Tuple

from sqlalchemy import and_, case, delete, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.models.asset_registry import AssetRegistry
from app.models.candle_revision import CandleRevision
from app.models.sync_ranges import SyncRange
from app.models.raw_1m_candles import Raw1mCandle
from app.models.gap_staging_candles import GapStagingCandle
from app.connectors.binance import BinanceClient
from app.connectors.exceptions import PayloadCorruptionError
from app.core.config import settings

logger = logging.getLogger(__name__)


_CANDLE_COLUMNS = (
    "asset_id",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "source_event_time",
    "source_received_at",
)


class IngestionService:
    def __init__(self, db_session: AsyncSession, binance_client: Optional[BinanceClient] = None):
        self.db = db_session
        self.client = binance_client

    async def detect_missing_ranges(
        self,
        asset_id: int,
        requested_start: datetime,
        requested_end: datetime,
        *,
        max_ranges: Optional[int] = None,
        max_gaps: Optional[int] = None,
    ) -> List[Tuple[datetime, datetime]]:
        """Return a bounded, conservative set of missing canonical intervals.

        ``sync_ranges`` is normally compact, but a long outage or legacy data
        can leave an arbitrary number of fragments.  Loading all fragments into
        Python would make gap detection itself an unbounded-memory path.  The
        query is therefore capped.  If the cap is reached, the unscanned suffix
        is deliberately treated as missing (and coalesced into the final
        returned interval); this can request idempotent extra REST data, but it
        can never claim coverage that was not inspected.
        """
        max_ranges = (
            settings.GAP_REPAIR_MAX_SYNC_RANGES_PER_DETECTION
            if max_ranges is None
            else max_ranges
        )
        max_gaps = (
            settings.GAP_REPAIR_MAX_GAPS_PER_DETECTION if max_gaps is None else max_gaps
        )
        if max_ranges <= 0 or max_gaps <= 0:
            raise ValueError("max_ranges and max_gaps must be positive")
        if requested_start >= requested_end:
            return []

        stmt = (
            select(SyncRange)
            .where(
                SyncRange.asset_id == asset_id,
                SyncRange.end_timestamp >= requested_start,
                SyncRange.start_timestamp <= requested_end
            )
            .order_by(SyncRange.start_timestamp.asc())
            # Fetch one sentinel row so truncation is explicit while the
            # result set remains bounded even for pathological fragmentation.
            .limit(max_ranges + 1)
        )
        result = await self.db.execute(stmt)
        existing_ranges = result.scalars().all()

        ranges_truncated = len(existing_ranges) > max_ranges
        if ranges_truncated:
            existing_ranges = existing_ranges[:max_ranges]

        gaps: List[Tuple[datetime, datetime]] = []
        current_pointer = requested_start

        def append_gap(start: datetime, end: datetime) -> bool:
            """Append a gap, coalescing the unknown suffix at the hard cap."""
            if start >= end:
                return False
            if len(gaps) < max_gaps:
                gaps.append((start, end))
                return False
            # The range cap was reached before we could enumerate all gaps.
            # Replace the final known gap with a conservative interval through
            # the requested end, keeping the returned list hard-bounded.
            prior_start, _ = gaps[-1]
            gaps[-1] = (prior_start, requested_end)
            return True

        for r in existing_ranges:
            if current_pointer < r.start_timestamp:
                if append_gap(current_pointer, r.start_timestamp):
                    ranges_truncated = True
                    break
            current_pointer = max(current_pointer, r.end_timestamp)
            if current_pointer >= requested_end:
                break

        if current_pointer < requested_end:
            if ranges_truncated:
                # We did not inspect whether later rows fill this suffix.  It
                # is safe only to repair it conservatively, never to call it
                # covered.
                append_gap(current_pointer, requested_end)
            else:
                append_gap(current_pointer, requested_end)

        if ranges_truncated:
            logger.warning(
                "Bounded sync-range detection reached its cap for asset %s; "
                "treating the unscanned suffix as unresolved",
                asset_id,
                extra={
                    "asset_id": asset_id,
                    "max_ranges": max_ranges,
                    "max_gaps": max_gaps,
                    "event": "sync_range_scan_bounded",
                },
            )

        return gaps

    @staticmethod
    def _source_priority(source_column: Any) -> Any:
        """Return a stable SQL source-precedence expression.

        WebSocket remains the primary live path.  A REST response is allowed to
        correct a finalized WebSocket candle, while a delayed WebSocket replay can
        never erase an accepted REST correction.  Unknown/legacy rows are never
        allowed to outrank either connector.
        """
        return case(
            (source_column == "binance_rest", 30),
            (source_column == "binance_ws", 20),
            (source_column == "legacy", 0),
            else_=10,
        )

    @staticmethod
    def _ohlcv_changed(target_model: Any, excluded: Any) -> Any:
        return or_(
            target_model.open.is_distinct_from(excluded.open),
            target_model.high.is_distinct_from(excluded.high),
            target_model.low.is_distinct_from(excluded.low),
            target_model.close.is_distinct_from(excluded.close),
            target_model.volume.is_distinct_from(excluded.volume),
        )

    @staticmethod
    def _canonical_values(candles: Iterable[dict]) -> List[dict]:
        """Keep only columns persisted by raw/staging candle tables."""
        return [{column: candle.get(column) for column in _CANDLE_COLUMNS} for candle in candles]

    async def _append_raw_revisions(self, changed_rows: List[dict]) -> None:
        """Append accepted canonical inserts/corrections in the caller transaction."""
        if not changed_rows:
            return

        recorded_at = datetime.now(timezone.utc)
        rows = [
            {
                "asset_id": row["asset_id"],
                "timestamp": row["timestamp"],
                "data_revision": row["data_revision"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "source": row["source"],
                "source_event_time": row["source_event_time"],
                "source_received_at": row["source_received_at"],
                "recorded_at": recorded_at,
            }
            for row in changed_rows
        ]
        statement = insert(CandleRevision).values(rows).on_conflict_do_nothing(
            index_elements=["asset_id", "timestamp", "data_revision"]
        )
        await self.db.execute(statement)

    async def _upsert_raw_candles(self, candles: List[dict]) -> List[dict]:
        """Upsert canonical raw rows and return values actually accepted.

        Exact duplicates return no row.  A lower-precedence transport cannot
        overwrite an accepted correction.  Every insert or accepted correction
        advances ``data_revision`` and is appended to ``candle_revisions`` in the
        same transaction.
        """
        values = self._canonical_values(candles)
        statement = insert(Raw1mCandle).values(values)
        excluded = statement.excluded
        changed = self._ohlcv_changed(Raw1mCandle, excluded)
        authoritative = self._source_priority(excluded.source) >= self._source_priority(
            Raw1mCandle.source
        )
        statement = statement.on_conflict_do_update(
            index_elements=["asset_id", "timestamp"],
            set_={
                "open": excluded.open,
                "high": excluded.high,
                "low": excluded.low,
                "close": excluded.close,
                "volume": excluded.volume,
                "source": excluded.source,
                "source_event_time": excluded.source_event_time,
                "source_received_at": excluded.source_received_at,
                "data_revision": Raw1mCandle.data_revision + 1,
            },
            where=and_(changed, authoritative),
        ).returning(
            Raw1mCandle.asset_id,
            Raw1mCandle.timestamp,
            Raw1mCandle.data_revision,
            Raw1mCandle.open,
            Raw1mCandle.high,
            Raw1mCandle.low,
            Raw1mCandle.close,
            Raw1mCandle.volume,
            Raw1mCandle.source,
            Raw1mCandle.source_event_time,
            Raw1mCandle.source_received_at,
        )
        result = await self.db.execute(statement)
        changed_rows = [dict(row) for row in result.mappings().all()]
        await self._append_raw_revisions(changed_rows)
        return changed_rows

    async def _upsert_staged_candles(self, candles: List[dict]) -> None:
        """Persist staged history with the same correction precedence as raw data."""
        values = self._canonical_values(candles)
        statement = insert(GapStagingCandle).values(values)
        excluded = statement.excluded
        changed = self._ohlcv_changed(GapStagingCandle, excluded)
        authoritative = self._source_priority(excluded.source) >= self._source_priority(
            GapStagingCandle.source
        )
        statement = statement.on_conflict_do_update(
            index_elements=["asset_id", "timestamp"],
            set_={
                "open": excluded.open,
                "high": excluded.high,
                "low": excluded.low,
                "close": excluded.close,
                "volume": excluded.volume,
                "source": excluded.source,
                "source_event_time": excluded.source_event_time,
                "source_received_at": excluded.source_received_at,
                "data_revision": GapStagingCandle.data_revision + 1,
            },
            where=and_(changed, authoritative),
        )
        await self.db.execute(statement)

    async def insert_candle_batch(self, candles: List[dict], target_model=Raw1mCandle):
        """Persist a validated batch using deterministic canonical semantics.

        ``Raw1mCandle`` is not a first-writer-wins table: REST may replace a
        materially different finalized WebSocket candle and records a new
        immutable revision.  Staging follows the same precedence but never claims
        canonical coverage until its rows are promoted to raw.
        """
        if not candles:
            return []

        # This method is also used directly by bounded worker pages and legacy
        # call sites.  Do not let those callers bypass the validation/provenance
        # boundary merely by avoiding `_commit_batch`.
        batch_asset_id = candles[0].get("asset_id")
        if not isinstance(batch_asset_id, int):
            raise PayloadCorruptionError("Candle batch requires an integer asset_id")
        self._prepare_batch(batch_asset_id, candles)

        if target_model is Raw1mCandle:
            return await self._upsert_raw_candles(candles)
        if target_model is GapStagingCandle:
            await self._upsert_staged_candles(candles)
            return []

        statement = insert(target_model).values(candles).on_conflict_do_nothing(
            index_elements=["asset_id", "timestamp"]
        )
        await self.db.execute(statement)
        return []

    async def _merge_verified_sync_ranges(
        self, asset_id: int, new_start: datetime, new_end: datetime
    ) -> None:
        """
        Merge an interval already proven against raw storage.

        Callers must hold the same transaction that established raw coverage.
        Keeping this mutation separate from the proof makes the invariant obvious
        in the two canonical write paths below while keeping the range merge
        itself serializable per asset.
        """
        # We consider ranges "touching" if they are within 1 minute of each other.
        margin = timedelta(minutes=1)

        # Acquire asset-level row lock to strictly serialize range merges per asset
        await self.db.execute(
            select(AssetRegistry.id).where(AssetRegistry.id == asset_id).with_for_update()
        )
        
        stmt = select(SyncRange).where(
            SyncRange.asset_id == asset_id,
            SyncRange.start_timestamp <= new_end + margin,
            SyncRange.end_timestamp >= new_start - margin
        ).with_for_update()
        result = await self.db.execute(stmt)
        overlaps = result.scalars().all()

        merged_start = new_start
        merged_end = new_end
        ids_to_delete = []

        for r in overlaps:
            merged_start = min(merged_start, r.start_timestamp)
            merged_end = max(merged_end, r.end_timestamp)
            ids_to_delete.append(r.id)

        if ids_to_delete:
            delete_stmt = delete(SyncRange).where(SyncRange.id.in_(ids_to_delete))
            await self.db.execute(delete_stmt)

        new_range = SyncRange(
            asset_id=asset_id,
            start_timestamp=merged_start,
            end_timestamp=merged_end
        )
        self.db.add(new_range)
        await self.db.flush()

    async def update_sync_ranges(self, asset_id: int, new_start: datetime, new_end: datetime):
        """Safely register coverage only when canonical raw rows prove it exists.

        This compatibility method is intentionally safe for direct callers: a
        range cannot be asserted based on a downloaded payload, a staging row, or
        caller intent alone.
        """
        if not await self._raw_block_is_complete(asset_id, new_start, new_end):
            raise RuntimeError(
                "Refusing to update sync_ranges for an interval that is not fully present in raw_1m_candles"
            )
        await self._merge_verified_sync_ranges(asset_id, new_start, new_end)

    async def sync_asset(
        self, asset_id: int, symbol: str, start_time: datetime, end_time: datetime
    ):
        """
        Detects missing coverage, fetches it from Binance, and persists it in batches.

        The coverage query is deliberately closed before the first network request.  A
        worker may keep this service object for the complete sync, but it must never
        retain a PostgreSQL transaction/connection while waiting on Binance.
        """
        if self.client is None:
            raise RuntimeError("sync_asset requires an initialized Binance client")

        await self._validate_binance_asset(asset_id, symbol)
        gaps = await self.detect_missing_ranges(asset_id, start_time, end_time)

        # SQLAlchemy autobegins a transaction for the SELECT above.  Releasing it here
        # both avoids a nested `begin()` in _commit_batch and returns the pooled
        # connection before potentially long-running external network I/O.
        await self.db.rollback()

        batch_size = 5000

        for gap_start, gap_end in gaps:
            logger.info(f"Syncing {symbol} gap: {gap_start} to {gap_end}")
            
            candles_batch = []
            
            async for candle in self.client.get_klines(symbol, "1m", gap_start, gap_end):
                candle["asset_id"] = asset_id
                candles_batch.append(candle)
                
                if len(candles_batch) >= batch_size:
                    await self._commit_batch(asset_id, candles_batch)
                    candles_batch = []
                    
            if candles_batch:
                await self._commit_batch(asset_id, candles_batch)

    async def _validate_binance_asset(self, asset_id: int, symbol: str) -> None:
        """Prevent the Binance REST transport from writing into another exchange's row."""
        result = await self.db.execute(
            select(AssetRegistry.symbol).where(
                AssetRegistry.id == asset_id,
                AssetRegistry.exchange == "BINANCE",
                AssetRegistry.is_active.is_(True),
            )
        )
        registered_symbol = result.scalar_one_or_none()
        if registered_symbol is None:
            raise ValueError(f"Asset {asset_id} is not an active BINANCE asset")
        if registered_symbol.strip().upper() != symbol.strip().upper():
            raise ValueError(
                f"Binance symbol {symbol!r} does not match asset {asset_id} registry symbol {registered_symbol!r}"
            )

    @staticmethod
    def _validate_candle_payload(asset_id: int, candle: dict) -> None:
        """Validate persisted OHLCV records before any database transaction starts.

        REST and WebSocket boundaries already validate their own payloads.  This
        second check protects direct worker/service callers and makes a failed batch
        incapable of creating sync coverage metadata.  The all-fields guard keeps
        the existing metadata-only service tests compatible; real inserts still
        require every column at the database boundary.
        """
        timestamp = candle.get("timestamp")
        if not isinstance(timestamp, datetime):
            raise PayloadCorruptionError("Candle timestamp must be a datetime")
        if timestamp.tzinfo is None:
            raise PayloadCorruptionError("Candle timestamp must be timezone-aware")
        timestamp = timestamp.astimezone(timezone.utc)
        if timestamp.second != 0 or timestamp.microsecond != 0:
            raise PayloadCorruptionError("Raw 1m candle timestamps must align exactly to a UTC minute")
        candle["timestamp"] = timestamp

        record_asset_id = candle.get("asset_id")
        if record_asset_id is not None and record_asset_id != asset_id:
            raise PayloadCorruptionError(
                f"Candle asset_id {record_asset_id} does not match batch asset_id {asset_id}"
            )
        candle["asset_id"] = asset_id

        source = candle.get("source", "unknown")
        if not isinstance(source, str) or not source.strip() or len(source.strip()) > 64:
            raise PayloadCorruptionError("Candle source must be a non-empty string of at most 64 characters")
        candle["source"] = source.strip().lower()

        for provenance_field in ("source_event_time", "source_received_at"):
            provenance_value = candle.get(provenance_field)
            if provenance_value is None:
                continue
            if not isinstance(provenance_value, datetime) or provenance_value.tzinfo is None:
                raise PayloadCorruptionError(
                    f"Candle {provenance_field} must be a timezone-aware datetime when provided"
                )
            candle[provenance_field] = provenance_value.astimezone(timezone.utc)
        candle.setdefault("source_received_at", datetime.now(timezone.utc))

        ohlcv_fields = ("open", "high", "low", "close", "volume")
        present_fields = [field for field in ohlcv_fields if field in candle]
        if not present_fields:
            return
        if len(present_fields) != len(ohlcv_fields):
            raise PayloadCorruptionError("Candle payload must contain complete OHLCV fields")

        try:
            open_price = float(candle["open"])
            high_price = float(candle["high"])
            low_price = float(candle["low"])
            close_price = float(candle["close"])
            volume = float(candle["volume"])
        except (TypeError, ValueError) as exc:
            raise PayloadCorruptionError("Candle OHLCV values must be numeric") from exc

        values = (open_price, high_price, low_price, close_price, volume)
        if not all(math.isfinite(value) for value in values):
            raise PayloadCorruptionError("Candle OHLCV values must be finite")
        if min(open_price, high_price, low_price, close_price) <= 0:
            raise PayloadCorruptionError("Candle prices must be strictly positive")
        if volume < 0:
            raise PayloadCorruptionError("Candle volume cannot be negative")

        epsilon = 1e-9
        if high_price < low_price - epsilon:
            raise PayloadCorruptionError("Candle high cannot be lower than low")
        if not low_price - epsilon <= open_price <= high_price + epsilon:
            raise PayloadCorruptionError("Candle open must be within [low, high]")
        if not low_price - epsilon <= close_price <= high_price + epsilon:
            raise PayloadCorruptionError("Candle close must be within [low, high]")

    @staticmethod
    def _contiguous_blocks(candles: List[dict]) -> List[Tuple[datetime, datetime]]:
        """Validate ordered 1m timestamps and return exact contiguous blocks."""
        if not candles:
            return []

        blocks: List[Tuple[datetime, datetime]] = []
        block_start = candles[0]["timestamp"]
        previous = block_start
        for candle in candles[1:]:
            current = candle["timestamp"]
            if current <= previous:
                raise PayloadCorruptionError(
                    "Payload corruption detected: candle timestamps must be strictly increasing. "
                    f"Timestamp {current} is <= {previous}."
                )
            if (current - previous).total_seconds() != 60:
                blocks.append((block_start, previous))
                block_start = current
            previous = current
        blocks.append((block_start, previous))
        return blocks

    async def _raw_block_is_complete(
        self, asset_id: int, block_start: datetime, block_end: datetime
    ) -> bool:
        """Prove every minute in a claimed interval exists in canonical raw data.

        The primary key and database minute-alignment constraint mean an exact
        count with matching min/max is a proof of contiguous raw coverage.  This
        query runs inside the same transaction as the raw upsert and range update.
        """
        expected_count = int((block_end - block_start).total_seconds() // 60) + 1
        statement = select(
            func.count(Raw1mCandle.timestamp),
            func.min(Raw1mCandle.timestamp),
            func.max(Raw1mCandle.timestamp),
        ).where(
            Raw1mCandle.asset_id == asset_id,
            Raw1mCandle.timestamp >= block_start,
            Raw1mCandle.timestamp <= block_end,
        )
        result = await self.db.execute(statement)
        count, first_timestamp, last_timestamp = result.one()
        return (
            int(count) == expected_count
            and first_timestamp == block_start
            and last_timestamp == block_end
        )

    async def _mark_verified_raw_blocks(
        self, asset_id: int, blocks: List[Tuple[datetime, datetime]]
    ) -> None:
        for block_start, block_end in blocks:
            if not await self._raw_block_is_complete(asset_id, block_start, block_end):
                logger.error(
                    "Refusing to claim sync coverage for asset %s [%s, %s]: canonical raw rows are incomplete",
                    asset_id,
                    block_start,
                    block_end,
                )
                continue
            await self._merge_verified_sync_ranges(asset_id, block_start, block_end)

    @staticmethod
    def _prepare_batch(asset_id: int, candles_batch: List[dict]) -> None:
        for candle in candles_batch:
            IngestionService._validate_candle_payload(asset_id, candle)
        IngestionService._contiguous_blocks(candles_batch)

    async def _lock_asset(self, asset_id: int) -> None:
        await self.db.execute(
            select(AssetRegistry.id).where(AssetRegistry.id == asset_id).with_for_update()
        )

    async def commit_raw_batch_in_transaction(self, asset_id: int, candles_batch: List[dict]) -> None:
        """Persist an already-selected canonical raw page in an open transaction.

        Historical-merge and fenced gap workers use this path after their own
        ownership predicate has been checked.  It never routes rows to staging,
        and it never marks ``sync_ranges`` until the raw table proves coverage.
        """
        if not candles_batch:
            return
        self._prepare_batch(asset_id, candles_batch)
        await self._lock_asset(asset_id)
        await self.insert_candle_batch(candles_batch, target_model=Raw1mCandle)
        await self._mark_verified_raw_blocks(asset_id, self._contiguous_blocks(candles_batch))

    async def commit_batch_in_transaction(self, asset_id: int, candles_batch: List[dict]) -> None:
        """Persist a normal ingestion batch in an already-open transaction.

        Recent data writes directly to canonical raw storage.  Older data remains
        staging-only until the historical merger calls
        :meth:`commit_raw_batch_in_transaction`; staging rows never create a
        ``sync_ranges`` assertion.
        """
        if not candles_batch:
            return
        self._prepare_batch(asset_id, candles_batch)
        await self._lock_asset(asset_id)

        cutoff_date = datetime.now(timezone.utc) - timedelta(days=7)
        live_candles = [candle for candle in candles_batch if candle["timestamp"] >= cutoff_date]
        historical_candles = [candle for candle in candles_batch if candle["timestamp"] < cutoff_date]

        if live_candles:
            await self.insert_candle_batch(live_candles, target_model=Raw1mCandle)
            await self._mark_verified_raw_blocks(asset_id, self._contiguous_blocks(live_candles))
        if historical_candles:
            await self.insert_candle_batch(historical_candles, target_model=GapStagingCandle)

    async def _commit_batch(
        self,
        asset_id: int,
        candles_batch: List[dict],
        *,
        ownership_check: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> None:
        """Transaction-owning compatibility wrapper for standard ingestion.

        ``ownership_check`` is intentionally invoked only after the transaction
        begins.  WebSocket persistence fencing uses it to lock and verify the
        current shard generation in the same transaction as raw candle writes;
        a Redis-only check outside this boundary could race a successor.
        """
        if not candles_batch:
            return
        async with self.db.begin():
            if ownership_check is not None:
                await ownership_check()
            await self.commit_batch_in_transaction(asset_id, candles_batch)
