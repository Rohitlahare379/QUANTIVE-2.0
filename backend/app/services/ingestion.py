import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.models.asset_registry import AssetRegistry
from app.models.sync_ranges import SyncRange
from app.models.raw_1m_candles import Raw1mCandle
from app.models.gap_staging_candles import GapStagingCandle
from app.connectors.binance import BinanceClient
from app.connectors.exceptions import PayloadCorruptionError

logger = logging.getLogger(__name__)

class IngestionService:
    def __init__(self, db_session: AsyncSession, binance_client: Optional[BinanceClient] = None):
        self.db = db_session
        self.client = binance_client

    async def detect_missing_ranges(
        self, asset_id: int, requested_start: datetime, requested_end: datetime
    ) -> List[Tuple[datetime, datetime]]:
        """
        Calculates the gaps between requested_start and requested_end by querying existing ranges.
        """
        stmt = (
            select(SyncRange)
            .where(
                SyncRange.asset_id == asset_id,
                SyncRange.end_timestamp >= requested_start,
                SyncRange.start_timestamp <= requested_end
            )
            .order_by(SyncRange.start_timestamp.asc())
        )
        result = await self.db.execute(stmt)
        existing_ranges = result.scalars().all()

        gaps = []
        current_pointer = requested_start

        for r in existing_ranges:
            if current_pointer < r.start_timestamp:
                gaps.append((current_pointer, r.start_timestamp))
            current_pointer = max(current_pointer, r.end_timestamp)

        if current_pointer < requested_end:
            gaps.append((current_pointer, requested_end))

        return gaps

    async def insert_candle_batch(self, candles: List[dict], target_model=Raw1mCandle):
        """
        Inserts a batch of candles using SQLAlchemy Core with ON CONFLICT DO NOTHING.
        """
        if not candles:
            return

        stmt = insert(target_model).values(candles)
        # Handle duplicates elegantly to prevent pipeline crashes
        stmt = stmt.on_conflict_do_nothing(index_elements=['asset_id', 'timestamp'])
        await self.db.execute(stmt)

    async def update_sync_ranges(self, asset_id: int, new_start: datetime, new_end: datetime):
        """
        Merges overlapping or touching ranges and inserts the new merged range.
        Must be called within a transaction context.
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

    async def sync_asset(
        self, asset_id: int, symbol: str, start_time: datetime, end_time: datetime
    ):
        """
        The main orchestrator. Detects gaps, fetches from Binance, batches inserts.
        """
        gaps = await self.detect_missing_ranges(asset_id, start_time, end_time)
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

    async def _commit_batch(self, asset_id: int, candles_batch: List[dict]):
        if not candles_batch:
            return

        # 1. Fragment payload into perfectly contiguous sub-blocks
        contiguous_blocks = []
        current_block = [candles_batch[0]]
        
        for i in range(1, len(candles_batch)):
            prev = candles_batch[i-1]
            curr = candles_batch[i]
            
            # Strict ordering validation
            if curr["timestamp"] <= prev["timestamp"]:
                raise PayloadCorruptionError(
                    f"Payload corruption detected for asset {asset_id}. "
                    f"Timestamp {curr['timestamp']} is <= {prev['timestamp']}."
                )
            
            delta_seconds = (curr["timestamp"] - prev["timestamp"]).total_seconds()
            
            if delta_seconds == 60:
                current_block.append(curr)
            else:
                # delta > 60 seconds (since we already guarded against <= 0)
                contiguous_blocks.append((current_block[0]["timestamp"], current_block[-1]["timestamp"]))
                current_block = [curr]
                
        if current_block:
            contiguous_blocks.append((current_block[0]["timestamp"], current_block[-1]["timestamp"]))
            
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=7)
        historical_candles = []
        live_candles = []
        
        for c in candles_batch:
            # Ensure timezone awareness for comparison
            ts = c["timestamp"]
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
                
            if ts < cutoff_date:
                historical_candles.append(c)
            else:
                live_candles.append(c)

        # 2. Guarantee atomicity using explicit transaction block
        async with self.db.begin():
            # Lock asset row first to establish consistent lock ordering across concurrent commits
            await self.db.execute(
                select(AssetRegistry.id).where(AssetRegistry.id == asset_id).with_for_update()
            )

            # Bulk inserts
            if live_candles:
                await self.insert_candle_batch(live_candles, target_model=Raw1mCandle)
            if historical_candles:
                await self.insert_candle_batch(historical_candles, target_model=GapStagingCandle)
            
            # Multiple metadata merges strictly for contiguous blocks
            for block_start, block_end in contiguous_blocks:
                await self.update_sync_ranges(asset_id, block_start, block_end)
