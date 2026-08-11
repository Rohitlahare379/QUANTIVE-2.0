import logging
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, delete, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert

from app.models.gap_staging_candles import GapStagingCandle
from app.models.raw_1m_candles import Raw1mCandle
from app.models.cagg_refresh_jobs import CaggRefreshJob, RefreshStatus

logger = logging.getLogger(__name__)

class HistoricalMergeService:
    """
    Dedicated batch worker to safely merge gap repairs into TimescaleDB compressed chunks.
    It sequentially identifies affected days, decompresses the chunks, performs the UPSERT,
    recompresses the chunks, and clears the staging table.
    """
    def __init__(self, db_session: AsyncSession):
        self.db = db_session

    async def merge_staged_data(self):
        # 1. Find all distinct days currently in the staging table
        stmt = text("SELECT DISTINCT date_trunc('day', timestamp) AS day_bucket FROM gap_staging_candles ORDER BY day_bucket ASC")
        result = await self.db.execute(stmt)
        days = [row[0] for row in result.fetchall()]

        if not days:
            logger.info("No staged historical data to merge.")
            return

        for day in days:
            # Ensure timezone awareness
            if day.tzinfo is None:
                day = day.replace(tzinfo=timezone.utc)
                
            next_day = day + timedelta(days=1)
            
            logger.info(f"Processing historical merge for chunk covering {day.date()}")

            try:
                # 2. Decompress chunks overlapping this day
                # show_chunks requires timestamp casts for correct matching
                decompress_stmt = text(f"""
                    SELECT decompress_chunk(c) 
                    FROM show_chunks('raw_1m_candles', 
                        newer_than => '{day.isoformat()}'::timestamptz, 
                        older_than => '{next_day.isoformat()}'::timestamptz
                    ) c;
                """)
                # In Timescale, if a chunk is already decompressed or not compressed, this might throw a warning,
                # but we handle it safely.
                await self.db.execute(decompress_stmt)
                await self.db.commit()
            except Exception as e:
                # If it's not compressed yet, that's fine. We log and proceed.
                logger.info(f"Decompression skipped or failed (might already be uncompressed): {e}")

            # 3. Merge data from staging to raw_1m_candles
            # Using transaction to ensure atomic insert + delete from staging
            async with self.db.begin():
                merge_stmt = text(f"""
                    INSERT INTO raw_1m_candles (asset_id, timestamp, open, high, low, close, volume)
                    SELECT asset_id, timestamp, open, high, low, close, volume
                    FROM gap_staging_candles
                    WHERE timestamp >= '{day.isoformat()}'::timestamptz 
                      AND timestamp < '{next_day.isoformat()}'::timestamptz
                    ON CONFLICT (asset_id, timestamp) DO NOTHING;
                """)
                await self.db.execute(merge_stmt)
                
                delete_stmt = text(f"""
                    DELETE FROM gap_staging_candles
                    WHERE timestamp >= '{day.isoformat()}'::timestamptz 
                      AND timestamp < '{next_day.isoformat()}'::timestamptz
                """)
                await self.db.execute(delete_stmt)
                
                # Register the CAGG refresh job for this modified window
                job = CaggRefreshJob(
                    window_start=day,
                    window_end=next_day,
                    status=RefreshStatus.PENDING
                )
                self.db.add(job)

            try:
                # 4. Re-compress the chunks
                compress_stmt = text(f"""
                    SELECT compress_chunk(c) 
                    FROM show_chunks('raw_1m_candles', 
                        newer_than => '{day.isoformat()}'::timestamptz, 
                        older_than => '{next_day.isoformat()}'::timestamptz
                    ) c;
                """)
                await self.db.execute(compress_stmt)
                await self.db.commit() # Autocommit for chunk functions outside transaction
            except Exception as e:
                logger.warning(f"Failed to recompress chunk for {day.date()}: {e}")
                
        logger.info("Historical merge complete.")
