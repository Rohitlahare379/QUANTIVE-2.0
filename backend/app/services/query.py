from typing import AsyncGenerator, Dict, Any, List
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.raw_1m_candles import Raw1mCandle
from app.models.aggregates import candles_5m, candles_15m, candles_1h, candles_4h, candles_1d
from app.repositories.asset import AssetRepository
from app.repositories.candle import CandleRepository
from app.services.exceptions import (
    AssetNotFoundError,
    UnsupportedTimeframeError,
    InvalidDateRangeError
)

# Timeframe Router
TIMEFRAME_TABLE_MAP = {
    "1m": Raw1mCandle.__table__, # Standardize to Table object for uniformity
    "5m": candles_5m,
    "15m": candles_15m,
    "1h": candles_1h,
    "4h": candles_4h,
    "1d": candles_1d,
}

TIMEFRAME_LIMITS_DAYS = {
    "1m": 30,
    "5m": 90,
    "15m": 180,
    "1h": 365 * 5,
    "4h": 365 * 10,
    "1d": None,
}

class AssetQueryService:
    def __init__(self, db: AsyncSession):
        self.repo = AssetRepository(db)

    async def get_asset_by_symbol(self, symbol: str) -> Dict[str, Any]:
        asset = await self.repo.get_by_symbol(symbol)
        if not asset:
            raise AssetNotFoundError(f"Asset with symbol {symbol} not found")
        return {
            "id": asset.id,
            "symbol": asset.symbol,
            "exchange": asset.exchange,
            "asset_type": asset.asset_type,
            "is_active": asset.is_active
        }

    async def list_assets(self) -> List[Dict[str, Any]]:
        assets = await self.repo.list_assets()
        return [
            {
                "id": asset.id,
                "symbol": asset.symbol,
                "exchange": asset.exchange,
                "asset_type": asset.asset_type,
                "is_active": asset.is_active
            }
            for asset in assets
        ]


class CandleQueryService:
    def __init__(self, db: AsyncSession):
        self.asset_repo = AssetRepository(db)
        self.candle_repo = CandleRepository(db)

    async def get_candles(
        self,
        asset_id: int,
        timeframe: str,
        start_time: datetime,
        end_time: datetime
    ) -> AsyncGenerator[Dict[str, Any], None]:
        
        # 1. Validation
        if timeframe not in TIMEFRAME_TABLE_MAP:
            raise UnsupportedTimeframeError(f"Unsupported timeframe: {timeframe}")
        
        if start_time >= end_time:
            raise InvalidDateRangeError("start_time must be before end_time")
        
        # Ensure UTC timezone awareness
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        # 1.5. Window Limit Validation
        duration_days = (end_time - start_time).days
        max_days = TIMEFRAME_LIMITS_DAYS[timeframe]
        
        if max_days is not None and duration_days > max_days:
            raise InvalidDateRangeError(f"Query window for {timeframe} exceeds maximum allowed of {max_days} days. Requested: {duration_days} days.")

        # 2. Asset Check
        asset = await self.asset_repo.get_by_id(asset_id)
        if not asset:
            raise AssetNotFoundError(f"Asset with id {asset_id} not found")

        # 3. Route to specific table
        table_model = TIMEFRAME_TABLE_MAP[timeframe]

        # 4. Stream & Normalize results
        stream = self.candle_repo.get_candles_stream(
            table_model=table_model,
            asset_id=asset_id,
            start_time=start_time,
            end_time=end_time
        )
        
        async for row in stream:
            # We map the SQLAlchemy Row object to a standard dict
            # Access by string name since it's a Table mapping.
            # Row mapping in SA 2.0 can be accessed via `row._mapping`.
            mapping = row._mapping
            yield {
                "timestamp": mapping["timestamp"],
                "open": float(mapping["open"]),
                "high": float(mapping["high"]),
                "low": float(mapping["low"]),
                "close": float(mapping["close"]),
                "volume": float(mapping["volume"])
            }
