import json
from datetime import datetime
from fastapi import APIRouter, Depends, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db, limiter
from app.services.query import CandleQueryService

router = APIRouter(prefix="/candles", tags=["Candles"])

@router.get("")
@limiter.limit("60/minute")
async def get_candles(
    request: Request,
    asset_id: int,
    timeframe: str,
    start_time: datetime,
    end_time: datetime,
    db: AsyncSession = Depends(get_db)
):
    """
    Streams requested candles in Newline-Delimited JSON (NDJSON) format.
    Ensures O(1) memory usage regardless of dataset size.
    
    NOTE: Synchronous streaming queries are hard-capped to 30 days maximum to completely 
    prevent Slowloris DB Connection Pool exhaustion attacks. For queries > 30 days,
    use the Async `POST /exports` pipeline.
    """
    
    # Limit calculation based on timeframe or absolute max days to prevent DB pool exhaustion
    duration_days = (end_time - start_time).days
    if duration_days > 30:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=413, 
            detail="Payload Too Large. Live streaming queries are capped at 30 days to protect Database Connection Pools. Use POST /exports for massive historical extractions."
        )
        
    service = CandleQueryService(db)
    
    # We retrieve the AsyncGenerator from the service
    candle_stream = service.get_candles(
        asset_id=asset_id,
        timeframe=timeframe,
        start_time=start_time,
        end_time=end_time
    )

    async def ndjson_generator():
        async for candle in candle_stream:
            # We must serialize the datetime object explicitly
            candle["timestamp"] = candle["timestamp"].isoformat()
            yield json.dumps(candle) + "\n"

    return StreamingResponse(
        ndjson_generator(),
        media_type="application/x-ndjson"
    )
