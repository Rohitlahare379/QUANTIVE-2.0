from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict

class AssetResponse(BaseModel):
    id: int
    symbol: str
    exchange: str
    asset_type: str
    is_active: bool

    model_config = ConfigDict(from_attributes=True)

class SyncRangeResponse(BaseModel):
    start_timestamp: datetime
    end_timestamp: datetime

class SyncStatusResponse(BaseModel):
    asset_id: int
    synced_ranges: List[SyncRangeResponse]
