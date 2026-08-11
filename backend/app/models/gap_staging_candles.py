from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import DateTime, Float, ForeignKey
from .base import Base

class GapStagingCandle(Base):
    """
    Standard PostgreSQL table (not a hypertable).
    Used to temporarily buffer historical gap repairs that target compressed chunks.
    A background job sequentially decompresses chunks, merges this data into raw_1m_candles,
    and recompresses the chunk, guaranteeing no OOM or locking failures.
    """
    __tablename__ = "gap_staging_candles"
    
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset_registry.id", ondelete="CASCADE"), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
