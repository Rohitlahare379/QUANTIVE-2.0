from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import DateTime, Float, ForeignKey
from .base import Base

class Raw1mCandle(Base):
    __tablename__ = "raw_1m_candles"
    
    # TimescaleDB requires the partitioning column (timestamp) to be part of the primary key
    # By using (asset_id, timestamp) as the composite primary key, we natively prevent duplicates.
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset_registry.id", ondelete="CASCADE"), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
