from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import CheckConstraint, DateTime, Float, ForeignKey, Integer, String
from .base import Base
from .raw_1m_candles import _OHLCV_VALIDITY_CHECK

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
    source: Mapped[str] = mapped_column(String(64), nullable=False, server_default="unknown")
    source_event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    data_revision: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")

    __table_args__ = (
        CheckConstraint(_OHLCV_VALIDITY_CHECK, name="ck_gap_staging_candles_ohlcv_valid"),
        CheckConstraint(
            "date_trunc('minute', timestamp) = timestamp",
            name="ck_gap_staging_candles_timestamp_minute",
        ),
        CheckConstraint("length(btrim(source)) > 0", name="ck_gap_staging_candles_source_nonempty"),
        CheckConstraint("data_revision > 0", name="ck_gap_staging_candles_positive_revision"),
    )
