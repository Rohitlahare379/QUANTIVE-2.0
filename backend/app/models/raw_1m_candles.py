from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import CheckConstraint, DateTime, Float, ForeignKey, Integer, String
from .base import Base


_FINITE_FLOAT = "{column} > '-Infinity'::double precision AND {column} < 'Infinity'::double precision"
_OHLCV_VALIDITY_CHECK = " AND ".join(
    _FINITE_FLOAT.format(column=column)
    for column in ("open", "high", "low", "close", "volume")
) + " AND open > 0 AND high > 0 AND low > 0 AND close > 0 AND volume >= 0 " \
    "AND high >= low AND open >= low AND open <= high AND close >= low AND close <= high"


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
    # Canonical provenance is intentionally retained on the current row.  Every
    # accepted revision is also appended to CandleRevision for forensic lineage.
    source: Mapped[str] = mapped_column(String(64), nullable=False, server_default="unknown")
    source_event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    data_revision: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")

    __table_args__ = (
        CheckConstraint(_OHLCV_VALIDITY_CHECK, name="ck_raw_1m_candles_ohlcv_valid"),
        CheckConstraint(
            "date_trunc('minute', timestamp) = timestamp",
            name="ck_raw_1m_candles_timestamp_minute",
        ),
        CheckConstraint("length(btrim(source)) > 0", name="ck_raw_1m_candles_source_nonempty"),
        CheckConstraint("data_revision > 0", name="ck_raw_1m_candles_positive_revision"),
    )
