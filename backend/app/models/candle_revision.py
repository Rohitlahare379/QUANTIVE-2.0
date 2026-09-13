"""Append-only lineage for canonical candle revisions."""

from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base
from .raw_1m_candles import _OHLCV_VALIDITY_CHECK


class CandleRevision(Base):
    """An immutable canonical value accepted for an asset/minute revision.

    ``raw_1m_candles`` holds the currently authoritative value.  This table is a
    compact append-only ledger that makes an accepted REST correction traceable
    without allowing a later lower-priority WebSocket replay to overwrite it.
    """

    __tablename__ = "candle_revisions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    asset_id: Mapped[int] = mapped_column(
        ForeignKey("asset_registry.id", ondelete="CASCADE"), nullable=False
    )
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    data_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    source_event_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    recorded_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "asset_id",
            "timestamp",
            "data_revision",
            name="uq_candle_revisions_asset_timestamp_revision",
        ),
        CheckConstraint(_OHLCV_VALIDITY_CHECK, name="ck_candle_revisions_ohlcv_valid"),
        CheckConstraint(
            "date_trunc('minute', timestamp) = timestamp",
            name="ck_candle_revisions_timestamp_minute",
        ),
        CheckConstraint("length(btrim(source)) > 0", name="ck_candle_revisions_source_nonempty"),
        CheckConstraint("data_revision > 0", name="ck_candle_revisions_positive_revision"),
    )
