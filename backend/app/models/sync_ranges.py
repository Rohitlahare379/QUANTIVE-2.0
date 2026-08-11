from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import DateTime, ForeignKey, func, Index, CheckConstraint
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from .base import Base

class SyncRange(Base):
    __tablename__ = "sync_ranges"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset_registry.id", ondelete="CASCADE"), nullable=False)
    start_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    
    __table_args__ = (
        Index("ix_sync_ranges_asset_time", "asset_id", "start_timestamp", "end_timestamp"),
        CheckConstraint("start_timestamp <= end_timestamp", name="chk_valid_time_range"),
        ExcludeConstraint(
            ("asset_id", "="),
            (func.tstzrange(start_timestamp, end_timestamp, "[]"), "&&"),
            name="exclude_overlapping_ranges",
            using="gist"
        )
    )
