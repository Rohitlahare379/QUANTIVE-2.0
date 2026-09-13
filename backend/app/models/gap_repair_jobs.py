from datetime import datetime, timezone
import enum
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import DateTime, Enum, String, Integer, ForeignKey, CheckConstraint, Index
from .base import Base


class GapRepairStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    # REST has persisted historical rows to staging.  It is intentionally not
    # claimable until the durable historical merge proves canonical raw coverage.
    AWAITING_MERGE = "awaiting_merge"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class GapRepairJob(Base):
    __tablename__ = "gap_repair_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, index=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset_registry.id", ondelete="CASCADE"), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[GapRepairStatus] = mapped_column(
        Enum(GapRepairStatus), default=GapRepairStatus.PENDING, index=True, nullable=False
    )
    worker_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    # Every successful claim receives a new opaque fencing token.  A worker id alone
    # is not sufficient because a restarted worker can reuse the same id after a
    # lease has been reclaimed by another process.
    lease_token: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    claimed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    # A durable not-before timestamp prevents a failed attempt from being claimed
    # repeatedly by a hot Dramatiq queue.
    next_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    error_message: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    error_category: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False, index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint("start_time < end_time", name="ck_gap_repair_jobs_valid_window"),
        CheckConstraint("retry_count >= 0", name="ck_gap_repair_jobs_retry_count_nonnegative"),
        CheckConstraint("max_retries >= 0", name="ck_gap_repair_jobs_max_retries_nonnegative"),
        Index(
            "ix_gap_repair_jobs_ready",
            "status",
            "next_attempt_at",
            "created_at",
            "id",
        ),
    )
