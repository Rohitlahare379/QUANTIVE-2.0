from datetime import datetime, timezone
import enum
from typing import Optional
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import DateTime, Enum, String, Integer, CheckConstraint, Index
from .base import Base

class RefreshStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class CaggRefreshJob(Base):
    __tablename__ = "cagg_refresh_jobs"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[RefreshStatus] = mapped_column(Enum(RefreshStatus), default=RefreshStatus.PENDING, index=True, nullable=False)
    error_message: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    worker_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    # A per-claim fencing token protects state transitions from an old worker that
    # resumes after its lease was reclaimed.
    lease_token: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    claimed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    next_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_category: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    __table_args__ = (
        CheckConstraint("window_start < window_end", name="ck_cagg_refresh_jobs_valid_window"),
        CheckConstraint("retry_count >= 0", name="ck_cagg_refresh_jobs_retry_count_nonnegative"),
        CheckConstraint("max_retries >= 0", name="ck_cagg_refresh_jobs_max_retries_nonnegative"),
        Index(
            "ix_cagg_refresh_jobs_ready",
            "status",
            "next_attempt_at",
            "created_at",
            "id",
        ),
    )
