"""Durable, fenced work records for staged historical-candle promotion."""

import enum
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import CheckConstraint, DateTime, Enum, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class HistoricalMergeStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class HistoricalMergeJob(Base):
    """One idempotent staged-data promotion job per UTC day bucket.

    The unique day bucket coalesces all staging writes for that day.  A fresh
    staging write can safely reopen a completed job; stale workers are fenced by
    ``lease_token`` on every state transition.
    """

    __tablename__ = "historical_merge_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    day_bucket: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, unique=True)
    status: Mapped[HistoricalMergeStatus] = mapped_column(
        Enum(HistoricalMergeStatus), default=HistoricalMergeStatus.PENDING, nullable=False, index=True
    )
    worker_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    lease_token: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    claimed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_retries: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    next_attempt_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    error_category: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint("retry_count >= 0", name="ck_historical_merge_jobs_retry_count_nonnegative"),
        CheckConstraint("max_retries >= 0", name="ck_historical_merge_jobs_max_retries_nonnegative"),
        Index(
            "ix_historical_merge_jobs_ready",
            "status",
            "next_attempt_at",
            "created_at",
            "id",
        ),
    )
