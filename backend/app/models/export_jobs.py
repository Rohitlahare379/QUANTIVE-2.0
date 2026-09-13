import enum
import uuid
from datetime import datetime, timezone
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import CheckConstraint, DateTime, Enum, Index, String, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID

from .base import Base

class ExportStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class ExportJob(Base):
    __tablename__ = "export_jobs"
    
    # Use UUID for jobs to prevent enumeration
    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset_registry.id"), nullable=False)
    timeframe: Mapped[str] = mapped_column(String, nullable=False)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    
    status: Mapped[ExportStatus] = mapped_column(Enum(ExportStatus), default=ExportStatus.PENDING, index=True, nullable=False)
    s3_key: Mapped[str] = mapped_column(String, nullable=True)
    error_message: Mapped[str] = mapped_column(String, nullable=True)
    error_category: Mapped[str] = mapped_column(String(64), nullable=True)

    # Exporting is a long-running external side effect.  A status alone is not
    # an ownership boundary: a crashed or paused worker must not be able to
    # finalize an export after a later worker has reclaimed it.  Each claim
    # therefore carries an opaque fencing token and a finite lease.
    worker_id: Mapped[str] = mapped_column(String(128), nullable=True)
    lease_token: Mapped[str] = mapped_column(String(64), nullable=True)
    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    # Count claims rather than only recorded exceptions, so a process that
    # repeatedly crashes after claiming a job cannot leave it retrying forever.
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_attempts: Mapped[int] = mapped_column(Integer, default=5, nullable=False)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

    __table_args__ = (
        CheckConstraint("attempt_count >= 0", name="ck_export_jobs_attempt_count_nonnegative"),
        CheckConstraint("max_attempts > 0", name="ck_export_jobs_max_attempts_positive"),
        CheckConstraint(
            "status != 'PROCESSING'::exportstatus OR "
            "(worker_id IS NOT NULL AND lease_token IS NOT NULL AND claimed_at IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_export_jobs_processing_claim_complete",
        ),
        Index(
            "ix_export_jobs_ready",
            "status",
            "next_attempt_at",
            "created_at",
            "id",
        ),
    )
