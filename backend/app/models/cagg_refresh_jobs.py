from datetime import datetime, timezone
import enum
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import DateTime, Enum, String, Integer
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
    error_message: Mapped[str] = mapped_column(String, nullable=True)
    worker_id: Mapped[str] = mapped_column(String, nullable=True)
    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    lease_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)
