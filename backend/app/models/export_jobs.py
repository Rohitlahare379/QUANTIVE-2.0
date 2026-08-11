import enum
import uuid
from datetime import datetime, timezone
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import DateTime, Enum, String, Integer, ForeignKey
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
    
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)
