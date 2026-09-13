"""Persistence models for immutable canonical strategy definitions."""

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class Strategy(Base):
    """Mutable catalog metadata for a logical strategy identity.

    Execution-affecting fields do not live here.  They belong to an immutable
    StrategyVersion so descriptive metadata can change without changing a
    strategy's reproducible definition.
    """

    __tablename__ = "strategies"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    strategy_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )


class StrategyVersion(Base):
    """One immutable, content-addressed strategy definition."""

    __tablename__ = "strategy_versions"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    strategy_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("strategies.id"), nullable=False, index=True
    )
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_version: Mapped[str] = mapped_column(String(16), nullable=False)
    definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    canonical_definition: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint("version_number > 0", name="ck_strategy_versions_positive_version"),
        CheckConstraint("definition_hash ~ '^[0-9a-f]{64}$'", name="ck_strategy_versions_hash_format"),
        UniqueConstraint("id", "strategy_id", name="uq_strategy_versions_id_strategy"),
        UniqueConstraint("strategy_id", "version_number", name="uq_strategy_versions_strategy_version"),
        UniqueConstraint("strategy_id", "definition_hash", name="uq_strategy_versions_strategy_hash"),
    )
