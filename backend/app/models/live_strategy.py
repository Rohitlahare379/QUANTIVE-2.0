"""Durable activation, state, and forensic observations for live strategies."""

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class LiveStrategyActivation(Base):
    """One explicit live runtime for a fixed strategy version and configuration."""

    __tablename__ = "live_strategy_activations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    strategy_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    strategy_version_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    strategy_version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    strategy_definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    configuration_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", index=True)
    health: Mapped[str] = mapped_column(String(16), nullable=False, default="unknown")
    last_error: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True)
    activated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    deactivated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_evaluated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    asset_cursor: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    open_position_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
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
        ForeignKeyConstraint(
            ["strategy_version_id", "strategy_id"],
            ["strategy_versions.id", "strategy_versions.strategy_id"],
            name="fk_live_activations_strategy_version_strategy",
        ),
        CheckConstraint("strategy_version_number > 0", name="ck_live_activations_positive_version"),
        CheckConstraint(
            "strategy_definition_hash ~ '^[0-9a-f]{64}$'",
            name="ck_live_activations_definition_hash_format",
        ),
        CheckConstraint(
            "configuration_hash ~ '^[0-9a-f]{64}$'",
            name="ck_live_activations_configuration_hash_format",
        ),
        CheckConstraint(
            "status IN ('active', 'deactivated', 'error')",
            name="ck_live_activations_status",
        ),
        CheckConstraint(
            "health IN ('unknown', 'healthy', 'degraded', 'error', 'stopped')",
            name="ck_live_activations_health",
        ),
        CheckConstraint("open_position_count >= 0", name="ck_live_activations_nonnegative_positions"),
        Index(
            "uq_live_strategy_activations_active_binding",
            "strategy_version_id",
            "configuration_hash",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
    )


class LiveStrategyState(Base):
    """Recoverable per-asset cursor and position state for an activation."""

    __tablename__ = "live_strategy_states"

    activation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("live_strategy_activations.id"), primary_key=True
    )
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset_registry.id"), primary_key=True)
    last_candle_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_observed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    position_state: Mapped[str] = mapped_column(String(8), nullable=False, default="flat")
    position_changed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    health: Mapped[str] = mapped_column(String(16), nullable=False, default="unknown")
    consecutive_errors: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    __table_args__ = (
        CheckConstraint(
            "position_state IN ('flat', 'long', 'short')",
            name="ck_live_states_position_state",
        ),
        CheckConstraint(
            "health IN ('unknown', 'healthy', 'degraded', 'error', 'stopped')",
            name="ck_live_states_health",
        ),
        CheckConstraint("consecutive_errors >= 0", name="ck_live_states_nonnegative_errors"),
    )


class LiveStrategyObservation(Base):
    """One idempotent evaluation outcome for one persisted canonical candle."""

    __tablename__ = "live_strategy_observations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    activation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("live_strategy_activations.id"), nullable=False, index=True
    )
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset_registry.id"), nullable=False, index=True)
    candle_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    # Failed/corrected attempts remain immutable evidence; a repaired replay is
    # recorded as a new attempt rather than overwriting forensic history.
    evaluation_attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    signal_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    position_before: Mapped[str] = mapped_column(String(8), nullable=False)
    position_after: Mapped[str] = mapped_column(String(8), nullable=False)
    evaluation_latency_ms: Mapped[float] = mapped_column(Float, nullable=False)
    error_code: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "status IN ('evaluated', 'duplicate', 'out_of_order', 'gap_detected', 'invalid', 'error')",
            name="ck_live_observations_status",
        ),
        CheckConstraint(
            "position_before IN ('flat', 'long', 'short') AND position_after IN ('flat', 'long', 'short')",
            name="ck_live_observations_position_state",
        ),
        CheckConstraint("evaluation_latency_ms >= 0", name="ck_live_observations_nonnegative_latency"),
        CheckConstraint("evaluation_attempt >= 1", name="ck_live_observations_positive_attempt"),
        CheckConstraint(
            "(error_code IS NULL) = (error_message IS NULL)",
            name="ck_live_observations_error_pair",
        ),
        UniqueConstraint(
            "activation_id",
            "asset_id",
            "candle_timestamp",
            "evaluation_attempt",
            name="uq_live_observations_activation_asset_candle",
        ),
    )


class LiveStrategyEvent(Base):
    """Forensic signal, entry, and exit event emitted by one observation."""

    __tablename__ = "live_strategy_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    observation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("live_strategy_observations.id"), nullable=False, index=True
    )
    activation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("live_strategy_activations.id"), nullable=False, index=True
    )
    asset_id: Mapped[int] = mapped_column(ForeignKey("asset_registry.id"), nullable=False)
    candle_timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(16), nullable=False)
    signal_id: Mapped[str] = mapped_column(String(64), nullable=False)
    position_state: Mapped[str] = mapped_column(String(8), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "event_type IN ('signal', 'entry', 'exit')",
            name="ck_live_events_event_type",
        ),
        CheckConstraint(
            "position_state IN ('flat', 'long', 'short')",
            name="ck_live_events_position_state",
        ),
        UniqueConstraint(
            "activation_id",
            "asset_id",
            "candle_timestamp",
            "event_type",
            "signal_id",
            name="uq_live_events_activation_asset_candle_event",
        ),
    )
