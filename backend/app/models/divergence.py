"""Immutable comparison runs and resolvable forensic divergence findings."""

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, ForeignKeyConstraint, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class StrategyComparisonRun(Base):
    """One deterministic analysis of a live activation against one reference run."""

    __tablename__ = "strategy_comparison_runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    strategy_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    strategy_version_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    strategy_version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    strategy_definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    activation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("live_strategy_activations.id"), nullable=False, index=True
    )
    reference_run_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("backtest_results.run_id"), nullable=False, index=True
    )
    reference_result_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_id: Mapped[str] = mapped_column(String(128), nullable=False)
    policy_revision: Mapped[str] = mapped_column(String(128), nullable=False)
    policy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    policy: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    reference_source: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    observed_source: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["strategy_version_id", "strategy_id"],
            ["strategy_versions.id", "strategy_versions.strategy_id"],
            name="fk_comparison_runs_strategy_version_strategy",
        ),
        CheckConstraint("char_length(run_id) = 64 AND run_id ~ '^[0-9a-f]{64}$'", name="ck_comparison_runs_id"),
        CheckConstraint("strategy_version_number > 0", name="ck_comparison_runs_positive_version"),
        CheckConstraint(
            "strategy_definition_hash ~ '^[0-9a-f]{64}$' AND reference_result_hash ~ '^[0-9a-f]{64}$' "
            "AND policy_hash ~ '^[0-9a-f]{64}$'",
            name="ck_comparison_runs_hashes",
        ),
        CheckConstraint("window_end > window_start", name="ck_comparison_runs_window"),
        CheckConstraint(
            "status IN ('healthy', 'divergent', 'insufficient_reference')",
            name="ck_comparison_runs_status",
        ),
    )


class StrategyDivergenceFinding(Base):
    """A single attributable observed difference, with a mutable resolution only."""

    __tablename__ = "strategy_divergence_findings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    comparison_run_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("strategy_comparison_runs.run_id"), nullable=False, index=True
    )
    fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    strategy_version_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    strategy_version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    strategy_definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    asset_id: Mapped[Optional[int]] = mapped_column(ForeignKey("asset_registry.id"), nullable=True, index=True)
    event_timestamp: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    divergence_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    category: Mapped[str] = mapped_column(String(128), nullable=False)
    expected_value: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    observed_value: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    source_reference: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    explanation: Mapped[str] = mapped_column(String(2000), nullable=False)
    resolution_status: Mapped[str] = mapped_column(String(32), nullable=False, default="open")
    resolution_note: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["strategy_version_id", "strategy_id"],
            ["strategy_versions.id", "strategy_versions.strategy_id"],
            name="fk_divergence_findings_strategy_version_strategy",
        ),
        UniqueConstraint("comparison_run_id", "fingerprint", name="uq_divergence_findings_run_fingerprint"),
        CheckConstraint("fingerprint ~ '^[0-9a-f]{64}$'", name="ck_divergence_findings_fingerprint"),
        CheckConstraint("strategy_version_number > 0", name="ck_divergence_findings_positive_version"),
        CheckConstraint("strategy_definition_hash ~ '^[0-9a-f]{64}$'", name="ck_divergence_findings_definition_hash"),
        CheckConstraint("window_end > window_start", name="ck_divergence_findings_window"),
        CheckConstraint(
            "severity IN ('info', 'warning', 'critical')",
            name="ck_divergence_findings_severity",
        ),
        CheckConstraint(
            "resolution_status IN ('open', 'acknowledged', 'resolved', 'not_applicable')",
            name="ck_divergence_findings_resolution",
        ),
    )
