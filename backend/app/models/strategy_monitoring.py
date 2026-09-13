"""Durable policy bindings, health, and deduplicated strategy incidents."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import (
    CheckConstraint,
    DateTime,
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


class StrategyMonitoringBinding(Base):
    """One explicit, frozen monitoring policy for a live strategy activation.

    A binding does not manufacture comparison runs.  It scopes the monitor to
    an active runtime and its explicit reference backtest, then provides a
    policy snapshot for aggregating immutable divergence evidence.
    """

    __tablename__ = "strategy_monitoring_bindings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    activation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("live_strategy_activations.id"), nullable=False, index=True
    )
    reference_run_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("backtest_results.run_id"), nullable=False, index=True
    )
    comparison_policy_id: Mapped[str] = mapped_column(String(128), nullable=False)
    comparison_policy_revision: Mapped[str] = mapped_column(String(128), nullable=False)
    comparison_policy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    # Bindings written before comparison production existed cannot truthfully
    # reconstruct a frozen comparator policy.  Migration 016 disables those
    # legacy bindings; only an active binding is required to carry a snapshot.
    comparison_policy: Mapped[Optional[dict[str, Any]]] = mapped_column(JSONB, nullable=True)
    policy_id: Mapped[str] = mapped_column(String(128), nullable=False)
    policy_revision: Mapped[str] = mapped_column(String(128), nullable=False)
    policy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    policy: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active", index=True)
    registered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    deactivated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    # The durable claim token makes worker ownership verifiable on every
    # mutation; a process restart simply reclaims an expired lease.
    lease_worker_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    lease_token: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    claimed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    claim_attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
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
        CheckConstraint(
            "policy_hash ~ '^[0-9a-f]{64}$' AND comparison_policy_hash ~ '^[0-9a-f]{64}$'",
            name="ck_monitoring_bindings_policy_hashes",
        ),
        CheckConstraint(
            "status IN ('active', 'deactivated', 'error')",
            name="ck_monitoring_bindings_status",
        ),
        CheckConstraint(
            "status <> 'active' OR comparison_policy IS NOT NULL",
            name="ck_monitoring_bindings_active_comparison_policy",
        ),
        CheckConstraint(
            "claim_attempt_count >= 0",
            name="ck_monitoring_bindings_nonnegative_claims",
        ),
        CheckConstraint(
            "(lease_worker_id IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL) "
            "OR (lease_worker_id IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_monitoring_bindings_complete_lease",
        ),
        CheckConstraint(
            "deactivated_at IS NULL OR deactivated_at >= registered_at",
            name="ck_monitoring_bindings_valid_deactivation",
        ),
        Index(
            "ix_monitoring_bindings_claimable",
            "status",
            "lease_expires_at",
            "last_processed_at",
        ),
        Index(
            "uq_monitoring_bindings_active_activation",
            "activation_id",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
    )


class StrategyHealthState(Base):
    """Current durable health projection for one monitored live activation."""

    __tablename__ = "strategy_health_states"

    activation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("live_strategy_activations.id"), primary_key=True
    )
    binding_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("strategy_monitoring_bindings.id"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="unknown")
    cause: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    severity: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    last_comparison_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_complete_comparison_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_complete_window_end: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_healthy_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_evaluated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    consecutive_healthy_checks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True)
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
        CheckConstraint(
            "status IN ('unknown', 'healthy', 'degraded', 'stale', 'error')",
            name="ck_strategy_health_states_status",
        ),
        CheckConstraint(
            "cause IS NULL OR cause IN "
            "('data_quality', 'execution', 'strategy_behavior', 'strategy_failure', 'reference', 'runtime')",
            name="ck_strategy_health_states_cause",
        ),
        CheckConstraint(
            "severity IS NULL OR severity IN ('info', 'warning', 'critical')",
            name="ck_strategy_health_states_severity",
        ),
        CheckConstraint(
            "consecutive_healthy_checks >= 0",
            name="ck_strategy_health_states_nonnegative_recovery",
        ),
        CheckConstraint(
            "(status IN ('unknown', 'healthy') AND cause IS NULL AND severity IS NULL) "
            "OR (status NOT IN ('unknown', 'healthy') AND cause IS NOT NULL AND severity IS NOT NULL)",
            name="ck_strategy_health_states_status_evidence",
        ),
        UniqueConstraint("binding_id", name="uq_strategy_health_states_binding"),
    )


class StrategyIncident(Base):
    """A mutable lifecycle envelope around immutable divergence evidence."""

    __tablename__ = "strategy_incidents"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    dedup_key: Mapped[str] = mapped_column(String(64), nullable=False)
    binding_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("strategy_monitoring_bindings.id"), nullable=False, index=True
    )
    activation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("live_strategy_activations.id"), nullable=False, index=True
    )
    reference_run_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("backtest_results.run_id"), nullable=False, index=True
    )
    monitoring_policy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    strategy_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("strategies.id"), nullable=False, index=True)
    strategy_version_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, index=True)
    strategy_version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    strategy_definition_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    asset_id: Mapped[Optional[int]] = mapped_column(ForeignKey("asset_registry.id"), nullable=True, index=True)
    divergence_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    category: Mapped[str] = mapped_column(String(128), nullable=False)
    cause: Mapped[str] = mapped_column(String(32), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open", index=True)
    first_detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    occurrence_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    acknowledged_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    acknowledgement_note: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True)
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_by: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    resolution_note: Mapped[Optional[str]] = mapped_column(String(2000), nullable=True)
    latest_comparison_run_id: Mapped[Optional[str]] = mapped_column(
        String(64), ForeignKey("strategy_comparison_runs.run_id"), nullable=True
    )
    latest_finding_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("strategy_divergence_findings.id"), nullable=True
    )
    source_reference: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    evidence_summary: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
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
            name="fk_strategy_incidents_strategy_version_strategy",
        ),
        UniqueConstraint("dedup_key", name="uq_strategy_incidents_dedup_key"),
        CheckConstraint("dedup_key ~ '^[0-9a-f]{64}$'", name="ck_strategy_incidents_dedup_key"),
        CheckConstraint(
            "monitoring_policy_hash ~ '^[0-9a-f]{64}$'",
            name="ck_strategy_incidents_policy_hash",
        ),
        CheckConstraint("strategy_version_number > 0", name="ck_strategy_incidents_positive_version"),
        CheckConstraint(
            "strategy_definition_hash ~ '^[0-9a-f]{64}$'",
            name="ck_strategy_incidents_definition_hash",
        ),
        CheckConstraint(
            "cause IN ('data_quality', 'execution', 'strategy_behavior', 'strategy_failure', 'reference', 'runtime')",
            name="ck_strategy_incidents_cause",
        ),
        CheckConstraint(
            "severity IN ('info', 'warning', 'critical')",
            name="ck_strategy_incidents_severity",
        ),
        CheckConstraint(
            "status IN ('open', 'acknowledged', 'resolved')",
            name="ck_strategy_incidents_status",
        ),
        CheckConstraint("occurrence_count > 0", name="ck_strategy_incidents_positive_occurrences"),
        CheckConstraint(
            "last_detected_at >= first_detected_at",
            name="ck_strategy_incidents_valid_detection_window",
        ),
        CheckConstraint(
            "acknowledged_at IS NULL OR acknowledged_by IS NOT NULL",
            name="ck_strategy_incidents_ack_actor",
        ),
        CheckConstraint(
            "status <> 'acknowledged' OR acknowledged_at IS NOT NULL",
            name="ck_strategy_incidents_ack_state",
        ),
        CheckConstraint(
            "status <> 'resolved' OR resolved_at IS NOT NULL",
            name="ck_strategy_incidents_resolution_state",
        ),
        Index("ix_strategy_incidents_activation_status", "activation_id", "status"),
        Index("ix_strategy_incidents_strategy_status", "strategy_id", "status"),
    )


class StrategyIncidentEvidence(Base):
    """Immutable evidence links, including stable synthetic stale-state proof."""

    __tablename__ = "strategy_incident_evidence"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    incident_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("strategy_incidents.id"), nullable=False, index=True
    )
    finding_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("strategy_divergence_findings.id"), nullable=True
    )
    comparison_run_id: Mapped[Optional[str]] = mapped_column(
        String(64), ForeignKey("strategy_comparison_runs.run_id"), nullable=True, index=True
    )
    evidence_key: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_reference: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint("evidence_key ~ '^[0-9a-f]{64}$'", name="ck_strategy_incident_evidence_key"),
        CheckConstraint(
            "evidence_kind IN ('divergence_finding', 'stale_strategy_state', 'stale_comparison_evidence', 'runtime_state')",
            name="ck_strategy_incident_evidence_kind",
        ),
        CheckConstraint(
            "(evidence_kind = 'divergence_finding' AND finding_id IS NOT NULL AND comparison_run_id IS NOT NULL) "
            "OR (evidence_kind IN ('stale_strategy_state', 'stale_comparison_evidence', 'runtime_state') AND finding_id IS NULL AND comparison_run_id IS NULL)",
            name="ck_strategy_incident_evidence_source_shape",
        ),
        UniqueConstraint("incident_id", "evidence_key", name="uq_strategy_incident_evidence_key"),
        UniqueConstraint("finding_id", name="uq_strategy_incident_evidence_finding"),
    )


class StrategyMonitoringAssessment(Base):
    """Immutable checkpoint making comparison-run aggregation restart-safe."""

    __tablename__ = "strategy_monitoring_assessments"

    comparison_run_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("strategy_comparison_runs.run_id"), primary_key=True
    )
    binding_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("strategy_monitoring_bindings.id"), nullable=False, index=True
    )
    activation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("live_strategy_activations.id"), nullable=False, index=True
    )
    reference_run_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("backtest_results.run_id"), nullable=False
    )
    comparison_status: Mapped[str] = mapped_column(String(32), nullable=False)
    finding_count: Mapped[int] = mapped_column(Integer, nullable=False)
    processor_worker_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "comparison_run_id ~ '^[0-9a-f]{64}$'",
            name="ck_monitoring_assessments_run_id",
        ),
        CheckConstraint(
            "comparison_status IN ('healthy', 'divergent', 'insufficient_reference')",
            name="ck_monitoring_assessments_status",
        ),
        CheckConstraint("finding_count >= 0", name="ck_monitoring_assessments_nonnegative_findings"),
    )
