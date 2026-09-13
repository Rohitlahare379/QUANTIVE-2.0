"""Strict, versioned contracts for strategy-divergence monitoring.

Monitoring deliberately consumes persisted comparison evidence; it does not
reinterpret market transport or make causal claims.  Its policy is stored as a
canonical snapshot on a monitoring binding so an incident can always be read
against the exact severity/escalation rules that were active when it was
created.
"""

from __future__ import annotations

import hashlib
import json
from enum import Enum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.divergence.contracts import ComparisonPolicy, DivergenceType


MONITORING_POLICY_SCHEMA_VERSION = "1"


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MonitoringBindingStatus(str, Enum):
    """Whether a registration may be claimed by the monitor worker."""

    ACTIVE = "active"
    DEACTIVATED = "deactivated"
    ERROR = "error"


class IncidentSeverity(str, Enum):
    """Actionability level, independent from the cause of a divergence."""

    INFORMATIONAL = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class IncidentLifecycle(str, Enum):
    OPEN = "open"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


class HealthStatus(str, Enum):
    """High-level current health, with cause/severity persisted separately."""

    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    STALE = "stale"
    ERROR = "error"


class HealthCause(str, Enum):
    """Evidence class, not a statement of root cause or strategy failure."""

    DATA_QUALITY = "data_quality"
    EXECUTION = "execution"
    STRATEGY_BEHAVIOR = "strategy_behavior"
    STRATEGY_FAILURE = "strategy_failure"
    REFERENCE = "reference"
    RUNTIME = "runtime"


class EvidenceKind(str, Enum):
    DIVERGENCE_FINDING = "divergence_finding"
    STALE_STRATEGY_STATE = "stale_strategy_state"
    STALE_COMPARISON_EVIDENCE = "stale_comparison_evidence"
    RUNTIME_STATE = "runtime_state"


class CauseSeverityPolicy(_StrictModel):
    """Explicit baseline severity for every evidence class.

    Separating ``data_quality`` and ``execution`` from ``strategy_failure`` is
    intentional: a severe feed/fill discrepancy is actionable, but it is not
    automatically labelled a failed strategy implementation.
    """

    data_quality: IncidentSeverity
    execution: IncidentSeverity
    strategy_behavior: IncidentSeverity
    strategy_failure: IncidentSeverity
    reference: IncidentSeverity
    runtime: IncidentSeverity

    def severity_for(self, cause: HealthCause) -> IncidentSeverity:
        return getattr(self, cause.value)


class EscalationPolicy(_StrictModel):
    """Occurrence counts at which a still-open incident becomes more urgent."""

    warning_after_occurrences: int = Field(ge=1)
    critical_after_occurrences: int = Field(ge=1)

    @model_validator(mode="after")
    def validate_order(self) -> "EscalationPolicy":
        if self.critical_after_occurrences < self.warning_after_occurrences:
            raise ValueError("critical_after_occurrences must be >= warning_after_occurrences")
        return self


class MonitoringPolicy(_StrictModel):
    """Frozen, explicit incident policy used by one monitoring binding.

    There are no hidden alert thresholds.  The baseline severity of every
    cause, escalation occurrence counts, stale-runtime timeout, and automatic
    recovery requirement must all be supplied by the caller and are hashed
    into the binding snapshot.
    """

    schema_version: Literal[MONITORING_POLICY_SCHEMA_VERSION] = MONITORING_POLICY_SCHEMA_VERSION
    policy_id: str = Field(min_length=1, max_length=128)
    revision: str = Field(min_length=1, max_length=128)
    cause_severity: CauseSeverityPolicy
    escalation: EscalationPolicy
    recovery_healthy_checks: int = Field(ge=1)
    stale_after_seconds: int = Field(ge=1)
    comparison_stale_after_seconds: int = Field(ge=1)

    @field_validator("policy_id", "revision")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("policy text cannot be blank")
        return normalized

    def policy_hash(self) -> str:
        """A stable SHA-256 identity over the complete policy snapshot."""
        payload = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def severity_for(self, cause: HealthCause, occurrence_count: int) -> IncidentSeverity:
        """Return the policy-derived severity for an observed occurrence count."""
        if occurrence_count < 1:
            raise ValueError("occurrence_count must be positive")
        baseline = self.cause_severity.severity_for(cause)
        rank = {
            IncidentSeverity.INFORMATIONAL: 0,
            IncidentSeverity.WARNING: 1,
            IncidentSeverity.CRITICAL: 2,
        }
        escalated = (
            IncidentSeverity.CRITICAL
            if occurrence_count >= self.escalation.critical_after_occurrences
            else IncidentSeverity.WARNING
            if occurrence_count >= self.escalation.warning_after_occurrences
            else IncidentSeverity.INFORMATIONAL
        )
        return baseline if rank[baseline] >= rank[escalated] else escalated


class MonitoringRegistrationInput(_StrictModel):
    """Explicitly binds one live runtime to one immutable reference run."""

    activation_id: UUID
    reference_run_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    comparison_policy_id: str = Field(min_length=1, max_length=128)
    comparison_policy_revision: str = Field(min_length=1, max_length=128)
    comparison_policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    # The monitor must retain the complete frozen comparator policy, not only
    # its labels/hash, so it can produce bounded evidence without guessing
    # thresholds during later maintenance cycles.
    comparison_policy: ComparisonPolicy
    policy: MonitoringPolicy

    @field_validator("comparison_policy_id", "comparison_policy_revision")
    @classmethod
    def normalize_comparison_policy_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("comparison policy text cannot be blank")
        return normalized

    @model_validator(mode="after")
    def validate_comparison_policy_snapshot(self) -> "MonitoringRegistrationInput":
        if (
            self.comparison_policy.policy_id != self.comparison_policy_id
            or self.comparison_policy.revision != self.comparison_policy_revision
            or self.comparison_policy.policy_hash() != self.comparison_policy_hash
        ):
            raise ValueError("comparison policy identity/hash must match its frozen snapshot")
        return self


_DIVERGENCE_CAUSES: dict[DivergenceType, HealthCause] = {
    DivergenceType.INSUFFICIENT_REFERENCE_DATA: HealthCause.REFERENCE,
    DivergenceType.STRATEGY_VERSION_MISMATCH: HealthCause.STRATEGY_FAILURE,
    DivergenceType.CONFIGURATION_MISMATCH: HealthCause.STRATEGY_FAILURE,
    DivergenceType.EVALUATOR_FAILURE: HealthCause.RUNTIME,
    DivergenceType.MISSING_SIGNAL: HealthCause.STRATEGY_BEHAVIOR,
    DivergenceType.UNEXPECTED_SIGNAL: HealthCause.STRATEGY_BEHAVIOR,
    DivergenceType.SIGNAL_TIMING_DIFFERENCE: HealthCause.STRATEGY_BEHAVIOR,
    DivergenceType.MARKET_DATA_MISSING_CANDLE: HealthCause.DATA_QUALITY,
    DivergenceType.MARKET_DATA_CORRECTED_CANDLE: HealthCause.DATA_QUALITY,
    DivergenceType.MARKET_DATA_TIMESTAMP_MISMATCH: HealthCause.DATA_QUALITY,
    DivergenceType.MARKET_DATA_OHLC_DISCREPANCY: HealthCause.DATA_QUALITY,
    DivergenceType.MARKET_DATA_VOLUME_DISCREPANCY: HealthCause.DATA_QUALITY,
    DivergenceType.MISSING_EXPECTED_ENTRY: HealthCause.EXECUTION,
    DivergenceType.MISSING_EXPECTED_EXIT: HealthCause.EXECUTION,
    DivergenceType.UNEXPECTED_ENTRY: HealthCause.EXECUTION,
    DivergenceType.UNEXPECTED_EXIT: HealthCause.EXECUTION,
    DivergenceType.EXECUTION_DELAY: HealthCause.EXECUTION,
    DivergenceType.EXECUTION_SLIPPAGE: HealthCause.EXECUTION,
    DivergenceType.FEE_DIFFERENCE: HealthCause.EXECUTION,
    DivergenceType.POSITION_MISMATCH: HealthCause.STRATEGY_BEHAVIOR,
    DivergenceType.EXPOSURE_MISMATCH: HealthCause.STRATEGY_BEHAVIOR,
    DivergenceType.UNEXPECTED_POSITION_PERSISTENCE: HealthCause.STRATEGY_BEHAVIOR,
    DivergenceType.RETURN_DIFFERENCE: HealthCause.STRATEGY_BEHAVIOR,
    DivergenceType.DRAWDOWN_DIFFERENCE: HealthCause.STRATEGY_BEHAVIOR,
    DivergenceType.VOLATILITY_DIFFERENCE: HealthCause.STRATEGY_BEHAVIOR,
    DivergenceType.TRADE_FREQUENCY_DIFFERENCE: HealthCause.STRATEGY_BEHAVIOR,
    DivergenceType.WIN_RATE_DIFFERENCE: HealthCause.STRATEGY_BEHAVIOR,
    DivergenceType.BENCHMARK_RELATIVE_DIFFERENCE: HealthCause.STRATEGY_BEHAVIOR,
}


def cause_for_divergence(divergence_type: DivergenceType | str) -> HealthCause:
    """Classify every supported comparator output without causal overreach.

    New comparison types must be explicitly mapped here; silently treating a
    future type as a strategy failure would make monitoring misleading.
    """
    normalized = (
        divergence_type
        if isinstance(divergence_type, DivergenceType)
        else DivergenceType(divergence_type)
    )
    try:
        return _DIVERGENCE_CAUSES[normalized]
    except KeyError as exc:  # defensive if the comparison enum grows
        raise ValueError(f"no monitoring cause configured for {normalized.value}") from exc
