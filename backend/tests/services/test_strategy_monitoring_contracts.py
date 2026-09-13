"""Pure-contract tests for the explicit strategy-monitoring policy.

These tests intentionally stay at the policy/classification boundary.  They do
not rely on a database, monitoring service, or worker implementation.
"""

import uuid
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.divergence.contracts import DivergenceType
from app.divergence.contracts import ComparisonPolicy
from app.monitoring.contracts import (
    CauseSeverityPolicy,
    EscalationPolicy,
    HealthCause,
    IncidentSeverity,
    MonitoringPolicy,
    MonitoringRegistrationInput,
    cause_for_divergence,
)


def _policy_payload(**overrides):
    payload = {
        "policy_id": "continuous-monitoring",
        "revision": "2026-09-13",
        "cause_severity": {
            "data_quality": "info",
            "execution": "info",
            "strategy_behavior": "info",
            "strategy_failure": "critical",
            "reference": "info",
            "runtime": "warning",
        },
        "escalation": {
            "warning_after_occurrences": 2,
            "critical_after_occurrences": 3,
        },
        "recovery_healthy_checks": 2,
        "stale_after_seconds": 300,
        "comparison_stale_after_seconds": 300,
    }
    payload.update(overrides)
    return payload


def _policy(**overrides) -> MonitoringPolicy:
    return MonitoringPolicy.model_validate(_policy_payload(**overrides))


def _comparison_policy_payload(**overrides):
    payload = {
        "policy_id": "forensic-default",
        "revision": "1",
        "signal_timing_tolerance_seconds": 0,
        "market_timestamp_tolerance_seconds": 0,
        "ohlc_tolerance_bps": "0",
        "volume_tolerance": "0",
        "execution_delay_tolerance_seconds": 0,
        "execution_price_tolerance_bps": "0",
        "fee_tolerance": "0",
        "slippage_tolerance": "0",
        "exposure_tolerance": "0",
        "return_tolerance": "0",
        "drawdown_tolerance": "0",
        "volatility_tolerance": "0",
        "trade_count_tolerance": 0,
        "win_rate_tolerance": "0",
        "benchmark_relative_tolerance": "0",
    }
    payload.update(overrides)
    return payload


def test_monitoring_policy_hash_is_deterministic_for_an_equivalent_snapshot():
    first = _policy()
    second = MonitoringPolicy.model_validate(
        {
            "stale_after_seconds": 300,
            "comparison_stale_after_seconds": 300,
            "recovery_healthy_checks": 2,
            "escalation": {
                "critical_after_occurrences": 3,
                "warning_after_occurrences": 2,
            },
            "cause_severity": {
                "runtime": "warning",
                "reference": "info",
                "strategy_failure": "critical",
                "strategy_behavior": "info",
                "execution": "info",
                "data_quality": "info",
            },
            "revision": "2026-09-13",
            "policy_id": "continuous-monitoring",
        }
    )

    assert first.policy_hash() == second.policy_hash()
    assert first.policy_hash() == first.policy_hash()
    assert first.policy_hash() != _policy(revision="2026-09-14").policy_hash()


def test_policy_rejects_invalid_escalation_order_and_zero_stale_timeout():
    with pytest.raises(ValidationError, match="critical_after_occurrences"):
        _policy(
            escalation={
                "warning_after_occurrences": 3,
                "critical_after_occurrences": 2,
            }
        )

    with pytest.raises(ValidationError):
        _policy(stale_after_seconds=0)

    with pytest.raises(ValidationError):
        _policy(comparison_stale_after_seconds=0)

    with pytest.raises(ValidationError):
        EscalationPolicy(warning_after_occurrences=0, critical_after_occurrences=1)


def test_explicit_policy_escalates_info_to_warning_to_critical():
    policy = _policy()

    assert policy.severity_for(HealthCause.DATA_QUALITY, 1) is IncidentSeverity.INFORMATIONAL
    assert policy.severity_for(HealthCause.DATA_QUALITY, 2) is IncidentSeverity.WARNING
    assert policy.severity_for(HealthCause.DATA_QUALITY, 3) is IncidentSeverity.CRITICAL


def test_market_data_and_execution_findings_never_become_strategy_failure_causes():
    market_data_cause = cause_for_divergence(DivergenceType.MARKET_DATA_MISSING_CANDLE)
    execution_cause = cause_for_divergence(DivergenceType.EXECUTION_SLIPPAGE)

    assert market_data_cause is HealthCause.DATA_QUALITY
    assert execution_cause is HealthCause.EXECUTION
    assert market_data_cause is not HealthCause.STRATEGY_FAILURE
    assert execution_cause is not HealthCause.STRATEGY_FAILURE


def test_strategy_version_mismatch_maps_to_explicit_strategy_failure_cause():
    assert (
        cause_for_divergence(DivergenceType.STRATEGY_VERSION_MISMATCH)
        is HealthCause.STRATEGY_FAILURE
    )


def test_policy_has_no_implicit_controls_and_is_frozen():
    required_fields = {
        "policy_id",
        "revision",
        "cause_severity",
        "escalation",
        "recovery_healthy_checks",
        "stale_after_seconds",
        "comparison_stale_after_seconds",
    }
    assert all(MonitoringPolicy.model_fields[name].is_required() for name in required_fields)
    assert all(CauseSeverityPolicy.model_fields[name].is_required() for name in HealthCause)
    assert all(EscalationPolicy.model_fields[name].is_required() for name in (
        "warning_after_occurrences",
        "critical_after_occurrences",
    ))

    policy = _policy()
    with pytest.raises(ValidationError, match="frozen"):
        policy.stale_after_seconds = 10
    with pytest.raises(ValidationError, match="frozen"):
        policy.cause_severity.execution = IncidentSeverity.CRITICAL


def test_registration_requires_a_uuid_activation_hash_reference_and_complete_policy():
    comparison_policy = ComparisonPolicy.model_validate(_comparison_policy_payload())
    valid = MonitoringRegistrationInput.model_validate(
        {
            "activation_id": str(uuid.uuid4()),
            "reference_run_id": "a" * 64,
            "comparison_policy_id": "forensic-default",
            "comparison_policy_revision": "1",
            "comparison_policy_hash": comparison_policy.policy_hash(),
            "comparison_policy": comparison_policy.model_dump(mode="json"),
            "policy": _policy_payload(),
        }
    )

    assert valid.reference_run_id == "a" * 64
    assert valid.policy.policy_hash() == _policy().policy_hash()

    with pytest.raises(ValidationError):
        MonitoringRegistrationInput.model_validate(
            {
                "activation_id": "not-a-uuid",
                "reference_run_id": "not-a-run-id",
                "comparison_policy_id": "",
                "comparison_policy_revision": "",
                "comparison_policy_hash": "not-a-hash",
                "comparison_policy": _comparison_policy_payload(),
                "policy": {"policy_id": "incomplete"},
            }
        )


def test_strategy_forensic_downgrade_restores_pre_column_binding_trigger_body():
    """Regression for a downgrade that once left a trigger referencing a dropped column."""
    migration = (
        Path(__file__).resolve().parents[2]
        / "alembic"
        / "versions"
        / "016_strategy_forensic_hardening.py"
    ).read_text()
    downgrade = migration.split("def downgrade() -> None:", maxsplit=1)[1]
    restore_index = downgrade.index(
        "CREATE OR REPLACE FUNCTION prevent_strategy_monitoring_binding_identity_mutation()"
    )
    drop_column_index = downgrade.index('op.drop_column("strategy_monitoring_bindings", "comparison_policy")')

    assert restore_index < drop_column_index
    restored_body = downgrade[restore_index:drop_column_index]
    assert "NEW.comparison_policy IS DISTINCT FROM OLD.comparison_policy" not in restored_body
