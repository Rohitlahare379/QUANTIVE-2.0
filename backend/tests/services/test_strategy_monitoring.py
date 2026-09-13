"""Unit tests for durable strategy-monitoring lifecycle semantics.

These deliberately exercise the monitoring service at its persistence boundary
with transient SQLAlchemy objects and mocked sessions.  They do not need a
PostgreSQL instance, but make the incident-idempotency and recovery behaviour
explicit before the PostgreSQL integration suite runs.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.divergence.contracts import ComparisonPolicy, DivergenceType
from app.models.divergence import StrategyComparisonRun, StrategyDivergenceFinding
from app.models.live_strategy import LiveStrategyActivation
from app.models.strategy_monitoring import (
    StrategyHealthState,
    StrategyIncident,
    StrategyIncidentEvidence,
    StrategyMonitoringBinding,
)
from app.monitoring.contracts import (
    HealthCause,
    HealthStatus,
    IncidentLifecycle,
    IncidentSeverity,
    MonitoringPolicy,
    MonitoringRegistrationInput,
)
from app.services.exceptions import StrategyMonitoringRegistrationError
from app.services.strategy_monitoring import (
    _BindingClaim,
    StrategyComparisonProducerService,
    StrategyMonitoringService,
    incident_dedup_key,
)


UTC = timezone.utc
START = datetime(2026, 1, 1, tzinfo=UTC)
NOW = START + timedelta(hours=1)
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64
HASH_D = "d" * 64


def _policy(*, recovery_checks: int = 2, stale_after_seconds: int = 300) -> MonitoringPolicy:
    return MonitoringPolicy.model_validate(
        {
            "policy_id": "continuous-monitoring",
            "revision": "2026-09-13",
            "cause_severity": {
                "data_quality": "info",
                "execution": "warning",
                "strategy_behavior": "warning",
                "strategy_failure": "critical",
                "reference": "info",
                "runtime": "warning",
            },
            "escalation": {
                "warning_after_occurrences": 2,
                "critical_after_occurrences": 3,
            },
            "recovery_healthy_checks": recovery_checks,
            "stale_after_seconds": stale_after_seconds,
            "comparison_stale_after_seconds": stale_after_seconds,
        }
    )


def _comparison_policy() -> ComparisonPolicy:
    return ComparisonPolicy.model_validate(
        {
            "policy_id": "comparison-policy",
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
    )


def _transactional_db() -> AsyncMock:
    db = AsyncMock()
    transaction = AsyncMock()
    transaction.__aenter__.return_value = None
    transaction.__aexit__.return_value = None
    db.begin = MagicMock(return_value=transaction)
    db.add = MagicMock()
    db.flush = AsyncMock()
    return db


def _scalar_first_result(value):
    result = MagicMock()
    result.scalars.return_value.first.return_value = value
    return result


def _scalar_all_result(values):
    result = MagicMock()
    result.scalars.return_value.all.return_value = values
    return result


def _activation() -> LiveStrategyActivation:
    return LiveStrategyActivation(
        id=uuid.uuid4(),
        strategy_id=uuid.uuid4(),
        strategy_version_id=uuid.uuid4(),
        strategy_version_number=1,
        strategy_definition_hash=HASH_A,
        configuration={"bar_source": "canonical"},
        configuration_hash=HASH_B,
        status="active",
        health="healthy",
        activated_at=START,
        open_position_count=0,
    )


def _binding(activation: LiveStrategyActivation, policy: MonitoringPolicy | None = None) -> StrategyMonitoringBinding:
    policy = policy or _policy()
    comparison_policy = _comparison_policy()
    return StrategyMonitoringBinding(
        id=uuid.uuid4(),
        activation_id=activation.id,
        reference_run_id=HASH_C,
        comparison_policy_id="comparison-policy",
        comparison_policy_revision="1",
        comparison_policy_hash=comparison_policy.policy_hash(),
        comparison_policy=comparison_policy.model_dump(mode="json"),
        policy_id=policy.policy_id,
        policy_revision=policy.revision,
        policy_hash=policy.policy_hash(),
        policy=policy.model_dump(mode="json"),
        status="active",
        registered_at=START,
        claim_attempt_count=0,
    )


def _comparison_run(binding: StrategyMonitoringBinding, activation: LiveStrategyActivation) -> StrategyComparisonRun:
    return StrategyComparisonRun(
        run_id=HASH_D,
        strategy_id=activation.strategy_id,
        strategy_version_id=activation.strategy_version_id,
        strategy_version_number=activation.strategy_version_number,
        strategy_definition_hash=activation.strategy_definition_hash,
        activation_id=activation.id,
        reference_run_id=binding.reference_run_id,
        reference_result_hash=HASH_B,
        policy_id="comparison-policy",
        policy_revision="1",
        policy_hash=HASH_A,
        policy={},
        reference_source={"run_id": binding.reference_run_id},
        observed_source={"activation_id": str(activation.id)},
        window_start=START,
        window_end=NOW,
        status="divergent",
        created_at=NOW,
    )


def _finding(
    activation: LiveStrategyActivation,
    comparison_run: StrategyComparisonRun,
    *,
    divergence_type: str = DivergenceType.MARKET_DATA_OHLC_DISCREPANCY.value,
    created_at: datetime = NOW,
    expected: dict | None = None,
    observed: dict | None = None,
    finding_id: uuid.UUID | None = None,
) -> StrategyDivergenceFinding:
    return StrategyDivergenceFinding(
        id=finding_id or uuid.uuid4(),
        comparison_run_id=comparison_run.run_id,
        fingerprint=HASH_A,
        strategy_id=activation.strategy_id,
        strategy_version_id=activation.strategy_version_id,
        strategy_version_number=activation.strategy_version_number,
        strategy_definition_hash=activation.strategy_definition_hash,
        asset_id=1,
        event_timestamp=created_at,
        window_start=START,
        window_end=NOW,
        divergence_type=divergence_type,
        severity="warning",
        category="market_data",
        expected_value=expected or {"asset_id": 1, "close": "100", "timestamp": START.isoformat()},
        observed_value=observed or {"asset_id": 1, "close": "99", "timestamp": START.isoformat()},
        source_reference={"source": "comparison"},
        explanation="Observed canonical candle differs from reference candle.",
        resolution_status="open",
        created_at=created_at,
    )


def _health(binding: StrategyMonitoringBinding, *, status: str = "unknown") -> StrategyHealthState:
    values: dict[str, object] = {
        "activation_id": binding.activation_id,
        "binding_id": binding.id,
        "status": status,
        "consecutive_healthy_checks": 0,
    }
    if status == "degraded":
        values.update(cause=HealthCause.DATA_QUALITY.value, severity=IncidentSeverity.WARNING.value)
    return StrategyHealthState(**values)


def _incident(
    binding: StrategyMonitoringBinding,
    activation: LiveStrategyActivation,
    *,
    cause: HealthCause = HealthCause.DATA_QUALITY,
    severity: IncidentSeverity = IncidentSeverity.WARNING,
    status: str = "open",
) -> StrategyIncident:
    resolved_at = NOW if status == IncidentLifecycle.RESOLVED.value else None
    return StrategyIncident(
        id=uuid.uuid4(),
        dedup_key=HASH_D,
        binding_id=binding.id,
        activation_id=activation.id,
        reference_run_id=binding.reference_run_id,
        monitoring_policy_hash=binding.policy_hash,
        strategy_id=activation.strategy_id,
        strategy_version_id=activation.strategy_version_id,
        strategy_version_number=activation.strategy_version_number,
        strategy_definition_hash=activation.strategy_definition_hash,
        asset_id=1,
        divergence_type=DivergenceType.MARKET_DATA_OHLC_DISCREPANCY.value,
        category="market_data",
        cause=cause.value,
        severity=severity.value,
        status=status,
        first_detected_at=START,
        last_detected_at=NOW,
        occurrence_count=1,
        resolved_at=resolved_at,
        resolved_by="operator" if resolved_at else None,
        source_reference={},
        evidence_summary={},
    )


def test_incident_key_groups_the_same_problem_across_windows_but_not_different_identity():
    activation = _activation()
    binding = _binding(activation)
    run = _comparison_run(binding, activation)
    first = _finding(activation, run)
    later = _finding(
        activation,
        run,
        created_at=NOW + timedelta(minutes=10),
        expected={"asset_id": 1, "close": "110", "timestamp": (NOW + timedelta(minutes=10)).isoformat()},
        observed={"asset_id": 1, "close": "105", "timestamp": (NOW + timedelta(minutes=10)).isoformat()},
    )
    different_signal = _finding(
        activation,
        run,
        divergence_type=DivergenceType.MISSING_SIGNAL.value,
        expected={"asset_id": 1, "signal_id": "enter-long"},
        observed={},
    )

    assert incident_dedup_key(binding=binding, finding=first, cause=HealthCause.DATA_QUALITY) == incident_dedup_key(
        binding=binding, finding=later, cause=HealthCause.DATA_QUALITY
    )
    assert incident_dedup_key(binding=binding, finding=first, cause=HealthCause.DATA_QUALITY) != incident_dedup_key(
        binding=binding, finding=different_signal, cause=HealthCause.STRATEGY_BEHAVIOR
    )


@pytest.mark.asyncio
async def test_repeated_finding_is_evidence_idempotent_and_repeated_problem_escalates_one_incident():
    db = _transactional_db()
    service = StrategyMonitoringService(db, now_fn=lambda: NOW)
    activation = _activation()
    binding = _binding(activation)
    run = _comparison_run(binding, activation)
    policy = _policy()
    first = _finding(activation, run)

    db.execute.side_effect = [_scalar_first_result(None), _scalar_first_result(None)]
    first_cause, first_severity = await service._record_finding(binding, run, first, policy, NOW)
    added = [call.args[0] for call in db.add.call_args_list]
    incident = next(item for item in added if isinstance(item, StrategyIncident))
    evidence = next(item for item in added if isinstance(item, StrategyIncidentEvidence))

    assert first_cause is HealthCause.DATA_QUALITY
    assert first_severity is IncidentSeverity.WARNING
    assert incident.occurrence_count == 1

    # A delivery retry of the exact immutable finding links no second evidence
    # and cannot bump incident counters.
    db.execute.reset_mock()
    db.add.reset_mock()
    db.execute.side_effect = [_scalar_first_result(evidence), _scalar_first_result(incident)]
    _, retry_severity = await service._record_finding(binding, run, first, policy, NOW)
    assert retry_severity is IncidentSeverity.WARNING
    assert incident.occurrence_count == 1
    db.add.assert_not_called()

    # A later finding with the same stable identity has a separate forensic
    # evidence link but updates the one logical incident and follows policy
    # escalation, rather than producing alert spam.
    later = _finding(
        activation,
        run,
        created_at=NOW + timedelta(minutes=1),
        expected={"asset_id": 1, "close": "101"},
        observed={"asset_id": 1, "close": "98"},
    )
    db.execute.reset_mock()
    db.add.reset_mock()
    db.execute.side_effect = [_scalar_first_result(None), _scalar_first_result(incident)]
    _, later_severity = await service._record_finding(binding, run, later, policy, NOW + timedelta(minutes=1))

    assert later_severity is IncidentSeverity.WARNING
    assert incident.occurrence_count == 2
    assert incident.latest_finding_id == later.id
    assert len(db.add.call_args_list) == 1
    assert isinstance(db.add.call_args.args[0], StrategyIncidentEvidence)

    third = _finding(
        activation,
        run,
        created_at=NOW + timedelta(minutes=2),
        expected={"asset_id": 1, "close": "102"},
        observed={"asset_id": 1, "close": "97"},
    )
    db.execute.reset_mock()
    db.add.reset_mock()
    db.execute.side_effect = [_scalar_first_result(None), _scalar_first_result(incident)]
    _, critical_severity = await service._record_finding(binding, run, third, policy, NOW + timedelta(minutes=2))

    assert critical_severity is IncidentSeverity.CRITICAL
    assert incident.occurrence_count == 3
    assert incident.severity == IncidentSeverity.CRITICAL.value


@pytest.mark.parametrize(
    ("divergence_type", "expected_cause"),
    [
        (DivergenceType.MARKET_DATA_MISSING_CANDLE, HealthCause.DATA_QUALITY),
        (DivergenceType.EXECUTION_SLIPPAGE, HealthCause.EXECUTION),
    ],
)
def test_data_and_execution_divergences_do_not_project_as_strategy_failure(divergence_type, expected_cause):
    service = StrategyMonitoringService(_transactional_db(), now_fn=lambda: NOW)
    activation = _activation()
    state = _health(_binding(activation))

    service._set_divergent_health(state, [(expected_cause, IncidentSeverity.CRITICAL)])

    assert state.status == HealthStatus.DEGRADED.value
    assert state.cause == expected_cause.value
    assert state.cause != HealthCause.STRATEGY_FAILURE.value


@pytest.mark.asyncio
async def test_acknowledgement_and_explicit_resolution_preserve_incident_lifecycle():
    db = _transactional_db()
    service = StrategyMonitoringService(db, now_fn=lambda: NOW)
    activation = _activation()
    binding = _binding(activation)
    incident = _incident(binding, activation)
    db.execute.return_value = _scalar_first_result(incident)

    acknowledged = await service.acknowledge_incident(incident.id, actor=" on-call ", note="Investigating")
    assert acknowledged.status == IncidentLifecycle.ACKNOWLEDGED.value
    assert acknowledged.acknowledged_at == NOW
    assert acknowledged.acknowledged_by == "on-call"
    assert acknowledged.acknowledgement_note == "Investigating"

    # Re-acknowledgement does not overwrite the original ownership evidence.
    await service.acknowledge_incident(incident.id, actor="different-operator", note="ignored")
    assert incident.acknowledged_by == "on-call"
    assert incident.acknowledgement_note == "Investigating"

    resolved = await service.resolve_incident(incident.id, actor=" on-call ", note="Feed corrected")
    assert resolved.status == IncidentLifecycle.RESOLVED.value
    assert resolved.resolved_at == NOW
    assert resolved.resolved_by == "on-call"
    assert resolved.resolution_note == "Feed corrected"


@pytest.mark.asyncio
async def test_complete_healthy_comparisons_require_configured_recovery_before_auto_resolution():
    db = _transactional_db()
    service = StrategyMonitoringService(db, now_fn=lambda: NOW)
    activation = _activation()
    policy = _policy(recovery_checks=2)
    binding = _binding(activation, policy)
    state = _health(binding, status=HealthStatus.DEGRADED.value)
    incident = _incident(binding, activation)

    db.execute.side_effect = [_scalar_all_result([incident]), _scalar_all_result([incident])]
    await service._record_healthy_comparison(binding, state, policy, NOW)

    assert state.consecutive_healthy_checks == 1
    assert state.status == HealthStatus.DEGRADED.value
    assert incident.status == IncidentLifecycle.OPEN.value

    await service._record_healthy_comparison(binding, state, policy, NOW + timedelta(minutes=1))

    assert state.consecutive_healthy_checks == 2
    assert state.status == HealthStatus.HEALTHY.value
    assert incident.status == IncidentLifecycle.RESOLVED.value
    assert incident.resolved_by == "strategy-monitoring-recovery"


@pytest.mark.asyncio
async def test_fresh_runtime_recovers_stale_incident_without_calling_strategy_healthy():
    db = _transactional_db()
    service = StrategyMonitoringService(db, now_fn=lambda: NOW)
    activation = _activation()
    activation.last_evaluated_at = NOW - timedelta(seconds=1)
    binding = _binding(activation)
    state = _health(binding, status=HealthStatus.STALE.value)
    state.cause = HealthCause.RUNTIME.value
    state.severity = IncidentSeverity.WARNING.value
    stale_incident = _incident(binding, activation, cause=HealthCause.RUNTIME)
    stale_incident.divergence_type = "stale_strategy_state"
    db.execute.return_value = _scalar_all_result([stale_incident])

    await service._evaluate_staleness(
        binding=binding,
        activation=activation,
        health=state,
        policy=_policy(stale_after_seconds=300),
    )

    assert stale_incident.status == IncidentLifecycle.RESOLVED.value
    assert stale_incident.resolved_by == "strategy-monitoring-recovery"
    # Runtime freshness proves liveness only; it must not create a false
    # conclusion that historical and live strategy behaviour is healthy.
    assert state.status == HealthStatus.UNKNOWN.value
    assert state.cause is None
    assert state.severity is None


@pytest.mark.asyncio
async def test_missing_current_comparison_evidence_creates_reference_incident_not_a_false_healthy_state():
    db = _transactional_db()
    service = StrategyMonitoringService(db, now_fn=lambda: NOW)
    activation = _activation()
    binding = _binding(activation)
    state = _health(binding, status=HealthStatus.HEALTHY.value)

    # No complete comparison has been recorded since registration.  Fresh
    # evaluator liveness cannot prove strategy equivalence.
    db.execute.return_value = _scalar_first_result(None)
    await service._evaluate_comparison_freshness(
        binding=binding,
        activation=activation,
        health=state,
        policy=_policy(stale_after_seconds=300),
    )

    added = [call.args[0] for call in db.add.call_args_list]
    incident = next(item for item in added if isinstance(item, StrategyIncident))
    assert incident.divergence_type == "stale_comparison_evidence"
    assert incident.cause == HealthCause.REFERENCE.value
    assert state.status == HealthStatus.DEGRADED.value
    assert state.cause == HealthCause.REFERENCE.value
    assert state.status != HealthStatus.HEALTHY.value


@pytest.mark.asyncio
async def test_stale_runtime_creates_one_runtime_incident_with_explicit_threshold():
    db = _transactional_db()
    service = StrategyMonitoringService(db, now_fn=lambda: NOW)
    activation = _activation()
    activation.last_evaluated_at = START
    binding = _binding(activation)
    state = _health(binding)

    db.execute.return_value = _scalar_first_result(None)
    await service._evaluate_staleness(
        binding=binding,
        activation=activation,
        health=state,
        policy=_policy(stale_after_seconds=300),
    )

    added = [call.args[0] for call in db.add.call_args_list]
    incident = next(item for item in added if isinstance(item, StrategyIncident))
    assert incident.divergence_type == "stale_strategy_state"
    assert incident.cause == HealthCause.RUNTIME.value
    assert state.status == HealthStatus.STALE.value
    assert state.cause == HealthCause.RUNTIME.value


@pytest.mark.asyncio
async def test_failed_activation_is_an_actionable_runtime_incident_not_unknown_health():
    """A durable evaluator failure must not be hidden as a deactivation/stale state."""
    db = _transactional_db()
    service = StrategyMonitoringService(db, now_fn=lambda: NOW)
    activation = _activation()
    activation.status = "error"
    activation.last_error = "strategy definition checksum mismatch"
    binding = _binding(activation)
    health = _health(binding)
    policy = _policy()
    db.execute.return_value = _scalar_first_result(None)

    await service._record_activation_runtime_failure(
        binding=binding,
        activation=activation,
        health=health,
        policy=policy,
    )

    added = [call.args[0] for call in db.add.call_args_list]
    incident = next(item for item in added if isinstance(item, StrategyIncident))
    evidence = next(item for item in added if isinstance(item, StrategyIncidentEvidence))
    assert health.status == HealthStatus.ERROR.value
    assert health.cause == HealthCause.RUNTIME.value
    assert incident.divergence_type == "evaluator_failure"
    assert incident.cause == HealthCause.RUNTIME.value
    assert evidence.evidence_kind == "runtime_state"

    # A retry after a process restart sees the same evidence and does not
    # create a second alert or inflate occurrence counts.
    db.execute.reset_mock()
    db.add.reset_mock()
    db.execute.side_effect = [_scalar_first_result(incident), _scalar_first_result(evidence)]
    await service._record_activation_runtime_failure(
        binding=binding,
        activation=activation,
        health=health,
        policy=policy,
    )
    assert incident.occurrence_count == 1
    db.add.assert_not_called()


@pytest.mark.asyncio
async def test_failed_activation_transitions_owned_binding_to_error_before_monitoring_runs():
    """A claimed binding is fenced from future work after evaluator failure."""
    db = _transactional_db()
    service = StrategyMonitoringService(db, now_fn=lambda: NOW)
    activation = _activation()
    activation.status = "error"
    binding = _binding(activation)
    health = _health(binding)
    claim = _BindingClaim(binding.id, activation.id, "token", "worker")

    service._locked_activation = AsyncMock(return_value=activation)
    service._owned_binding = AsyncMock(return_value=binding)
    service._health_state = AsyncMock(return_value=health)
    service._validated_policy = MagicMock(return_value=_policy())
    service._record_activation_runtime_failure = AsyncMock()

    assert await service._process_one_comparison(claim) is False
    assert binding.status == "error"
    service._record_activation_runtime_failure.assert_awaited_once()


@pytest.mark.asyncio
async def test_registration_rejects_an_already_failed_activation_without_creating_a_binding():
    """Registration is only valid for active runtimes; no undefined/error path is allowed."""
    db = _transactional_db()
    service = StrategyMonitoringService(db, now_fn=lambda: NOW)
    activation = _activation()
    activation.status = "error"
    comparison_policy = _comparison_policy()
    registration = MonitoringRegistrationInput(
        activation_id=activation.id,
        reference_run_id=HASH_C,
        comparison_policy_id=comparison_policy.policy_id,
        comparison_policy_revision=comparison_policy.revision,
        comparison_policy_hash=comparison_policy.policy_hash(),
        comparison_policy=comparison_policy,
        policy=_policy(),
    )
    db.execute.return_value = _scalar_first_result(activation)

    with pytest.raises(StrategyMonitoringRegistrationError, match="only an active"):
        await service.register(registration)
    db.add.assert_not_called()


@pytest.mark.asyncio
async def test_monitoring_internal_failure_creates_runtime_evidence_and_is_not_overwritten_by_finalization():
    """Crash/retry handling preserves an actionable error until a later healthy cycle."""
    db = _transactional_db()
    service = StrategyMonitoringService(db, now_fn=lambda: NOW)
    activation = _activation()
    binding = _binding(activation)
    health = _health(binding)
    claim = _BindingClaim(binding.id, activation.id, "token", "worker")

    service._locked_activation = AsyncMock(return_value=activation)
    service._owned_binding = AsyncMock(return_value=binding)
    service._health_state = AsyncMock(return_value=health)
    service._validated_policy = MagicMock(return_value=_policy())
    service._record_activation_runtime_failure = AsyncMock()
    await service._record_binding_error(claim, RuntimeError("database transaction lost"))

    assert health.status == HealthStatus.ERROR.value
    assert health.cause == HealthCause.RUNTIME.value
    assert health.last_error == "database transaction lost"
    service._record_activation_runtime_failure.assert_awaited_once()
    assert service._record_activation_runtime_failure.await_args.kwargs["runtime_status"] == "monitoring_error"

    service._claim_binding = AsyncMock(return_value=claim)
    service._process_one_comparison = AsyncMock(side_effect=RuntimeError("database transaction lost"))
    service._record_binding_error = AsyncMock()
    service._finalize_binding_cycle = AsyncMock()
    service._release_claim = AsyncMock()

    assert await service.process_binding(binding.id, worker_id="worker") == 0
    service._record_binding_error.assert_awaited_once()
    service._finalize_binding_cycle.assert_not_awaited()
    service._release_claim.assert_awaited_once_with(claim)


@pytest.mark.asyncio
async def test_expired_monitoring_claim_cannot_be_renewed_by_the_stale_owner():
    """The atomic renewal update must fence an owner after its expiry deadline."""
    db = _transactional_db()
    service = StrategyMonitoringService(db, now_fn=lambda: NOW)
    claim = _BindingClaim(uuid.uuid4(), uuid.uuid4(), "expired-token", "stale-worker")
    update_result = MagicMock(rowcount=0)
    db.execute.return_value = update_result

    assert await service._renew_claim(claim) is False

    statement = db.execute.call_args.args[0]
    assert "lease_expires_at >" in str(statement)
    assert "lease_expires_at >=" not in str(statement)


@pytest.mark.asyncio
async def test_comparison_producer_persists_runtime_error_when_production_fails(monkeypatch):
    """A producer failure must not leave a previous healthy projection silent."""
    binding_id, activation_id = uuid.uuid4(), uuid.uuid4()
    list_session = AsyncMock()
    listed = MagicMock()
    listed.fetchall.return_value = [(binding_id,)]
    list_session.execute.return_value = listed
    work_session = AsyncMock()

    @asynccontextmanager
    async def _session_context(session):
        yield session

    contexts = iter([_session_context(list_session), _session_context(work_session)])
    claim = _BindingClaim(binding_id, activation_id, "lease-token", "worker")
    record_error = AsyncMock()
    release = AsyncMock()

    async def _claim(self, requested_binding_id, *, worker_id):
        assert requested_binding_id == binding_id
        return claim

    async def _produce(self, acquired_claim):
        assert acquired_claim == claim
        raise RuntimeError("comparison persistence failed")

    monkeypatch.setattr(StrategyMonitoringService, "_claim_binding", _claim)
    monkeypatch.setattr(StrategyMonitoringService, "_produce_next_comparison", _produce)
    monkeypatch.setattr(StrategyMonitoringService, "_record_binding_error", record_error)
    monkeypatch.setattr(StrategyMonitoringService, "_release_claim", release)

    assert await StrategyComparisonProducerService.process_active(lambda: next(contexts), binding_limit=1) == 0
    record_error.assert_awaited_once()
    assert record_error.await_args.args[0] == claim
    assert isinstance(record_error.await_args.args[1], RuntimeError)
    release.assert_awaited_once_with(claim)


@pytest.mark.asyncio
async def test_process_active_uses_one_listing_session_then_a_fresh_session_per_binding(monkeypatch):
    binding_ids = [uuid.uuid4(), uuid.uuid4()]
    list_session = AsyncMock()
    list_result = MagicMock()
    list_result.fetchall.return_value = [(binding_ids[0],), (binding_ids[1],)]
    list_session.execute.return_value = list_result
    binding_session_one = AsyncMock()
    binding_session_two = AsyncMock()

    @asynccontextmanager
    async def _session_context(session):
        yield session

    contexts = iter(
        [
            _session_context(list_session),
            _session_context(binding_session_one),
            _session_context(binding_session_two),
        ]
    )

    def session_factory():
        return next(contexts)

    processed_sessions: list[tuple[object, uuid.UUID, str | None]] = []

    async def _process_binding(self, binding_id, *, worker_id=None):
        processed_sessions.append((self.db, binding_id, worker_id))
        return 1

    monkeypatch.setattr(StrategyMonitoringService, "process_binding", _process_binding)

    processed = await StrategyMonitoringService.process_active(session_factory, binding_limit=2)

    assert processed == 2
    assert list_session.execute.await_count == 1
    assert [item[0] for item in processed_sessions] == [binding_session_one, binding_session_two]
    assert [item[1] for item in processed_sessions] == binding_ids
    assert processed_sessions[0][2] == processed_sessions[1][2]
    assert processed_sessions[0][2]


@pytest.mark.asyncio
async def test_process_active_isolates_one_failed_binding_and_continues_with_later_bindings(monkeypatch):
    binding_ids = [uuid.uuid4(), uuid.uuid4()]
    list_session = AsyncMock()
    list_result = MagicMock()
    list_result.fetchall.return_value = [(binding_ids[0],), (binding_ids[1],)]
    list_session.execute.return_value = list_result
    first_session = AsyncMock()
    second_session = AsyncMock()

    @asynccontextmanager
    async def _session_context(session):
        yield session

    contexts = iter([_session_context(list_session), _session_context(first_session), _session_context(second_session)])

    def session_factory():
        return next(contexts)

    async def _process_binding(self, binding_id, *, worker_id=None):
        if binding_id == binding_ids[0]:
            raise RuntimeError("corrupt comparison payload")
        return 1

    monkeypatch.setattr(StrategyMonitoringService, "process_binding", _process_binding)

    assert await StrategyMonitoringService.process_active(session_factory, binding_limit=2) == 1
