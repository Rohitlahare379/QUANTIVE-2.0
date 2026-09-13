"""Durable incident monitoring over immutable strategy-comparison evidence.

This module deliberately does not produce market data, execute strategies, or
reinterpret the comparison engine.  A caller explicitly binds an active live
strategy to a reference result and a frozen monitoring policy.  The worker then
consumes only already-persisted comparison runs and live-runtime timestamps.

Two idempotency boundaries prevent alert spam and make redelivery safe:

* a stable incident key groups the same underlying condition across comparison
  windows without using the per-finding forensic fingerprint; and
* each immutable finding can be linked as incident evidence once only.

The durable binding lease is separate from the Redis dispatch lease.  Redis
only coalesces messages; database ownership protects state mutations when
multiple worker processes or a restarted worker see the same binding.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable

from sqlalchemy import and_, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.models.backtest import BacktestResult
from app.models.divergence import StrategyComparisonRun, StrategyDivergenceFinding
from app.models.live_strategy import LiveStrategyActivation, LiveStrategyState
from app.models.strategy_monitoring import (
    StrategyHealthState,
    StrategyIncident,
    StrategyIncidentEvidence,
    StrategyMonitoringAssessment,
    StrategyMonitoringBinding,
)
from app.monitoring.contracts import (
    EvidenceKind,
    HealthCause,
    HealthStatus,
    IncidentLifecycle,
    IncidentSeverity,
    MonitoringBindingStatus,
    MonitoringPolicy,
    MonitoringRegistrationInput,
    cause_for_divergence,
)
from app.divergence.contracts import ComparisonPolicy
from app.services.exceptions import (
    StrategyIncidentNotFoundError,
    StrategyMonitoringError,
    StrategyMonitoringRegistrationError,
)
from app.services.strategy_comparison import ComparisonSupplement, StrategyComparisonService


logger = logging.getLogger(__name__)

_STALE_DIVERGENCE_TYPE = "stale_strategy_state"
_STALE_CATEGORY = "runtime_state"
_STALE_COMPARISON_DIVERGENCE_TYPE = "stale_comparison_evidence"
_STALE_COMPARISON_CATEGORY = "coverage"
_RECOVERY_ACTOR = "strategy-monitoring-recovery"


@dataclass(frozen=True)
class _BindingClaim:
    binding_id: uuid.UUID
    activation_id: uuid.UUID
    token: str
    worker_id: str


def _severity_rank(value: IncidentSeverity | str) -> int:
    return {
        IncidentSeverity.INFORMATIONAL.value: 0,
        IncidentSeverity.WARNING.value: 1,
        IncidentSeverity.CRITICAL.value: 2,
    }[value.value if isinstance(value, IncidentSeverity) else value]


def _max_severity(*values: IncidentSeverity | str) -> IncidentSeverity:
    highest = max(values, key=_severity_rank)
    return highest if isinstance(highest, IncidentSeverity) else IncidentSeverity(highest)


def _hash_payload(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _stable_value_discriminator(value: dict[str, Any]) -> dict[str, Any]:
    """Keep only identity-like fields when grouping recurring evidence.

    Finding values deliberately contain timestamps, prices and metric values so
    their forensic fingerprints are unique per observation.  Those fields are
    intentionally excluded here: a recurring data discrepancy for the same
    asset/signal should update one incident rather than page on every window.
    """

    stable_keys = (
        "asset_id",
        "event_type",
        "signal_id",
        "side",
        "direction",
        "position_state",
        "error_code",
        "configuration_hash",
        "strategy_id",
        "strategy_version_id",
        "strategy_version_number",
        "strategy_definition_hash",
        "revision",
        "reference_complete",
        "observed_complete",
    )
    return {key: value[key] for key in stable_keys if key in value}


def incident_dedup_key(
    *,
    binding: StrategyMonitoringBinding,
    finding: StrategyDivergenceFinding,
    cause: HealthCause,
) -> str:
    """Return a cross-run incident key without collapsing unrelated evidence.

    The binding (and therefore its reference/policy scope) is part of the key.
    This makes a changed reference or monitoring policy an auditable rebasing,
    not an invisible continuation of an old alert stream.
    """

    return _hash_payload(
        {
            "binding_id": str(binding.id),
            "reference_run_id": binding.reference_run_id,
            "policy_hash": binding.policy_hash,
            "strategy_id": str(finding.strategy_id),
            "strategy_version_id": str(finding.strategy_version_id),
            "strategy_definition_hash": finding.strategy_definition_hash,
            "divergence_type": finding.divergence_type,
            "category": finding.category,
            "cause": cause.value,
            "asset_id": finding.asset_id,
            "expected": _stable_value_discriminator(finding.expected_value),
            "observed": _stable_value_discriminator(finding.observed_value),
        }
    )


class StrategyMonitoringService:
    """Register monitored activations and aggregate their comparison evidence."""

    def __init__(
        self,
        db: AsyncSession,
        *,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self.db = db
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    def _now(self) -> datetime:
        value = self._now_fn()
        if value.tzinfo is None or value.utcoffset() is None:
            raise StrategyMonitoringError("monitoring clock must return a timezone-aware datetime")
        return value.astimezone(timezone.utc)

    async def register(self, registration: MonitoringRegistrationInput) -> StrategyMonitoringBinding:
        """Enroll one active runtime under an explicit reference and policy.

        Monitoring never guesses a reference result.  If a registration changes
        reference/policy, the former active binding is deactivated and retained
        for incident audit history before a new immutable binding is created.
        """

        now = self._now()
        policy = registration.policy
        async with self.db.begin():
            activation_result = await self.db.execute(
                select(LiveStrategyActivation)
                .where(LiveStrategyActivation.id == registration.activation_id)
                .with_for_update()
            )
            activation = activation_result.scalars().first()
            if activation is None:
                raise StrategyMonitoringRegistrationError(
                    f"live activation {registration.activation_id} not found"
                )
            if activation.status != "active":
                raise StrategyMonitoringRegistrationError("only an active strategy runtime can be monitored")

            reference_result = await self.db.execute(
                select(BacktestResult)
                .where(BacktestResult.run_id == registration.reference_run_id)
                .with_for_update()
            )
            reference = reference_result.scalars().first()
            if reference is None:
                raise StrategyMonitoringRegistrationError(
                    f"reference backtest {registration.reference_run_id} not found"
                )
            if reference.strategy_id != activation.strategy_id:
                raise StrategyMonitoringRegistrationError(
                    "monitoring reference must belong to the activation's logical strategy"
                )

            active_binding_result = await self.db.execute(
                select(StrategyMonitoringBinding)
                .where(
                    StrategyMonitoringBinding.activation_id == activation.id,
                    StrategyMonitoringBinding.status == MonitoringBindingStatus.ACTIVE.value,
                )
                .with_for_update()
            )
            existing = active_binding_result.scalars().first()
            policy_hash = policy.policy_hash()
            if (
                existing is not None
                and existing.reference_run_id == registration.reference_run_id
                and existing.comparison_policy_id == registration.comparison_policy_id
                and existing.comparison_policy_revision == registration.comparison_policy_revision
                and existing.comparison_policy_hash == registration.comparison_policy_hash
                and existing.policy_hash == policy_hash
            ):
                return existing

            if existing is not None:
                existing.status = MonitoringBindingStatus.DEACTIVATED.value
                existing.deactivated_at = now
                self._clear_claim(existing)

            binding = StrategyMonitoringBinding(
                id=uuid.uuid4(),
                activation_id=activation.id,
                reference_run_id=registration.reference_run_id,
                comparison_policy_id=registration.comparison_policy_id,
                comparison_policy_revision=registration.comparison_policy_revision,
                comparison_policy_hash=registration.comparison_policy_hash,
                comparison_policy=registration.comparison_policy.model_dump(mode="json"),
                policy_id=policy.policy_id,
                policy_revision=policy.revision,
                policy_hash=policy_hash,
                policy=policy.model_dump(mode="json"),
                status=MonitoringBindingStatus.ACTIVE.value,
                registered_at=now,
                claim_attempt_count=0,
            )
            self.db.add(binding)

            health_result = await self.db.execute(
                select(StrategyHealthState)
                .where(StrategyHealthState.activation_id == activation.id)
                .with_for_update()
            )
            health = health_result.scalars().first()
            if health is None:
                self.db.add(
                    StrategyHealthState(
                        activation_id=activation.id,
                        binding_id=binding.id,
                        status=HealthStatus.UNKNOWN.value,
                        consecutive_healthy_checks=0,
                    )
                )
            else:
                health.binding_id = binding.id
                self._set_unknown(health)
                health.last_error = None
            await self.db.flush()
            return binding

    async def deactivate(self, binding_id: uuid.UUID) -> bool:
        """Stop monitoring without deleting historical incidents or evidence."""

        now = self._now()
        async with self.db.begin():
            result = await self.db.execute(
                select(StrategyMonitoringBinding)
                .where(StrategyMonitoringBinding.id == binding_id)
                .with_for_update()
            )
            binding = result.scalars().first()
            if binding is None:
                return False
            if binding.status == MonitoringBindingStatus.ACTIVE.value:
                binding.status = MonitoringBindingStatus.DEACTIVATED.value
                binding.deactivated_at = now
                self._clear_claim(binding)
                health_result = await self.db.execute(
                    select(StrategyHealthState)
                    .where(StrategyHealthState.activation_id == binding.activation_id)
                    .with_for_update()
                )
                health = health_result.scalars().first()
                if health is not None and health.binding_id == binding.id:
                    # The live runtime may remain active, but it is no longer
                    # under this monitoring policy.  Do not leave a stale
                    # healthy/degraded projection after explicit unenrolment.
                    self._set_unknown(health)
                    health.last_error = None
            return True

    async def acknowledge_incident(
        self,
        incident_id: uuid.UUID,
        *,
        actor: str,
        note: str | None = None,
    ) -> StrategyIncident:
        """Acknowledge an open incident without modifying immutable evidence."""

        actor = actor.strip()
        if not actor:
            raise StrategyMonitoringError("acknowledgement actor cannot be blank")
        if note is not None and len(note) > 2000:
            raise StrategyMonitoringError("acknowledgement note exceeds 2000 characters")
        now = self._now()
        async with self.db.begin():
            incident = await self._locked_incident(incident_id)
            if incident is None:
                raise StrategyIncidentNotFoundError(f"incident {incident_id} not found")
            if incident.status == IncidentLifecycle.RESOLVED.value:
                raise StrategyMonitoringError("a resolved incident cannot be acknowledged")
            if incident.status == IncidentLifecycle.OPEN.value:
                incident.status = IncidentLifecycle.ACKNOWLEDGED.value
                incident.acknowledged_at = now
                incident.acknowledged_by = actor
                incident.acknowledgement_note = note
            return incident

    async def resolve_incident(
        self,
        incident_id: uuid.UUID,
        *,
        actor: str,
        note: str | None = None,
    ) -> StrategyIncident:
        """Explicitly resolve an incident; a later new finding reopens it."""

        actor = actor.strip()
        if not actor:
            raise StrategyMonitoringError("resolution actor cannot be blank")
        if note is not None and len(note) > 2000:
            raise StrategyMonitoringError("resolution note exceeds 2000 characters")
        now = self._now()
        async with self.db.begin():
            incident = await self._locked_incident(incident_id)
            if incident is None:
                raise StrategyIncidentNotFoundError(f"incident {incident_id} not found")
            if incident.status != IncidentLifecycle.RESOLVED.value:
                incident.status = IncidentLifecycle.RESOLVED.value
                incident.resolved_at = now
                incident.resolved_by = actor
                incident.resolution_note = note
            return incident

    @classmethod
    async def process_active(
        cls,
        session_factory: async_sessionmaker[AsyncSession],
        binding_limit: int | None = None,
    ) -> int:
        """Process active bindings sequentially with a fresh session each.

        This deliberately avoids concurrent access to a single ``AsyncSession``
        and bounds the polling cycle before it reaches durable incident work.
        """

        limit = binding_limit or settings.STRATEGY_MONITORING_MAX_BINDINGS_PER_CYCLE
        async with session_factory() as list_session:
            result = await list_session.execute(
                select(StrategyMonitoringBinding.id)
                .where(StrategyMonitoringBinding.status == MonitoringBindingStatus.ACTIVE.value)
                .order_by(
                    StrategyMonitoringBinding.last_processed_at.asc().nullsfirst(),
                    StrategyMonitoringBinding.registered_at.asc(),
                    StrategyMonitoringBinding.id.asc(),
                )
                .limit(limit)
            )
            binding_ids = [row[0] for row in result.fetchall()]

        worker_id = f"strategy-monitoring-{uuid.uuid4()}"
        processed = 0
        for binding_id in binding_ids:
            async with session_factory() as monitoring_session:
                try:
                    processed += await cls(monitoring_session).process_binding(
                        binding_id,
                        worker_id=worker_id,
                    )
                except Exception:
                    # One corrupt/misconfigured binding must not block every
                    # other active strategy in this bounded dispatch cycle.
                    logger.exception("Strategy monitoring failed for binding %s", binding_id)
        return processed

    async def process_binding(
        self,
        binding_id: uuid.UUID,
        *,
        worker_id: str | None = None,
    ) -> int:
        """Claim one binding and process a bounded, restart-safe run slice.

        Each comparison run commits independently.  A malformed run therefore
        cannot roll back incidents already aggregated for earlier runs or stop
        all other active bindings in the worker cycle.
        """

        worker_id = worker_id or f"strategy-monitoring-{uuid.uuid4()}"
        claim = await self._claim_binding(binding_id, worker_id=worker_id)
        if claim is None:
            return 0

        processed = 0
        processing_failed = False
        try:
            for _ in range(settings.STRATEGY_MONITORING_MAX_COMPARISONS_PER_BINDING):
                try:
                    has_run = await self._process_one_comparison(claim)
                except Exception as exc:
                    await self._record_binding_error(claim, exc)
                    logger.exception(
                        "Strategy monitoring failed for binding %s; retaining prior committed evidence",
                        claim.binding_id,
                    )
                    processing_failed = True
                    break
                if not has_run:
                    break
                processed += 1
                if not await self._renew_claim(claim):
                    logger.warning("Strategy monitoring lost binding lease for %s", claim.binding_id)
                    break

            # A failure projection is authoritative for this cycle.  Running
            # freshness/recovery logic immediately afterwards could overwrite
            # it with unknown/stale and hide the runtime error we just stored.
            if not processing_failed:
                try:
                    await self._finalize_binding_cycle(claim)
                except Exception as exc:
                    await self._record_binding_error(claim, exc)
                    logger.exception("Unable to finalize strategy monitoring binding %s", claim.binding_id)
            return processed
        finally:
            # Completion/retry cleanup is ownership conditional.  If this
            # process lost its lease, it cannot clear a newer worker's claim.
            await self._release_claim(claim)

    async def _produce_next_comparison(self, claim: _BindingClaim) -> bool:
        """Persist at most one bounded canonical comparison for a binding.

        Current Quantive data does not retain an immutable historical candle
        snapshot for every backtest.  The producer therefore deliberately
        emits ``insufficient_reference`` until a reference candle snapshot is
        supplied; it still persists signal/data evidence and can never turn
        missing reference lineage into a healthy result.
        """
        plan = await self._next_comparison_plan(claim)
        if plan is None:
            return False
        activation_id, reference_run_id, supplement = plan
        await StrategyComparisonService(self.db).compare_activation(
            activation_id=activation_id,
            reference_run_id=reference_run_id,
            supplement=supplement,
        )
        return True

    async def _next_comparison_plan(
        self,
        claim: _BindingClaim,
    ) -> tuple[uuid.UUID, str, ComparisonSupplement] | None:
        async with self.db.begin():
            activation = await self._locked_activation(claim.activation_id)
            binding = await self._owned_binding(claim)
            if activation is None or binding is None or activation.status != "active":
                return None
            policy = self._validated_comparison_policy(binding)
            cursor_result = await self.db.execute(
                select(StrategyComparisonRun.window_end)
                .where(
                    StrategyComparisonRun.activation_id == activation.id,
                    StrategyComparisonRun.reference_run_id == binding.reference_run_id,
                    StrategyComparisonRun.policy_hash == binding.comparison_policy_hash,
                )
                .order_by(StrategyComparisonRun.window_end.desc(), StrategyComparisonRun.created_at.desc())
                .limit(1)
            )
            previous_end = cursor_result.scalar_one_or_none()
            state_result = await self.db.execute(
                select(LiveStrategyState.last_candle_timestamp)
                .where(LiveStrategyState.activation_id == activation.id)
            )
            cursor_values = [row[0] for row in state_result.all() if row[0] is not None]
            if not cursor_values:
                return None
            # A multi-asset strategy is comparable only through its slowest
            # durable cursor.  It cannot claim a window an asset has not seen.
            common_end = min(cursor_values)
            window_start = (
                previous_end + timedelta(microseconds=1)
                if previous_end is not None
                else activation.activated_at
            )
            if common_end <= window_start:
                return None
            window_end = min(
                common_end,
                window_start + timedelta(seconds=settings.STRATEGY_COMPARISON_MAX_WINDOW_SECONDS),
            )
            if window_end <= window_start:
                return None
            return (
                activation.id,
                binding.reference_run_id,
                ComparisonSupplement(
                    policy=policy,
                    window_start=window_start,
                    window_end=window_end,
                    observed_complete=True,
                    observed_source={
                        "producer": "quantive.canonical-live-observations",
                        "coverage_mode": "durable_per_asset_cursor",
                        "binding_id": str(binding.id),
                    },
                ),
            )

    @staticmethod
    def _validated_comparison_policy(binding: StrategyMonitoringBinding) -> ComparisonPolicy:
        try:
            policy = ComparisonPolicy.model_validate(binding.comparison_policy)
        except ValueError as exc:
            raise StrategyMonitoringError("stored comparison policy is invalid") from exc
        if (
            policy.policy_id != binding.comparison_policy_id
            or policy.revision != binding.comparison_policy_revision
            or policy.policy_hash() != binding.comparison_policy_hash
        ):
            raise StrategyMonitoringError("stored comparison policy does not match its immutable snapshot")
        return policy


    async def _process_one_comparison(self, claim: _BindingClaim) -> bool:
        """Claim the oldest unassessed run and commit only that run's effects."""

        async with self.db.begin():
            activation = await self._locked_activation(claim.activation_id)
            if activation is None:
                return False
            binding = await self._owned_binding(claim)
            if binding is None:
                return False
            policy = self._validated_policy(binding)
            health = await self._health_state(binding, activation)
            if activation.status == "error":
                binding.status = MonitoringBindingStatus.ERROR.value
                await self._record_activation_runtime_failure(
                    binding=binding,
                    activation=activation,
                    health=health,
                    policy=policy,
                )
                return False
            if activation.status != "active":
                binding.status = MonitoringBindingStatus.DEACTIVATED.value
                binding.deactivated_at = self._now()
                self._set_unknown(health)
                return False

            run = await self._next_pending_comparison(binding)
            if run is None:
                return False
            await self._process_comparison_run(
                binding=binding,
                activation=activation,
                health=health,
                policy=policy,
                comparison_run=run,
                worker_id=claim.worker_id,
            )
            binding.last_processed_at = self._now()
            return True

    async def _finalize_binding_cycle(self, claim: _BindingClaim) -> None:
        """Project stale coverage/runtime state after the bounded run slice."""

        async with self.db.begin():
            activation = await self._locked_activation(claim.activation_id)
            if activation is None:
                return
            binding = await self._owned_binding(claim)
            if binding is None:
                return
            policy = self._validated_policy(binding)
            health = await self._health_state(binding, activation)
            if activation.status == "error":
                binding.status = MonitoringBindingStatus.ERROR.value
                await self._record_activation_runtime_failure(
                    binding=binding,
                    activation=activation,
                    health=health,
                    policy=policy,
                )
                return
            if activation.status != "active":
                binding.status = MonitoringBindingStatus.DEACTIVATED.value
                binding.deactivated_at = self._now()
                self._set_unknown(health)
                return
            await self._evaluate_comparison_freshness(
                binding=binding,
                activation=activation,
                health=health,
                policy=policy,
            )
            await self._evaluate_staleness(
                binding=binding,
                activation=activation,
                health=health,
                policy=policy,
            )
            binding.last_processed_at = self._now()

    async def _record_binding_error(self, claim: _BindingClaim, error: Exception) -> None:
        """Persist an isolated runtime error without taking other bindings down."""

        try:
            async with self.db.begin():
                activation = await self._locked_activation(claim.activation_id)
                binding = await self._owned_binding(claim)
                if activation is None or binding is None:
                    return
                health = await self._health_state(binding, activation)
                policy: MonitoringPolicy | None = None
                try:
                    policy = self._validated_policy(binding)
                    severity = policy.severity_for(HealthCause.RUNTIME, 1)
                except StrategyMonitoringError:
                    # The policy itself cannot be trusted here; a critical
                    # runtime projection is safer than silently retaining a
                    # healthy state for corrupt durable configuration.
                    severity = IncidentSeverity.CRITICAL
                self._set_health(health, HealthStatus.ERROR, HealthCause.RUNTIME, severity)
                health.last_error = str(error)[:2000]
                await self._record_activation_runtime_failure(
                    binding=binding,
                    activation=activation,
                    health=health,
                    policy=policy,
                    error_message=health.last_error,
                    runtime_status="monitoring_error",
                    fallback_severity=severity,
                )
                binding.last_processed_at = self._now()
        except Exception:
            logger.exception("Unable to record strategy monitoring binding error")

    async def _record_activation_runtime_failure(
        self,
        *,
        binding: StrategyMonitoringBinding,
        activation: LiveStrategyActivation,
        health: StrategyHealthState,
        policy: MonitoringPolicy | None,
        error_message: str | None = None,
        runtime_status: str | None = None,
        fallback_severity: IncidentSeverity | None = None,
    ) -> None:
        """Turn a durable evaluator error into an actionable incident.

        Treating a failed activation as an ordinary deactivation leaves health
        ``unknown`` and suppresses exactly the runtime failure an operator must
        see.  The evidence is token-independent and deduplicated by binding so
        repeated monitor cycles update one incident rather than alert-spam.
        """
        now = self._now()
        severity = (
            policy.severity_for(HealthCause.RUNTIME, 1)
            if policy is not None
            else fallback_severity or IncidentSeverity.CRITICAL
        )
        self._set_health(health, HealthStatus.ERROR, HealthCause.RUNTIME, severity)
        health.last_error = (
            error_message
            or activation.last_error
            or "live strategy activation entered error state"
        )
        status_label = runtime_status or activation.status
        key = _hash_payload(
            {
                "binding_id": str(binding.id),
                "reference_run_id": binding.reference_run_id,
                "policy_hash": binding.policy_hash,
                "divergence_type": "evaluator_failure",
                "category": "runtime_state",
                "activation_id": str(activation.id),
            }
        )
        evidence_key = _hash_payload(
            {
                "binding_id": str(binding.id),
                "kind": EvidenceKind.RUNTIME_STATE.value,
                "activation_status": status_label,
                "last_error": health.last_error,
            }
        )
        source_reference = {
            "binding_id": str(binding.id),
            "activation_id": str(activation.id),
            "reference_run_id": binding.reference_run_id,
            "activation_status": status_label,
        }
        incident_result = await self.db.execute(
            select(StrategyIncident).where(StrategyIncident.dedup_key == key).with_for_update()
        )
        incident = incident_result.scalars().first()
        if incident is None:
            incident = StrategyIncident(
                id=uuid.uuid4(),
                dedup_key=key,
                binding_id=binding.id,
                activation_id=activation.id,
                reference_run_id=binding.reference_run_id,
                monitoring_policy_hash=binding.policy_hash,
                strategy_id=activation.strategy_id,
                strategy_version_id=activation.strategy_version_id,
                strategy_version_number=activation.strategy_version_number,
                strategy_definition_hash=activation.strategy_definition_hash,
                asset_id=None,
                divergence_type="evaluator_failure",
                category="runtime_state",
                cause=HealthCause.RUNTIME.value,
                severity=severity.value,
                status=IncidentLifecycle.OPEN.value,
                first_detected_at=now,
                last_detected_at=now,
                occurrence_count=1,
                source_reference=source_reference,
                evidence_summary={"activation_error": health.last_error},
            )
            self.db.add(incident)
        else:
            evidence_result = await self.db.execute(
                select(StrategyIncidentEvidence)
                .where(
                    StrategyIncidentEvidence.incident_id == incident.id,
                    StrategyIncidentEvidence.evidence_key == evidence_key,
                )
                .with_for_update()
            )
            if evidence_result.scalars().first() is not None:
                return
            incident.occurrence_count += 1
            incident.last_detected_at = now
            incident.severity = _max_severity(
                incident.severity,
                (
                    policy.severity_for(HealthCause.RUNTIME, incident.occurrence_count)
                    if policy is not None
                    else fallback_severity or IncidentSeverity.CRITICAL
                ),
            ).value
            incident.source_reference = source_reference
            if incident.status == IncidentLifecycle.RESOLVED.value:
                incident.status = IncidentLifecycle.OPEN.value
                incident.resolved_at = None
                incident.resolved_by = None
                incident.resolution_note = None
        self.db.add(
            StrategyIncidentEvidence(
                id=uuid.uuid4(),
                incident_id=incident.id,
                finding_id=None,
                comparison_run_id=None,
                evidence_key=evidence_key,
                evidence_kind=EvidenceKind.RUNTIME_STATE.value,
                observed_at=now,
                source_reference=source_reference,
                payload={"activation_status": status_label, "last_error": health.last_error},
            )
        )

    async def _claim_binding(
        self,
        binding_id: uuid.UUID,
        *,
        worker_id: str,
    ) -> _BindingClaim | None:
        now = self._now()
        token = uuid.uuid4().hex
        async with self.db.begin():
            result = await self.db.execute(
                select(StrategyMonitoringBinding)
                .where(
                    StrategyMonitoringBinding.id == binding_id,
                    StrategyMonitoringBinding.status == MonitoringBindingStatus.ACTIVE.value,
                    or_(
                        StrategyMonitoringBinding.lease_expires_at.is_(None),
                        # The deadline is exclusive: an owner at the exact
                        # timestamp is stale and can be replaced immediately.
                        StrategyMonitoringBinding.lease_expires_at <= now,
                    ),
                )
                .with_for_update(skip_locked=True)
            )
            binding = result.scalars().first()
            if binding is None:
                return None
            binding.lease_worker_id = worker_id
            binding.lease_token = token
            binding.claimed_at = now
            binding.lease_expires_at = now + timedelta(
                seconds=settings.STRATEGY_MONITORING_BINDING_LEASE_SECONDS
            )
            binding.claim_attempt_count += 1
            await self.db.flush()
        return _BindingClaim(
            binding_id=binding_id,
            activation_id=binding.activation_id,
            token=token,
            worker_id=worker_id,
        )

    async def _release_claim(self, claim: _BindingClaim) -> None:
        try:
            async with self.db.begin():
                await self.db.execute(
                    update(StrategyMonitoringBinding)
                    .where(
                        StrategyMonitoringBinding.id == claim.binding_id,
                        StrategyMonitoringBinding.lease_token == claim.token,
                    )
                    .values(
                        lease_worker_id=None,
                        lease_token=None,
                        lease_expires_at=None,
                        claimed_at=None,
                    )
                )
        except Exception:
            # Lease expiry is the crash/recovery mechanism.  A failure here
            # must never mask the original processing outcome or issue an
            # unconditional delete of another worker's ownership.
            logger.exception("Unable to release strategy monitoring binding lease")

    async def _renew_claim(self, claim: _BindingClaim) -> bool:
        """Extend only the lease still owned by this worker/token pair."""

        try:
            now = self._now()
            async with self.db.begin():
                result = await self.db.execute(
                    update(StrategyMonitoringBinding)
                    .where(
                        StrategyMonitoringBinding.id == claim.binding_id,
                        StrategyMonitoringBinding.status == MonitoringBindingStatus.ACTIVE.value,
                        StrategyMonitoringBinding.lease_worker_id == claim.worker_id,
                        StrategyMonitoringBinding.lease_token == claim.token,
                        # A late owner must never reclaim an expired lease.
                        # At the exact deadline it is already stale, so only
                        # a fresh claim may continue with a new fencing token.
                        StrategyMonitoringBinding.lease_expires_at > now,
                    )
                    .values(
                        lease_expires_at=now + timedelta(
                            seconds=settings.STRATEGY_MONITORING_BINDING_LEASE_SECONDS
                        )
                    )
                )
                return bool(result.rowcount)
        except Exception:
            logger.exception("Unable to renew strategy monitoring binding lease")
            return False

    async def _owned_binding(self, claim: _BindingClaim) -> StrategyMonitoringBinding | None:
        result = await self.db.execute(
            select(StrategyMonitoringBinding)
            .where(
                StrategyMonitoringBinding.id == claim.binding_id,
                StrategyMonitoringBinding.status == MonitoringBindingStatus.ACTIVE.value,
                StrategyMonitoringBinding.lease_token == claim.token,
                StrategyMonitoringBinding.lease_worker_id == claim.worker_id,
                StrategyMonitoringBinding.lease_expires_at > self._now(),
            )
            .with_for_update(skip_locked=True)
        )
        return result.scalars().first()

    async def _locked_activation(self, activation_id: uuid.UUID) -> LiveStrategyActivation | None:
        result = await self.db.execute(
            select(LiveStrategyActivation)
            .where(LiveStrategyActivation.id == activation_id)
            .with_for_update()
        )
        return result.scalars().first()

    async def _health_state(
        self,
        binding: StrategyMonitoringBinding,
        activation: LiveStrategyActivation,
    ) -> StrategyHealthState:
        result = await self.db.execute(
            select(StrategyHealthState)
            .where(StrategyHealthState.activation_id == activation.id)
            .with_for_update()
        )
        state = result.scalars().first()
        if state is not None:
            if state.binding_id != binding.id:
                state.binding_id = binding.id
                self._set_unknown(state)
            return state
        state = StrategyHealthState(
            activation_id=activation.id,
            binding_id=binding.id,
            status=HealthStatus.UNKNOWN.value,
            consecutive_healthy_checks=0,
        )
        self.db.add(state)
        await self.db.flush()
        return state

    def _validated_policy(self, binding: StrategyMonitoringBinding) -> MonitoringPolicy:
        try:
            policy = MonitoringPolicy.model_validate(binding.policy)
        except ValueError as exc:
            raise StrategyMonitoringError("stored monitoring policy is invalid") from exc
        if (
            policy.policy_id != binding.policy_id
            or policy.revision != binding.policy_revision
            or policy.policy_hash() != binding.policy_hash
        ):
            raise StrategyMonitoringError("stored monitoring policy does not match its immutable snapshot")
        return policy

    async def _next_pending_comparison(
        self,
        binding: StrategyMonitoringBinding,
    ) -> StrategyComparisonRun | None:
        """Get one durable comparison in evidence-window order.

        The caller commits an assessment before asking again, so a large
        backlog advances in bounded slices instead of failing permanently at
        ``MAX_COMPARISONS_PER_BINDING``.
        """

        result = await self.db.execute(
            select(StrategyComparisonRun)
            .outerjoin(
                StrategyMonitoringAssessment,
                StrategyMonitoringAssessment.comparison_run_id == StrategyComparisonRun.run_id,
            )
            .where(
                StrategyComparisonRun.activation_id == binding.activation_id,
                StrategyComparisonRun.reference_run_id == binding.reference_run_id,
                StrategyComparisonRun.policy_hash == binding.comparison_policy_hash,
                StrategyMonitoringAssessment.comparison_run_id.is_(None),
            )
            .order_by(
                StrategyComparisonRun.window_end.asc(),
                StrategyComparisonRun.created_at.asc(),
                StrategyComparisonRun.run_id.asc(),
            )
            .limit(1)
        )
        return result.scalars().first()

    async def _process_comparison_run(
        self,
        *,
        binding: StrategyMonitoringBinding,
        activation: LiveStrategyActivation,
        health: StrategyHealthState,
        policy: MonitoringPolicy,
        comparison_run: StrategyComparisonRun,
        worker_id: str,
    ) -> None:
        if comparison_run.activation_id != binding.activation_id:
            raise StrategyMonitoringError("comparison run does not belong to the monitoring binding activation")
        if comparison_run.reference_run_id != binding.reference_run_id:
            raise StrategyMonitoringError("comparison run does not belong to the monitoring binding reference")
        if comparison_run.policy_hash != binding.comparison_policy_hash:
            raise StrategyMonitoringError("comparison run does not use the binding's explicit comparison policy")

        now = self._now()
        finding_count = 0
        projection: tuple[HealthCause, IncidentSeverity] | None = None
        known_projection: tuple[HealthCause, IncidentSeverity] | None = None
        cursor_created_at: datetime | None = None
        cursor_id: uuid.UUID | None = None
        page_size = settings.STRATEGY_MONITORING_FINDING_PAGE_SIZE
        while True:
            predicates = [
                StrategyDivergenceFinding.comparison_run_id == comparison_run.run_id,
            ]
            if cursor_created_at is not None and cursor_id is not None:
                predicates.append(
                    or_(
                        StrategyDivergenceFinding.created_at > cursor_created_at,
                        and_(
                            StrategyDivergenceFinding.created_at == cursor_created_at,
                            StrategyDivergenceFinding.id > cursor_id,
                        ),
                    )
                )
            findings_result = await self.db.execute(
                select(StrategyDivergenceFinding)
                .where(*predicates)
                .order_by(StrategyDivergenceFinding.created_at.asc(), StrategyDivergenceFinding.id.asc())
                .limit(page_size)
            )
            findings = list(findings_result.scalars().all())
            if not findings:
                break
            for finding in findings:
                classification = await self._record_finding(
                    binding,
                    comparison_run,
                    finding,
                    policy,
                    now,
                )
                projection = self._more_actionable_projection(projection, classification)
                if classification[0] is not HealthCause.REFERENCE:
                    known_projection = self._more_actionable_projection(
                        known_projection,
                        classification,
                    )
                finding_count += 1
            # The binding row is already ownership-locked in this transaction.
            # Keep its durable expiry ahead of a large but paged finding set so
            # a second worker cannot reclaim the binding when this commit lands.
            binding.lease_expires_at = self._now() + timedelta(
                seconds=settings.STRATEGY_MONITORING_BINDING_LEASE_SECONDS
            )
            if len(findings) < page_size:
                break
            cursor_created_at = self._aware_or_now(findings[-1].created_at, now)
            cursor_id = findings[-1].id

        self.db.add(
            StrategyMonitoringAssessment(
                comparison_run_id=comparison_run.run_id,
                binding_id=binding.id,
                activation_id=activation.id,
                reference_run_id=binding.reference_run_id,
                comparison_status=comparison_run.status,
                finding_count=finding_count,
                processor_worker_id=worker_id,
                processed_at=now,
            )
        )
        health.last_comparison_at = now
        window_end = self._aware_or_now(comparison_run.window_end, now)
        current_window = (
            health.last_complete_window_end is None
            or window_end >= self._aware_or_now(health.last_complete_window_end, now)
        )

        if comparison_run.status == "healthy":
            if finding_count:
                raise StrategyMonitoringError("healthy comparison cannot contain divergence findings")
            if not current_window:
                return
            health.last_complete_comparison_at = now
            health.last_complete_window_end = window_end
            await self._record_healthy_comparison(binding, health, policy, now)
            return
        if comparison_run.status == "insufficient_reference":
            # An incomplete comparison is never recovery evidence.  Keep the
            # coverage incident(s) from its findings and make the current
            # projection explicitly unknown rather than falsely healthy.
            if not current_window:
                return
            if known_projection is None:
                self._set_unknown(health)
            else:
                self._set_divergent_health(health, (known_projection,))
            health.last_error = "comparison coverage is incomplete"
            return
        if comparison_run.status != "divergent":
            raise StrategyMonitoringError(f"unsupported comparison status {comparison_run.status!r}")
        if not finding_count:
            raise StrategyMonitoringError("divergent comparison must contain at least one finding")
        assert projection is not None
        if not current_window:
            return
        health.last_complete_comparison_at = now
        health.last_complete_window_end = window_end
        self._set_divergent_health(health, (projection,))

    async def _record_finding(
        self,
        binding: StrategyMonitoringBinding,
        comparison_run: StrategyComparisonRun,
        finding: StrategyDivergenceFinding,
        policy: MonitoringPolicy,
        now: datetime,
    ) -> tuple[HealthCause, IncidentSeverity]:
        try:
            cause = cause_for_divergence(finding.divergence_type)
            comparison_severity = IncidentSeverity(finding.severity)
        except ValueError as exc:
            raise StrategyMonitoringError("comparison finding has unsupported monitoring classification") from exc

        evidence_result = await self.db.execute(
            select(StrategyIncidentEvidence)
            .where(StrategyIncidentEvidence.finding_id == finding.id)
            .with_for_update()
        )
        existing_evidence = evidence_result.scalars().first()
        if existing_evidence is not None:
            # Redelivery/restart sees the same immutable evidence.  It must not
            # increment count or change first/last detection timestamps.
            incident_result = await self.db.execute(
                select(StrategyIncident)
                .where(StrategyIncident.id == existing_evidence.incident_id)
                .with_for_update()
            )
            incident = incident_result.scalars().first()
            if incident is None:
                raise StrategyMonitoringError("incident evidence references a missing incident")
            return cause, IncidentSeverity(incident.severity)

        key = incident_dedup_key(binding=binding, finding=finding, cause=cause)
        incident_result = await self.db.execute(
            select(StrategyIncident)
            .where(StrategyIncident.dedup_key == key)
            .with_for_update()
        )
        incident = incident_result.scalars().first()
        # Lifecycle timestamps describe when monitoring detected a condition,
        # not when a potentially backfilled comparison row was created.  The
        # immutable evidence retains event/creation timestamps separately.
        detected_at = now
        if incident is None:
            severity = _max_severity(policy.severity_for(cause, 1), comparison_severity)
            incident = StrategyIncident(
                id=uuid.uuid4(),
                dedup_key=key,
                binding_id=binding.id,
                activation_id=binding.activation_id,
                reference_run_id=binding.reference_run_id,
                monitoring_policy_hash=binding.policy_hash,
                strategy_id=finding.strategy_id,
                strategy_version_id=finding.strategy_version_id,
                strategy_version_number=finding.strategy_version_number,
                strategy_definition_hash=finding.strategy_definition_hash,
                asset_id=finding.asset_id,
                divergence_type=finding.divergence_type,
                category=finding.category,
                cause=cause.value,
                severity=severity.value,
                status=IncidentLifecycle.OPEN.value,
                first_detected_at=detected_at,
                last_detected_at=detected_at,
                occurrence_count=1,
                latest_comparison_run_id=comparison_run.run_id,
                latest_finding_id=finding.id,
                source_reference=self._finding_source_reference(binding, comparison_run, finding),
                evidence_summary=self._finding_summary(cause, finding, policy),
            )
            self.db.add(incident)
        else:
            occurrence_count = incident.occurrence_count + 1
            policy_severity = policy.severity_for(cause, occurrence_count)
            severity = _max_severity(incident.severity, policy_severity, comparison_severity)
            incident.occurrence_count = occurrence_count
            # First detection is lifecycle history, not the earliest event
            # timestamp in a later-arriving forensic window.  It is immutable
            # by schema and deliberately remains the original monitor sighting.
            incident.last_detected_at = max(incident.last_detected_at, detected_at)
            incident.severity = severity.value
            incident.latest_comparison_run_id = comparison_run.run_id
            incident.latest_finding_id = finding.id
            incident.source_reference = self._finding_source_reference(binding, comparison_run, finding)
            incident.evidence_summary = self._finding_summary(cause, finding, policy)
            if incident.status == IncidentLifecycle.RESOLVED.value:
                # Keep the same logical incident and count so recurrence is
                # visible, while returning it to the actionable lifecycle.
                incident.status = IncidentLifecycle.OPEN.value
                incident.resolved_at = None
                incident.resolved_by = None
                incident.resolution_note = None

        self.db.add(
            StrategyIncidentEvidence(
                id=uuid.uuid4(),
                incident_id=incident.id,
                finding_id=finding.id,
                comparison_run_id=comparison_run.run_id,
                evidence_key=_hash_payload(
                    {"kind": EvidenceKind.DIVERGENCE_FINDING.value, "finding_id": str(finding.id)}
                ),
                evidence_kind=EvidenceKind.DIVERGENCE_FINDING.value,
                observed_at=detected_at,
                source_reference=self._finding_source_reference(binding, comparison_run, finding),
                payload={
                    "finding_id": str(finding.id),
                    "fingerprint": finding.fingerprint,
                    "event_timestamp": self._iso(finding.event_timestamp),
                    "finding_created_at": self._iso(finding.created_at),
                },
            )
        )
        return cause, IncidentSeverity(incident.severity)

    async def _record_healthy_comparison(
        self,
        binding: StrategyMonitoringBinding,
        health: StrategyHealthState,
        policy: MonitoringPolicy,
        now: datetime,
    ) -> None:
        health.consecutive_healthy_checks += 1
        health.last_healthy_at = now
        health.last_error = None
        if health.consecutive_healthy_checks < policy.recovery_healthy_checks:
            unresolved = await self._unresolved_incidents(binding.id, include_stale=False)
            if unresolved:
                recovery_count = health.consecutive_healthy_checks
                self._set_divergent_health(
                    health,
                    [(HealthCause(item.cause), IncidentSeverity(item.severity)) for item in unresolved],
                )
                # This is a complete healthy comparison, so it advances the
                # explicit recovery policy even while older incidents remain
                # actionable until the required count is reached.
                health.consecutive_healthy_checks = recovery_count
            else:
                self._set_healthy(health)
            return

        unresolved = await self._unresolved_incidents(binding.id, include_stale=False)
        for incident in unresolved:
            incident.status = IncidentLifecycle.RESOLVED.value
            incident.resolved_at = now
            incident.resolved_by = _RECOVERY_ACTOR
            incident.resolution_note = (
                f"Resolved after {health.consecutive_healthy_checks} complete healthy comparison check(s)."
            )
        self._set_healthy(health)

    async def _evaluate_staleness(
        self,
        *,
        binding: StrategyMonitoringBinding,
        activation: LiveStrategyActivation,
        health: StrategyHealthState,
        policy: MonitoringPolicy,
    ) -> None:
        now = self._now()
        health.last_evaluated_at = activation.last_evaluated_at
        baseline = activation.last_evaluated_at or activation.activated_at
        baseline = self._aware_or_now(baseline, now)
        stale = now - baseline >= timedelta(seconds=policy.stale_after_seconds)
        if stale:
            severity = await self._record_stale_state(
                binding=binding,
                activation=activation,
                policy=policy,
                baseline=baseline,
                now=now,
            )
            self._set_health(health, HealthStatus.STALE, HealthCause.RUNTIME, severity)
            health.last_error = (
                "live strategy runtime has not evaluated canonical data within the configured stale threshold"
            )
            return

        await self._recover_stale_state(binding.id, now)
        # A fresh evaluator timestamp proves runtime liveness but not strategy
        # equivalence.  Initial/no-comparison state remains unknown.
        if health.status == HealthStatus.STALE.value:
            self._set_unknown(health)
            health.last_error = None

    async def _evaluate_comparison_freshness(
        self,
        *,
        binding: StrategyMonitoringBinding,
        activation: LiveStrategyActivation,
        health: StrategyHealthState,
        policy: MonitoringPolicy,
    ) -> None:
        """Ensure an old healthy comparison cannot remain healthy forever.

        Fresh live evaluation proves only that the runtime is alive.  It does
        not prove that current live behavior still matches a reference, so
        comparison coverage has its own explicit freshness policy and incident.
        """

        now = self._now()
        baseline = health.last_complete_comparison_at or binding.registered_at
        baseline = self._aware_or_now(baseline, now)
        stale = now - baseline >= timedelta(seconds=policy.comparison_stale_after_seconds)
        if not stale:
            await self._recover_stale_comparison_evidence(binding.id, now)
            return

        severity = await self._record_stale_comparison_evidence(
            binding=binding,
            activation=activation,
            policy=policy,
            baseline=baseline,
            latest_window_end=health.last_complete_window_end,
            now=now,
        )
        current: tuple[HealthCause, IncidentSeverity] | None = None
        if health.cause is not None and health.severity is not None:
            current = (HealthCause(health.cause), IncidentSeverity(health.severity))
        projection = self._more_actionable_projection(
            current,
            (HealthCause.REFERENCE, severity),
        )
        self._set_divergent_health(health, (projection,))
        if health.last_error is None:
            health.last_error = (
                "no complete comparison evidence exists within the configured freshness threshold"
            )

    async def _record_stale_comparison_evidence(
        self,
        *,
        binding: StrategyMonitoringBinding,
        activation: LiveStrategyActivation,
        policy: MonitoringPolicy,
        baseline: datetime,
        latest_window_end: datetime | None,
        now: datetime,
    ) -> IncidentSeverity:
        key = _hash_payload(
            {
                "binding_id": str(binding.id),
                "reference_run_id": binding.reference_run_id,
                "comparison_policy_hash": binding.comparison_policy_hash,
                "monitoring_policy_hash": binding.policy_hash,
                "divergence_type": _STALE_COMPARISON_DIVERGENCE_TYPE,
                "category": _STALE_COMPARISON_CATEGORY,
            }
        )
        evidence_key = _hash_payload(
            {
                "binding_id": str(binding.id),
                "kind": EvidenceKind.STALE_COMPARISON_EVIDENCE.value,
                "last_complete_comparison_at": self._iso(baseline),
                "last_complete_window_end": self._iso(latest_window_end),
            }
        )
        incident_result = await self.db.execute(
            select(StrategyIncident)
            .where(StrategyIncident.dedup_key == key)
            .with_for_update()
        )
        incident = incident_result.scalars().first()
        if incident is None:
            severity = policy.severity_for(HealthCause.REFERENCE, 1)
            incident = StrategyIncident(
                id=uuid.uuid4(),
                dedup_key=key,
                binding_id=binding.id,
                activation_id=activation.id,
                reference_run_id=binding.reference_run_id,
                monitoring_policy_hash=binding.policy_hash,
                strategy_id=activation.strategy_id,
                strategy_version_id=activation.strategy_version_id,
                strategy_version_number=activation.strategy_version_number,
                strategy_definition_hash=activation.strategy_definition_hash,
                asset_id=None,
                divergence_type=_STALE_COMPARISON_DIVERGENCE_TYPE,
                category=_STALE_COMPARISON_CATEGORY,
                cause=HealthCause.REFERENCE.value,
                severity=severity.value,
                status=IncidentLifecycle.OPEN.value,
                first_detected_at=now,
                last_detected_at=now,
                occurrence_count=1,
                source_reference=self._stale_comparison_source_reference(
                    binding,
                    activation,
                    baseline,
                    latest_window_end,
                    policy,
                ),
                evidence_summary={
                    "cause": HealthCause.REFERENCE.value,
                    "divergence_type": _STALE_COMPARISON_DIVERGENCE_TYPE,
                    "comparison_stale_after_seconds": policy.comparison_stale_after_seconds,
                },
            )
            self.db.add(incident)
        else:
            evidence_result = await self.db.execute(
                select(StrategyIncidentEvidence)
                .where(
                    StrategyIncidentEvidence.incident_id == incident.id,
                    StrategyIncidentEvidence.evidence_key == evidence_key,
                )
                .with_for_update()
            )
            if evidence_result.scalars().first() is not None:
                return IncidentSeverity(incident.severity)
            occurrence_count = incident.occurrence_count + 1
            severity = _max_severity(
                incident.severity,
                policy.severity_for(HealthCause.REFERENCE, occurrence_count),
            )
            incident.occurrence_count = occurrence_count
            incident.last_detected_at = now
            incident.severity = severity.value
            incident.source_reference = self._stale_comparison_source_reference(
                binding,
                activation,
                baseline,
                latest_window_end,
                policy,
            )
            if incident.status == IncidentLifecycle.RESOLVED.value:
                incident.status = IncidentLifecycle.OPEN.value
                incident.resolved_at = None
                incident.resolved_by = None
                incident.resolution_note = None

        self.db.add(
            StrategyIncidentEvidence(
                id=uuid.uuid4(),
                incident_id=incident.id,
                finding_id=None,
                comparison_run_id=None,
                evidence_key=evidence_key,
                evidence_kind=EvidenceKind.STALE_COMPARISON_EVIDENCE.value,
                observed_at=now,
                source_reference=self._stale_comparison_source_reference(
                    binding,
                    activation,
                    baseline,
                    latest_window_end,
                    policy,
                ),
                payload={
                    "last_complete_comparison_at": self._iso(baseline),
                    "last_complete_window_end": self._iso(latest_window_end),
                    "comparison_stale_after_seconds": policy.comparison_stale_after_seconds,
                },
            )
        )
        return IncidentSeverity(incident.severity)

    async def _recover_stale_comparison_evidence(self, binding_id: uuid.UUID, now: datetime) -> None:
        result = await self.db.execute(
            select(StrategyIncident)
            .where(
                StrategyIncident.binding_id == binding_id,
                StrategyIncident.divergence_type == _STALE_COMPARISON_DIVERGENCE_TYPE,
                StrategyIncident.status.in_(
                    (IncidentLifecycle.OPEN.value, IncidentLifecycle.ACKNOWLEDGED.value)
                ),
            )
            .with_for_update()
        )
        for incident in result.scalars().all():
            incident.status = IncidentLifecycle.RESOLVED.value
            incident.resolved_at = now
            incident.resolved_by = _RECOVERY_ACTOR
            incident.resolution_note = "Complete comparison evidence resumed within the configured threshold."

    async def _record_stale_state(
        self,
        *,
        binding: StrategyMonitoringBinding,
        activation: LiveStrategyActivation,
        policy: MonitoringPolicy,
        baseline: datetime,
        now: datetime,
    ) -> IncidentSeverity:
        key = _hash_payload(
            {
                "binding_id": str(binding.id),
                "reference_run_id": binding.reference_run_id,
                "policy_hash": binding.policy_hash,
                "divergence_type": _STALE_DIVERGENCE_TYPE,
                "category": _STALE_CATEGORY,
            }
        )
        evidence_key = _hash_payload(
            {
                "binding_id": str(binding.id),
                "kind": EvidenceKind.STALE_STRATEGY_STATE.value,
                "last_evaluated_at": self._iso(baseline),
            }
        )
        incident_result = await self.db.execute(
            select(StrategyIncident)
            .where(StrategyIncident.dedup_key == key)
            .with_for_update()
        )
        incident = incident_result.scalars().first()
        if incident is None:
            severity = policy.severity_for(HealthCause.RUNTIME, 1)
            incident = StrategyIncident(
                id=uuid.uuid4(),
                dedup_key=key,
                binding_id=binding.id,
                activation_id=activation.id,
                reference_run_id=binding.reference_run_id,
                monitoring_policy_hash=binding.policy_hash,
                strategy_id=activation.strategy_id,
                strategy_version_id=activation.strategy_version_id,
                strategy_version_number=activation.strategy_version_number,
                strategy_definition_hash=activation.strategy_definition_hash,
                asset_id=None,
                divergence_type=_STALE_DIVERGENCE_TYPE,
                category=_STALE_CATEGORY,
                cause=HealthCause.RUNTIME.value,
                severity=severity.value,
                status=IncidentLifecycle.OPEN.value,
                first_detected_at=now,
                last_detected_at=now,
                occurrence_count=1,
                source_reference=self._stale_source_reference(binding, activation, baseline, policy),
                evidence_summary={
                    "cause": HealthCause.RUNTIME.value,
                    "divergence_type": _STALE_DIVERGENCE_TYPE,
                    "stale_after_seconds": policy.stale_after_seconds,
                },
            )
            self.db.add(incident)
        else:
            evidence_result = await self.db.execute(
                select(StrategyIncidentEvidence)
                .where(
                    StrategyIncidentEvidence.incident_id == incident.id,
                    StrategyIncidentEvidence.evidence_key == evidence_key,
                )
                .with_for_update()
            )
            if evidence_result.scalars().first() is not None:
                return IncidentSeverity(incident.severity)
            occurrence_count = incident.occurrence_count + 1
            severity = _max_severity(
                incident.severity,
                policy.severity_for(HealthCause.RUNTIME, occurrence_count),
            )
            incident.occurrence_count = occurrence_count
            incident.last_detected_at = now
            incident.severity = severity.value
            incident.source_reference = self._stale_source_reference(binding, activation, baseline, policy)
            if incident.status == IncidentLifecycle.RESOLVED.value:
                incident.status = IncidentLifecycle.OPEN.value
                incident.resolved_at = None
                incident.resolved_by = None
                incident.resolution_note = None

        self.db.add(
            StrategyIncidentEvidence(
                id=uuid.uuid4(),
                incident_id=incident.id,
                finding_id=None,
                comparison_run_id=None,
                evidence_key=evidence_key,
                evidence_kind=EvidenceKind.STALE_STRATEGY_STATE.value,
                observed_at=now,
                source_reference=self._stale_source_reference(binding, activation, baseline, policy),
                payload={
                    "last_evaluated_at": self._iso(baseline),
                    "stale_after_seconds": policy.stale_after_seconds,
                },
            )
        )
        return IncidentSeverity(incident.severity)

    async def _recover_stale_state(self, binding_id: uuid.UUID, now: datetime) -> None:
        incidents_result = await self.db.execute(
            select(StrategyIncident)
            .where(
                StrategyIncident.binding_id == binding_id,
                StrategyIncident.divergence_type == _STALE_DIVERGENCE_TYPE,
                StrategyIncident.status.in_(
                    (IncidentLifecycle.OPEN.value, IncidentLifecycle.ACKNOWLEDGED.value)
                ),
            )
            .with_for_update()
        )
        for incident in incidents_result.scalars().all():
            incident.status = IncidentLifecycle.RESOLVED.value
            incident.resolved_at = now
            incident.resolved_by = _RECOVERY_ACTOR
            incident.resolution_note = "Live evaluator resumed within the configured stale threshold."

    async def _unresolved_incidents(
        self,
        binding_id: uuid.UUID,
        *,
        include_stale: bool,
    ) -> list[StrategyIncident]:
        filters = [
            StrategyIncident.binding_id == binding_id,
            StrategyIncident.status.in_((IncidentLifecycle.OPEN.value, IncidentLifecycle.ACKNOWLEDGED.value)),
        ]
        if not include_stale:
            filters.append(StrategyIncident.divergence_type != _STALE_DIVERGENCE_TYPE)
        result = await self.db.execute(select(StrategyIncident).where(*filters).with_for_update())
        return list(result.scalars().all())

    async def _locked_incident(self, incident_id: uuid.UUID) -> StrategyIncident | None:
        result = await self.db.execute(
            select(StrategyIncident)
            .where(StrategyIncident.id == incident_id)
            .with_for_update()
        )
        return result.scalars().first()

    @staticmethod
    def _clear_claim(binding: StrategyMonitoringBinding) -> None:
        binding.lease_worker_id = None
        binding.lease_token = None
        binding.lease_expires_at = None
        binding.claimed_at = None

    @staticmethod
    def _set_unknown(state: StrategyHealthState) -> None:
        state.status = HealthStatus.UNKNOWN.value
        state.cause = None
        state.severity = None
        state.consecutive_healthy_checks = 0

    @staticmethod
    def _set_healthy(state: StrategyHealthState) -> None:
        state.status = HealthStatus.HEALTHY.value
        state.cause = None
        state.severity = None

    @staticmethod
    def _set_health(
        state: StrategyHealthState,
        status: HealthStatus,
        cause: HealthCause,
        severity: IncidentSeverity,
    ) -> None:
        state.status = status.value
        state.cause = cause.value
        state.severity = severity.value
        state.consecutive_healthy_checks = 0

    def _set_divergent_health(
        self,
        state: StrategyHealthState,
        classifications: Iterable[tuple[HealthCause, IncidentSeverity]],
    ) -> None:
        values = list(classifications)
        if not values:
            self._set_unknown(state)
            return
        # Severity is the primary health projection; for a tie select the
        # evidence class that most directly represents a runtime/definition
        # fault.  Market-data and execution facts deliberately remain their
        # own causes even when their severity is critical.
        cause, severity = max(values, key=self._projection_sort_key)
        status = (
            HealthStatus.ERROR
            if cause in {HealthCause.STRATEGY_FAILURE, HealthCause.RUNTIME}
            else HealthStatus.DEGRADED
        )
        self._set_health(state, status, cause, severity)

    @staticmethod
    def _projection_sort_key(item: tuple[HealthCause, IncidentSeverity]) -> tuple[int, int]:
        cause, severity = item
        cause_rank = {
            HealthCause.STRATEGY_FAILURE: 5,
            HealthCause.RUNTIME: 4,
            HealthCause.STRATEGY_BEHAVIOR: 3,
            HealthCause.EXECUTION: 2,
            HealthCause.DATA_QUALITY: 1,
            HealthCause.REFERENCE: 0,
        }
        return _severity_rank(severity), cause_rank[cause]

    def _more_actionable_projection(
        self,
        current: tuple[HealthCause, IncidentSeverity] | None,
        candidate: tuple[HealthCause, IncidentSeverity],
    ) -> tuple[HealthCause, IncidentSeverity]:
        return candidate if current is None or self._projection_sort_key(candidate) > self._projection_sort_key(current) else current

    @staticmethod
    def _aware_or_now(value: datetime | None, now: datetime) -> datetime:
        if value is None:
            return now
        if value.tzinfo is None or value.utcoffset() is None:
            raise StrategyMonitoringError("persisted monitoring timestamps must be timezone-aware")
        return value.astimezone(timezone.utc)

    @staticmethod
    def _iso(value: datetime | None) -> str | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise StrategyMonitoringError("timestamp must be timezone-aware")
        return value.astimezone(timezone.utc).isoformat()

    @staticmethod
    def _finding_source_reference(
        binding: StrategyMonitoringBinding,
        comparison_run: StrategyComparisonRun,
        finding: StrategyDivergenceFinding,
    ) -> dict[str, Any]:
        return {
            "binding_id": str(binding.id),
            "reference_run_id": binding.reference_run_id,
            "comparison_run_id": comparison_run.run_id,
            "finding_id": str(finding.id),
            "forensic_source": finding.source_reference,
        }

    @staticmethod
    def _finding_summary(
        cause: HealthCause,
        finding: StrategyDivergenceFinding,
        policy: MonitoringPolicy,
    ) -> dict[str, Any]:
        return {
            "cause": cause.value,
            "divergence_type": finding.divergence_type,
            "category": finding.category,
            "asset_id": finding.asset_id,
            "monitoring_policy_id": policy.policy_id,
            "monitoring_policy_revision": policy.revision,
            "expected": finding.expected_value,
            "observed": finding.observed_value,
        }

    @staticmethod
    def _stale_source_reference(
        binding: StrategyMonitoringBinding,
        activation: LiveStrategyActivation,
        baseline: datetime,
        policy: MonitoringPolicy,
    ) -> dict[str, Any]:
        return {
            "binding_id": str(binding.id),
            "reference_run_id": binding.reference_run_id,
            "activation_id": str(activation.id),
            "last_evaluated_at": baseline.isoformat(),
            "stale_after_seconds": policy.stale_after_seconds,
        }

    @staticmethod
    def _stale_comparison_source_reference(
        binding: StrategyMonitoringBinding,
        activation: LiveStrategyActivation,
        baseline: datetime,
        latest_window_end: datetime | None,
        policy: MonitoringPolicy,
    ) -> dict[str, Any]:
        return {
            "binding_id": str(binding.id),
            "reference_run_id": binding.reference_run_id,
            "comparison_policy_hash": binding.comparison_policy_hash,
            "activation_id": str(activation.id),
            "last_complete_comparison_at": baseline.isoformat(),
            "last_complete_window_end": StrategyMonitoringService._iso(latest_window_end),
            "comparison_stale_after_seconds": policy.comparison_stale_after_seconds,
        }


class StrategyComparisonProducerService:
    """Bounded producer of durable comparison evidence for active bindings.

    It reuses the binding's token-checked database lease rather than inventing
    a parallel ownership mechanism. The producer and monitor therefore never
    mutate the same binding concurrently; duplicate message delivery is also
    harmless because deterministic comparison-run persistence is idempotent.
    """

    @classmethod
    async def process_active(
        cls,
        session_factory: async_sessionmaker[AsyncSession],
        binding_limit: int | None = None,
    ) -> int:
        limit = binding_limit or settings.STRATEGY_MONITORING_MAX_BINDINGS_PER_CYCLE
        async with session_factory() as list_session:
            result = await list_session.execute(
                select(StrategyMonitoringBinding.id)
                .where(StrategyMonitoringBinding.status == MonitoringBindingStatus.ACTIVE.value)
                .order_by(
                    StrategyMonitoringBinding.last_processed_at.asc().nullsfirst(),
                    StrategyMonitoringBinding.registered_at.asc(),
                    StrategyMonitoringBinding.id.asc(),
                )
                .limit(limit)
            )
            binding_ids = [row[0] for row in result.fetchall()]

        worker_id = f"strategy-comparison-producer-{uuid.uuid4()}"
        produced = 0
        for binding_id in binding_ids:
            async with session_factory() as session:
                coordinator = StrategyMonitoringService(session)
                claim = await coordinator._claim_binding(binding_id, worker_id=worker_id)
                if claim is None:
                    continue
                try:
                    if await coordinator._produce_next_comparison(claim):
                        produced += 1
                except Exception as exc:
                    # A bad binding must not stop later active bindings. The
                    # failure is durable incident evidence immediately; merely
                    # logging it would leave a previously healthy projection
                    # in place until a later stale check.
                    logger.exception("Unable to produce comparison for binding %s", binding_id)
                    await coordinator._record_binding_error(claim, exc)
                finally:
                    await coordinator._release_claim(claim)
        return produced
