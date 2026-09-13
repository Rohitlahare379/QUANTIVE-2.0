"""Bounded database consumer for live strategy observation.

This service intentionally reads only persisted canonical candles.  It imports
neither a Binance connector nor a WebSocket runtime and never attaches work to
the ingestion pipeline's in-memory queue.
"""

from __future__ import annotations

import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import settings
from app.live_strategies.contracts import (
    LiveCandle,
    LiveEvaluationOutcome,
    LiveEvaluationStatus,
    LiveStrategyActivationInput,
    LiveStrategyActivationStatus,
    LiveStrategyConfiguration,
    LiveStrategyHealth,
    PositionState,
    evaluate_live_candle,
    validate_live_definition,
)
from app.models.live_strategy import (
    LiveStrategyActivation,
    LiveStrategyEvent,
    LiveStrategyObservation,
    LiveStrategyState,
)
from app.models.strategy import Strategy, StrategyVersion
from app.services.exceptions import LiveStrategyActivationError, LiveStrategyError
from app.services.query import TIMEFRAME_TABLE_MAP
from app.strategies.contracts import CanonicalStrategyDefinition


logger = logging.getLogger(__name__)


class LiveStrategyEvaluatorService:
    """Evaluates activated strategy versions from canonical persisted candles."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def activate(self, activation_input: LiveStrategyActivationInput) -> LiveStrategyActivation:
        """Create an explicit runtime and durable per-asset state rows."""
        async with self.db.begin():
            strategy, version, definition = await self._load_strategy_version(activation_input.strategy_version_id)
            try:
                validate_live_definition(definition, activation_input.configuration)
            except ValueError as exc:
                raise LiveStrategyActivationError(str(exc)) from exc

            configuration_hash = activation_input.configuration.configuration_hash()
            existing_result = await self.db.execute(
                select(LiveStrategyActivation)
                .where(
                    LiveStrategyActivation.strategy_version_id == version.id,
                    LiveStrategyActivation.configuration_hash == configuration_hash,
                    LiveStrategyActivation.status == LiveStrategyActivationStatus.ACTIVE.value,
                )
                .with_for_update()
            )
            existing = existing_result.scalars().first()
            if existing is not None:
                return existing

            now = datetime.now(timezone.utc)
            activation = LiveStrategyActivation(
                id=uuid.uuid4(),
                strategy_id=strategy.id,
                strategy_version_id=version.id,
                strategy_version_number=version.version_number,
                strategy_definition_hash=version.definition_hash,
                configuration=activation_input.configuration.model_dump(mode="json"),
                configuration_hash=configuration_hash,
                status=LiveStrategyActivationStatus.ACTIVE.value,
                # Activation is not evidence of correct live behavior.
                health=LiveStrategyHealth.UNKNOWN.value,
                activated_at=now,
                open_position_count=0,
            )
            try:
                # An absent-row lookup cannot reserve the partial-unique
                # active binding.  Resolve a concurrent first activation as
                # the same idempotent runtime, not a raw IntegrityError.
                async with self.db.begin_nested():
                    self.db.add(activation)
                    for instrument in definition.universe:
                        self.db.add(
                            LiveStrategyState(
                                activation_id=activation.id,
                                asset_id=instrument.asset_id,
                                position_state=PositionState.FLAT.value,
                                health=LiveStrategyHealth.UNKNOWN.value,
                                consecutive_errors=0,
                            )
                        )
                    await self.db.flush()
            except IntegrityError:
                raced_result = await self.db.execute(
                    select(LiveStrategyActivation)
                    .where(
                        LiveStrategyActivation.strategy_version_id == version.id,
                        LiveStrategyActivation.configuration_hash == configuration_hash,
                        LiveStrategyActivation.status == LiveStrategyActivationStatus.ACTIVE.value,
                    )
                    .with_for_update()
                )
                raced = raced_result.scalars().first()
                if raced is None:
                    raise
                return raced
            return activation

    async def deactivate(self, activation_id: uuid.UUID) -> bool:
        """Stop future polling while retaining immutable observations and state."""
        async with self.db.begin():
            result = await self.db.execute(
                select(LiveStrategyActivation)
                .where(LiveStrategyActivation.id == activation_id)
                .with_for_update()
            )
            activation = result.scalars().first()
            if activation is None:
                return False
            if activation.status != LiveStrategyActivationStatus.ACTIVE.value:
                return True

            now = datetime.now(timezone.utc)
            activation.status = LiveStrategyActivationStatus.DEACTIVATED.value
            activation.health = LiveStrategyHealth.STOPPED.value
            activation.deactivated_at = now
            await self.db.execute(
                update(LiveStrategyState)
                .where(LiveStrategyState.activation_id == activation_id)
                .values(health=LiveStrategyHealth.STOPPED.value)
            )
            return True

    async def process_activation(self, activation_id: uuid.UUID) -> int:
        """Process one bounded canonical-candle batch for one active runtime."""
        async with self.db.begin():
            activation = await self._locked_active_activation(activation_id)
            if activation is None:
                return 0

            try:
                configuration = LiveStrategyConfiguration.model_validate(activation.configuration)
                _, version, definition = await self._load_strategy_version(activation.strategy_version_id)
                if version.definition_hash != activation.strategy_definition_hash:
                    raise ValueError("activation strategy definition hash no longer matches its version")
                validate_live_definition(definition, configuration)
            except (ValueError, LiveStrategyError) as exc:
                activation.status = LiveStrategyActivationStatus.ERROR.value
                activation.health = LiveStrategyHealth.ERROR.value
                activation.last_error = str(exc)
                return 0

            asset_limit = settings.LIVE_STRATEGY_MAX_ASSETS_PER_CYCLE
            states = await self._locked_state_slice(activation, asset_limit)
            if not states:
                return 0
            activation.asset_cursor = states[-1].asset_id

            candle_limit = min(
                configuration.max_candles_per_cycle,
                settings.LIVE_STRATEGY_MAX_CANDLES_PER_ASSET_CYCLE,
            )
            evaluated = 0
            for state in states:
                candles = await self._read_canonical_candles(
                    asset_id=state.asset_id,
                    timeframe=definition.timeframe.value,
                    after_timestamp=state.last_candle_timestamp or activation.activated_at,
                    limit=candle_limit,
                )
                for candle in candles:
                    persisted = await self._evaluate_and_persist(activation, state, definition, candle)
                    if persisted and state.last_candle_timestamp == candle.timestamp:
                        evaluated += 1
                    if state.last_candle_timestamp != candle.timestamp:
                        # Do not manufacture observations for later candles
                        # while an earlier gap/invalid evaluation is unresolved.
                        break
            return evaluated

    @classmethod
    async def process_active(
        cls,
        session_factory: async_sessionmaker[AsyncSession],
        activation_limit: Optional[int] = None,
    ) -> int:
        """Evaluate active runtimes sequentially using one fresh session each."""
        limit = activation_limit or settings.LIVE_STRATEGY_MAX_ACTIVATIONS_PER_CYCLE
        async with session_factory() as list_session:
            result = await list_session.execute(
                select(LiveStrategyActivation.id)
                .where(LiveStrategyActivation.status == LiveStrategyActivationStatus.ACTIVE.value)
                .order_by(
                    LiveStrategyActivation.last_evaluated_at.asc().nullsfirst(),
                    LiveStrategyActivation.created_at.asc(),
                    LiveStrategyActivation.id.asc(),
                )
                .limit(limit)
            )
            activation_ids = [row[0] for row in result.fetchall()]

        total = 0
        for activation_id in activation_ids:
            async with session_factory() as evaluation_session:
                try:
                    total += await cls(evaluation_session).process_activation(activation_id)
                except Exception:
                    # One corrupt runtime must not starve other active
                    # strategies in this bounded dispatch cycle.
                    logger.exception("Live strategy evaluation failed for activation %s", activation_id)
        return total

    async def _load_strategy_version(
        self,
        strategy_version_id: uuid.UUID,
    ) -> tuple[Strategy, StrategyVersion, CanonicalStrategyDefinition]:
        result = await self.db.execute(
            select(Strategy, StrategyVersion)
            .join(StrategyVersion, StrategyVersion.strategy_id == Strategy.id)
            .where(StrategyVersion.id == strategy_version_id)
        )
        row = result.first()
        if row is None:
            raise LiveStrategyActivationError(f"strategy version {strategy_version_id} not found")
        strategy, version = row
        try:
            definition = CanonicalStrategyDefinition.model_validate(version.canonical_definition)
        except ValueError as exc:
            raise LiveStrategyActivationError("stored strategy definition is invalid") from exc
        if definition.definition_hash() != version.definition_hash:
            raise LiveStrategyActivationError("stored strategy definition does not match its definition hash")
        return strategy, version, definition

    async def _locked_active_activation(
        self,
        activation_id: uuid.UUID,
    ) -> LiveStrategyActivation | None:
        result = await self.db.execute(
            select(LiveStrategyActivation)
            .where(
                LiveStrategyActivation.id == activation_id,
                LiveStrategyActivation.status == LiveStrategyActivationStatus.ACTIVE.value,
            )
            .with_for_update(skip_locked=True)
        )
        return result.scalars().first()

    async def _locked_state_slice(
        self,
        activation: LiveStrategyActivation,
        limit: int,
    ) -> list[LiveStrategyState]:
        statement = (
            select(LiveStrategyState)
            .where(LiveStrategyState.activation_id == activation.id)
            .order_by(LiveStrategyState.asset_id.asc())
            .limit(limit)
            .with_for_update()
        )
        if activation.asset_cursor is not None:
            statement = statement.where(LiveStrategyState.asset_id > activation.asset_cursor)
        result = await self.db.execute(statement)
        states = list(result.scalars().all())
        if not states and activation.asset_cursor is not None:
            activation.asset_cursor = None
            result = await self.db.execute(
                select(LiveStrategyState)
                .where(LiveStrategyState.activation_id == activation.id)
                .order_by(LiveStrategyState.asset_id.asc())
                .limit(limit)
                .with_for_update()
            )
            states = list(result.scalars().all())
        return states

    async def _read_canonical_candles(
        self,
        *,
        asset_id: int,
        timeframe: str,
        after_timestamp: datetime,
        limit: int,
    ) -> list[LiveCandle]:
        table = TIMEFRAME_TABLE_MAP[timeframe]
        result = await self.db.execute(
            select(
                table.c.asset_id,
                table.c.timestamp,
                table.c.open,
                table.c.high,
                table.c.low,
                table.c.close,
                table.c.volume,
            )
            .where(table.c.asset_id == asset_id, table.c.timestamp > after_timestamp)
            .order_by(table.c.timestamp.asc())
            .limit(limit)
        )
        candles: list[LiveCandle] = []
        for row in result.mappings().all():
            candles.append(LiveCandle.model_validate(dict(row)))
        return candles

    async def _evaluate_and_persist(
        self,
        activation: LiveStrategyActivation,
        state: LiveStrategyState,
        definition: CanonicalStrategyDefinition,
        candle: LiveCandle,
    ) -> bool:
        observed_at = datetime.now(timezone.utc)
        started_at = time.perf_counter()
        try:
            outcome = evaluate_live_candle(
                definition,
                candle,
                position_state=PositionState(state.position_state),
                last_candle_timestamp=state.last_candle_timestamp,
                can_open_position=(
                    state.position_state != PositionState.FLAT.value
                    or activation.open_position_count < definition.position.max_open_positions
                ),
            )
        except Exception as exc:  # Defensive boundary around untrusted persisted rows/configs.
            outcome = LiveEvaluationOutcome(
                status=LiveEvaluationStatus.ERROR,
                position_before=PositionState(state.position_state),
                position_after=PositionState(state.position_state),
                error_code="evaluation_error",
                error_message=str(exc),
            )
        latency_ms = (time.perf_counter() - started_at) * 1000.0

        if outcome.status in {LiveEvaluationStatus.DUPLICATE, LiveEvaluationStatus.OUT_OF_ORDER}:
            return False

        existing_result = await self.db.execute(
            select(
                LiveStrategyObservation.status,
                LiveStrategyObservation.evaluation_attempt,
            )
            .where(
                LiveStrategyObservation.activation_id == activation.id,
                LiveStrategyObservation.asset_id == candle.asset_id,
                LiveStrategyObservation.candle_timestamp == candle.timestamp,
            )
            .order_by(LiveStrategyObservation.evaluation_attempt.desc())
            .limit(1)
            .with_for_update()
        )
        previous = existing_result.first()
        attempt = 1
        if previous is not None:
            previous_status = getattr(previous, "status", None)
            previous_attempt = getattr(previous, "evaluation_attempt", None)
            if previous_status is None and isinstance(previous, tuple):
                previous_status, previous_attempt = previous
            # Legacy/simple test doubles that model the old id-only lookup are
            # conservatively a completed observation.
            if previous_status is None:
                previous_status = LiveEvaluationStatus.EVALUATED.value
            if previous_status == LiveEvaluationStatus.EVALUATED.value:
                return False
            # Do not append a new immutable failure on every poll.  A replay is
            # useful only when reconciliation/correction made state progress
            # possible at the same candle timestamp.
            if outcome.status is not LiveEvaluationStatus.EVALUATED:
                return False
            attempt = int(previous_attempt or 1) + 1

        observation = LiveStrategyObservation(
            id=uuid.uuid4(),
            activation_id=activation.id,
            asset_id=candle.asset_id,
            candle_timestamp=candle.timestamp,
            evaluation_attempt=attempt,
            observed_at=observed_at,
            status=outcome.status.value,
            signal_id=outcome.signal_id,
            position_before=outcome.position_before.value,
            position_after=outcome.position_after.value,
            evaluation_latency_ms=latency_ms,
            error_code=outcome.error_code,
            error_message=outcome.error_message,
        )
        self.db.add(observation)
        for event in outcome.events:
            self.db.add(
                LiveStrategyEvent(
                    id=uuid.uuid4(),
                    observation_id=observation.id,
                    activation_id=activation.id,
                    asset_id=candle.asset_id,
                    candle_timestamp=candle.timestamp,
                    event_time=candle.timestamp,
                    event_type=event.event_type.value,
                    signal_id=event.signal_id,
                    position_state=event.position_state.value,
                )
            )

        state.last_observed_at = observed_at
        if outcome.status is LiveEvaluationStatus.EVALUATED:
            # A gap or invalid candle must not move the durable cursor.  When
            # reconciliation later persists the missing valid candle, the
            # evaluator will read and apply it before this later candle.
            state.last_candle_timestamp = candle.timestamp
        if (
            outcome.status is LiveEvaluationStatus.EVALUATED
            and outcome.position_after is not outcome.position_before
        ):
            if outcome.position_before is PositionState.FLAT:
                activation.open_position_count += 1
            elif outcome.position_after is PositionState.FLAT:
                activation.open_position_count -= 1
            state.position_state = outcome.position_after.value
            state.position_changed_at = observed_at

        if outcome.status is LiveEvaluationStatus.EVALUATED:
            state.health = LiveStrategyHealth.HEALTHY.value
            state.consecutive_errors = 0
            state.last_error = None
            await self._refresh_activation_health(activation)
        elif outcome.status is LiveEvaluationStatus.GAP_DETECTED:
            state.health = LiveStrategyHealth.DEGRADED.value
            state.consecutive_errors += 1
            state.last_error = outcome.error_message
            activation.health = LiveStrategyHealth.DEGRADED.value
            activation.last_error = outcome.error_message
        else:
            state.health = LiveStrategyHealth.ERROR.value
            state.consecutive_errors += 1
            state.last_error = outcome.error_message
            activation.health = LiveStrategyHealth.ERROR.value
            activation.last_error = outcome.error_message

        if outcome.status is LiveEvaluationStatus.EVALUATED:
            activation.last_evaluated_at = observed_at
        return True

    async def _refresh_activation_health(self, activation: LiveStrategyActivation) -> None:
        """Project activation health from every durable state row.

        A recovered asset cannot erase another asset's gap/error, and a runtime
        with unobserved assets remains unknown rather than falsely healthy.
        """
        result = await self.db.execute(
            select(LiveStrategyState.health, LiveStrategyState.last_candle_timestamp)
            .where(LiveStrategyState.activation_id == activation.id)
        )
        rows = result.all()
        if not rows:
            activation.health = LiveStrategyHealth.UNKNOWN.value
            return
        def _value(row, attribute: str, index: int):
            try:
                return getattr(row, attribute)
            except AttributeError:
                return row[index]

        health_values = [_value(row, "health", 0) for row in rows]
        timestamps = [_value(row, "last_candle_timestamp", 1) for row in rows]
        if any(value == LiveStrategyHealth.ERROR.value for value in health_values):
            activation.health = LiveStrategyHealth.ERROR.value
            return
        if any(value == LiveStrategyHealth.DEGRADED.value for value in health_values):
            activation.health = LiveStrategyHealth.DEGRADED.value
            return
        if any(value == LiveStrategyHealth.UNKNOWN.value for value in health_values) or any(value is None for value in timestamps):
            activation.health = LiveStrategyHealth.UNKNOWN.value
            return
        activation.health = LiveStrategyHealth.HEALTHY.value
        activation.last_error = None
