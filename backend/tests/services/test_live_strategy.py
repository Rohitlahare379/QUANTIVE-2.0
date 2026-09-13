"""Live-strategy observation tests at the persistence and pure-evaluator boundary."""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from app.live_strategies.contracts import (
    LiveCandle,
    LiveEvaluationStatus,
    LiveStrategyActivationInput,
    LiveStrategyActivationStatus,
    LiveStrategyConfiguration,
    LiveStrategyHealth,
    PositionState,
    evaluate_live_candle,
    validate_live_definition,
)
from app.models.live_strategy import LiveStrategyActivation, LiveStrategyObservation, LiveStrategyState
from app.models.strategy import Strategy, StrategyVersion
from app.services.exceptions import LiveStrategyActivationError
from app.services.live_strategy import LiveStrategyEvaluatorService
from app.strategies.contracts import CanonicalStrategyDefinition, InstrumentSnapshot, StrategyDefinitionInput


UTC = timezone.utc
START = datetime(2026, 1, 1, tzinfo=UTC)


def _definition(*, entry_threshold: float = 100, exit_threshold: float = 90) -> CanonicalStrategyDefinition:
    strategy_input = StrategyDefinitionInput.model_validate(
        {
            "timeframe": "1h",
            "universe_asset_ids": [2, 1],
            "parameters": {"entry_threshold": entry_threshold, "exit_threshold": exit_threshold},
            "signals": [
                {
                    "signal_id": "enter-long",
                    "role": "entry",
                    "direction": "long",
                    "priority": 100,
                    "definition": {"kind": "close_threshold", "operator": "gt", "threshold": entry_threshold},
                },
                {
                    "signal_id": "exit-long",
                    "role": "exit",
                    "direction": "flat",
                    "priority": 200,
                    "definition": {"kind": "close_threshold", "operator": "lt", "threshold": exit_threshold},
                },
            ],
            "position": {"mode": "long_only", "max_open_positions": 1, "sizing": {"kind": "unit"}},
            "execution": {"timing": "bar_close", "order_type": "market", "fee_bps": 0, "slippage_bps": 0},
            "implementation": {
                "kind": "declarative",
                "identifier": "threshold-v1",
                "revision": "2026-09-13",
            },
        }
    )
    return CanonicalStrategyDefinition.from_input(
        strategy_input,
        {
            1: InstrumentSnapshot(asset_id=1, symbol="BTCUSDT", exchange="BINANCE", asset_type="SPOT"),
            2: InstrumentSnapshot(asset_id=2, symbol="ETHUSDT", exchange="BINANCE", asset_type="SPOT"),
        },
    )


def _configuration(**overrides) -> LiveStrategyConfiguration:
    payload = {
        "evaluator_id": "threshold-v1",
        "evaluator_revision": "2026-09-13",
        "max_candles_per_cycle": 10,
        "runtime_config": {"bar_source": "canonical"},
    }
    payload.update(overrides)
    return LiveStrategyConfiguration.model_validate(payload)


def _candle(*, timestamp: datetime = START, asset_id: int = 1, close: float = 101) -> LiveCandle:
    return LiveCandle(
        asset_id=asset_id,
        timestamp=timestamp,
        open=100,
        high=max(101, close),
        low=min(99, close),
        close=close,
        volume=5,
    )


def _transactional_db():
    db = AsyncMock()
    transaction = AsyncMock()
    transaction.__aenter__.return_value = None
    transaction.__aexit__.return_value = None
    db.begin = MagicMock(return_value=transaction)
    db.begin_nested = MagicMock(return_value=transaction)
    db.add = MagicMock()
    db.flush = AsyncMock()
    return db


def _first_result(value):
    result = MagicMock()
    result.first.return_value = value
    return result


def _scalar_first_result(value):
    result = MagicMock()
    result.scalars.return_value.first.return_value = value
    return result


def _strategy_version(definition: CanonicalStrategyDefinition | None = None):
    definition = definition or _definition()
    strategy = Strategy(id=uuid.uuid4(), strategy_key="threshold-observer", name="Threshold observer")
    version = StrategyVersion(
        id=uuid.uuid4(),
        strategy_id=strategy.id,
        version_number=1,
        schema_version="1",
        definition_hash=definition.definition_hash(),
        canonical_definition=definition.canonical_payload(),
    )
    return strategy, version, definition


def _activation(
    strategy: Strategy,
    version: StrategyVersion,
    configuration: LiveStrategyConfiguration | None = None,
    open_position_count: int = 0,
):
    configuration = configuration or _configuration()
    return LiveStrategyActivation(
        id=uuid.uuid4(),
        strategy_id=strategy.id,
        strategy_version_id=version.id,
        strategy_version_number=version.version_number,
        strategy_definition_hash=version.definition_hash,
        configuration=configuration.model_dump(mode="json"),
        configuration_hash=configuration.configuration_hash(),
        status=LiveStrategyActivationStatus.ACTIVE.value,
        health=LiveStrategyHealth.HEALTHY.value,
        activated_at=START,
        open_position_count=open_position_count,
    )


def _state(activation: LiveStrategyActivation, *, last_candle: datetime | None = None, position="flat"):
    return LiveStrategyState(
        activation_id=activation.id,
        asset_id=1,
        last_candle_timestamp=last_candle,
        position_state=position,
        health=LiveStrategyHealth.HEALTHY.value,
        consecutive_errors=0,
    )


def test_live_configuration_identity_and_signal_generation_are_deterministic():
    definition = _definition()
    first = evaluate_live_candle(definition, _candle(), position_state=PositionState.FLAT, last_candle_timestamp=None)
    second = evaluate_live_candle(definition, _candle(), position_state=PositionState.FLAT, last_candle_timestamp=None)

    assert _configuration(runtime_config={"b": 2, "a": 1}).configuration_hash() == _configuration(
        runtime_config={"a": 1, "b": 2}
    ).configuration_hash()
    assert first == second
    assert first.status is LiveEvaluationStatus.EVALUATED
    assert first.position_after is PositionState.LONG
    assert [event.event_type.value for event in first.events] == ["signal", "entry"]


def test_repeated_duplicate_and_out_of_order_candles_never_emit_new_events():
    definition = _definition()
    duplicate = evaluate_live_candle(
        definition, _candle(), position_state=PositionState.LONG, last_candle_timestamp=START
    )
    stale = evaluate_live_candle(
        definition,
        _candle(timestamp=START - timedelta(hours=1)),
        position_state=PositionState.LONG,
        last_candle_timestamp=START,
    )

    assert duplicate.status is LiveEvaluationStatus.DUPLICATE
    assert stale.status is LiveEvaluationStatus.OUT_OF_ORDER
    assert duplicate.events == stale.events == ()
    assert duplicate.position_after is stale.position_after is PositionState.LONG


def test_missing_candle_marks_a_gap_without_advancing_position():
    outcome = evaluate_live_candle(
        _definition(),
        _candle(timestamp=START + timedelta(hours=2)),
        position_state=PositionState.LONG,
        last_candle_timestamp=START,
    )

    assert outcome.status is LiveEvaluationStatus.GAP_DETECTED
    assert outcome.error_code == "missing_candles"
    assert outcome.position_after is PositionState.LONG


def test_position_limit_preserves_signal_but_prevents_an_untracked_entry():
    outcome = evaluate_live_candle(
        _definition(),
        _candle(),
        position_state=PositionState.FLAT,
        last_candle_timestamp=None,
        can_open_position=False,
    )

    assert outcome.status is LiveEvaluationStatus.EVALUATED
    assert outcome.position_after is PositionState.FLAT
    assert [event.event_type.value for event in outcome.events] == ["signal"]


def test_invalid_live_configuration_and_invalid_candle_are_rejected():
    with pytest.raises(ValueError, match="implementation revision"):
        validate_live_definition(
            _definition(),
            _configuration(evaluator_revision="other-revision"),
        )
    with pytest.raises(ValidationError, match="timezone-aware"):
        _candle(timestamp=datetime(2026, 1, 1))

    invalid_asset = evaluate_live_candle(
        _definition(), _candle(asset_id=999), position_state=PositionState.FLAT, last_candle_timestamp=None
    )
    assert invalid_asset.status is LiveEvaluationStatus.INVALID
    assert invalid_asset.error_code == "asset_outside_universe"

    misaligned = evaluate_live_candle(
        _definition(),
        _candle(timestamp=START + timedelta(minutes=30)),
        position_state=PositionState.FLAT,
        last_candle_timestamp=None,
    )
    assert misaligned.status is LiveEvaluationStatus.INVALID
    assert misaligned.error_code == "timestamp_not_aligned"


@pytest.mark.asyncio
async def test_activation_persists_explicit_version_binding_and_per_asset_recovery_state():
    strategy, version, definition = _strategy_version()
    db = _transactional_db()
    db.execute.side_effect = [_first_result((strategy, version)), _scalar_first_result(None)]

    activation = await LiveStrategyEvaluatorService(db).activate(
        LiveStrategyActivationInput(strategy_version_id=version.id, configuration=_configuration())
    )

    added = [call.args[0] for call in db.add.call_args_list]
    assert activation.strategy_id == strategy.id
    assert activation.strategy_version_id == version.id
    assert activation.strategy_version_number == version.version_number
    assert activation.strategy_definition_hash == definition.definition_hash()
    assert activation.configuration_hash == _configuration().configuration_hash()
    assert activation.health == LiveStrategyHealth.UNKNOWN.value
    assert sorted(row.asset_id for row in added[1:]) == [1, 2]
    assert all(row.position_state == "flat" for row in added[1:])
    assert all(row.health == LiveStrategyHealth.UNKNOWN.value for row in added[1:])
    db.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_activation_rejects_invalid_version_configuration_before_writing():
    strategy, version, _ = _strategy_version()
    db = _transactional_db()
    db.execute.return_value = _first_result((strategy, version))

    with pytest.raises(LiveStrategyActivationError, match="revision"):
        await LiveStrategyEvaluatorService(db).activate(
            LiveStrategyActivationInput(
                strategy_version_id=version.id,
                configuration=_configuration(evaluator_revision="wrong"),
            )
        )

    db.add.assert_not_called()


@pytest.mark.asyncio
async def test_deactivation_stops_the_runtime_without_erasing_forensic_state():
    strategy, version, _ = _strategy_version()
    activation = _activation(strategy, version)
    db = _transactional_db()
    db.execute.side_effect = [_scalar_first_result(activation), MagicMock()]

    deactivated = await LiveStrategyEvaluatorService(db).deactivate(activation.id)

    assert deactivated is True
    assert activation.status == LiveStrategyActivationStatus.DEACTIVATED.value
    assert activation.health == LiveStrategyHealth.STOPPED.value
    assert activation.deactivated_at is not None
    assert db.execute.await_count == 2


@pytest.mark.asyncio
async def test_persistence_records_signal_entry_latency_and_updates_recoverable_state():
    strategy, version, definition = _strategy_version()
    activation = _activation(strategy, version)
    state = _state(activation)
    db = AsyncMock()
    db.add = MagicMock()
    db.execute.return_value = _first_result(None)

    persisted = await LiveStrategyEvaluatorService(db)._evaluate_and_persist(
        activation, state, definition, _candle()
    )

    rows = [call.args[0] for call in db.add.call_args_list]
    observation = rows[0]
    assert persisted is True
    assert isinstance(observation, LiveStrategyObservation)
    assert observation.status == LiveEvaluationStatus.EVALUATED.value
    assert observation.evaluation_latency_ms >= 0
    assert [row.event_type for row in rows[1:]] == ["signal", "entry"]
    assert state.last_candle_timestamp == START
    assert state.position_state == PositionState.LONG.value
    assert activation.open_position_count == 1
    assert state.health == LiveStrategyHealth.HEALTHY.value


@pytest.mark.asyncio
async def test_existing_observation_makes_delivery_idempotent_without_changing_state():
    strategy, version, definition = _strategy_version()
    activation = _activation(strategy, version)
    state = _state(activation)
    db = AsyncMock()
    db.add = MagicMock()
    db.execute.return_value = _first_result(uuid.uuid4())

    persisted = await LiveStrategyEvaluatorService(db)._evaluate_and_persist(
        activation, state, definition, _candle()
    )

    assert persisted is False
    assert state.last_candle_timestamp is None
    assert state.position_state == PositionState.FLAT.value
    db.add.assert_not_called()


@pytest.mark.asyncio
async def test_repaired_gap_replays_as_a_new_immutable_attempt_and_unblocks_the_cursor():
    """A repaired earlier candle must not leave a later gap observation stuck forever."""
    strategy, version, definition = _strategy_version()
    activation = _activation(strategy, version, open_position_count=1)
    activation.health = LiveStrategyHealth.DEGRADED.value
    state = _state(activation, last_candle=START + timedelta(hours=1), position="long")
    state.health = LiveStrategyHealth.DEGRADED.value
    prior_gap = MagicMock(status=LiveEvaluationStatus.GAP_DETECTED.value, evaluation_attempt=1)
    health_rows = MagicMock()
    health_rows.all.return_value = [(LiveStrategyHealth.HEALTHY.value, START + timedelta(hours=2))]
    db = AsyncMock()
    db.add = MagicMock()
    db.execute.side_effect = [_first_result(prior_gap), health_rows]

    persisted = await LiveStrategyEvaluatorService(db)._evaluate_and_persist(
        activation,
        state,
        definition,
        _candle(timestamp=START + timedelta(hours=2), close=89),
    )

    observation = db.add.call_args_list[0].args[0]
    assert persisted is True
    assert observation.status == LiveEvaluationStatus.EVALUATED.value
    assert observation.evaluation_attempt == 2
    assert state.last_candle_timestamp == START + timedelta(hours=2)
    assert state.health == LiveStrategyHealth.HEALTHY.value
    assert activation.health == LiveStrategyHealth.HEALTHY.value


@pytest.mark.asyncio
async def test_unrepaired_gap_does_not_append_unbounded_identical_observations():
    strategy, version, definition = _strategy_version()
    activation = _activation(strategy, version, open_position_count=1)
    state = _state(activation, last_candle=START, position="long")
    prior_gap = MagicMock(status=LiveEvaluationStatus.GAP_DETECTED.value, evaluation_attempt=1)
    db = AsyncMock()
    db.add = MagicMock()
    db.execute.return_value = _first_result(prior_gap)

    persisted = await LiveStrategyEvaluatorService(db)._evaluate_and_persist(
        activation, state, definition, _candle(timestamp=START + timedelta(hours=2))
    )

    assert persisted is False
    assert state.last_candle_timestamp == START
    db.add.assert_not_called()


@pytest.mark.asyncio
async def test_concurrent_activation_first_writer_race_returns_the_existing_runtime():
    from sqlalchemy.exc import IntegrityError

    strategy, version, _ = _strategy_version()
    configuration = _configuration()
    raced = _activation(strategy, version, configuration)
    db = _transactional_db()
    db.execute.side_effect = [
        _first_result((strategy, version)),
        _scalar_first_result(None),
        _scalar_first_result(raced),
    ]
    db.flush.side_effect = IntegrityError("duplicate activation", {}, Exception("unique"))

    activation = await LiveStrategyEvaluatorService(db).activate(
        LiveStrategyActivationInput(strategy_version_id=version.id, configuration=configuration)
    )

    assert activation is raced
    db.begin_nested.assert_called_once()


@pytest.mark.asyncio
async def test_state_recovery_after_strategy_and_process_restart_uses_durable_cursor():
    strategy, version, definition = _strategy_version()
    activation = _activation(strategy, version, open_position_count=1)
    state = _state(activation, last_candle=START, position="long")
    db = AsyncMock()
    db.add = MagicMock()
    db.execute.return_value = _first_result(None)

    # A new service instance represents a process restart; it uses the persisted state row.
    duplicate = await LiveStrategyEvaluatorService(db)._evaluate_and_persist(
        activation, state, definition, _candle()
    )
    recovered = await LiveStrategyEvaluatorService(db)._evaluate_and_persist(
        activation,
        state,
        definition,
        _candle(timestamp=START + timedelta(hours=1), close=89),
    )

    assert duplicate is False
    assert recovered is True
    assert state.last_candle_timestamp == START + timedelta(hours=1)
    assert state.position_state == PositionState.FLAT.value
    assert activation.open_position_count == 0
    event_rows = [call.args[0] for call in db.add.call_args_list if hasattr(call.args[0], "event_type")]
    assert [row.event_type for row in event_rows] == ["signal", "exit"]


@pytest.mark.asyncio
async def test_gap_observation_degrades_runtime_health_and_records_error():
    strategy, version, definition = _strategy_version()
    activation = _activation(strategy, version, open_position_count=1)
    state = _state(activation, last_candle=START, position="long")
    db = AsyncMock()
    db.add = MagicMock()
    db.execute.return_value = _first_result(None)

    persisted = await LiveStrategyEvaluatorService(db)._evaluate_and_persist(
        activation,
        state,
        definition,
        _candle(timestamp=START + timedelta(hours=2)),
    )

    observation = db.add.call_args_list[0].args[0]
    assert persisted is True
    assert observation.status == LiveEvaluationStatus.GAP_DETECTED.value
    assert observation.error_code == "missing_candles"
    assert state.last_candle_timestamp == START
    assert state.health == LiveStrategyHealth.DEGRADED.value
    assert activation.health == LiveStrategyHealth.DEGRADED.value


@pytest.mark.asyncio
async def test_invalid_evaluation_records_error_without_advancing_the_durable_cursor():
    strategy, version, definition = _strategy_version()
    activation = _activation(strategy, version)
    state = _state(activation)
    db = AsyncMock()
    db.add = MagicMock()
    db.execute.return_value = _first_result(None)

    persisted = await LiveStrategyEvaluatorService(db)._evaluate_and_persist(
        activation,
        state,
        definition,
        _candle(timestamp=START + timedelta(minutes=30)),
    )

    observation = db.add.call_args_list[0].args[0]
    assert persisted is True
    assert observation.status == LiveEvaluationStatus.INVALID.value
    assert observation.error_code == "timestamp_not_aligned"
    assert state.last_candle_timestamp is None
    assert state.health == LiveStrategyHealth.ERROR.value
    assert activation.health == LiveStrategyHealth.ERROR.value


@pytest.mark.asyncio
async def test_batch_stops_after_a_non_progressing_gap_instead_of_buffering_later_errors(monkeypatch):
    strategy, version, definition = _strategy_version()
    activation = _activation(strategy, version, open_position_count=1)
    state = _state(activation, last_candle=START, position="long")
    db = _transactional_db()
    service = LiveStrategyEvaluatorService(db)
    candles = [
        _candle(timestamp=START + timedelta(hours=2)),
        _candle(timestamp=START + timedelta(hours=3)),
    ]
    evaluate = AsyncMock(return_value=True)

    monkeypatch.setattr(service, "_locked_active_activation", AsyncMock(return_value=activation))
    monkeypatch.setattr(service, "_load_strategy_version", AsyncMock(return_value=(strategy, version, definition)))
    monkeypatch.setattr(service, "_locked_state_slice", AsyncMock(return_value=[state]))
    monkeypatch.setattr(service, "_read_canonical_candles", AsyncMock(return_value=candles))
    monkeypatch.setattr(service, "_evaluate_and_persist", evaluate)

    evaluated = await service.process_activation(activation.id)

    assert evaluated == 0
    evaluate.assert_awaited_once_with(activation, state, definition, candles[0])


def test_multiple_strategy_versions_remain_state_isolated_for_the_same_canonical_candle():
    candle = _candle(close=150)
    first = evaluate_live_candle(
        _definition(entry_threshold=100), candle, position_state=PositionState.FLAT, last_candle_timestamp=None
    )
    second = evaluate_live_candle(
        _definition(entry_threshold=200), candle, position_state=PositionState.FLAT, last_candle_timestamp=None
    )

    assert first.position_after is PositionState.LONG
    assert second.position_after is PositionState.FLAT
    assert first.events != second.events


@pytest.mark.asyncio
async def test_active_processing_uses_a_fresh_session_for_each_strategy_runtime(monkeypatch):
    first_id, second_id = uuid.uuid4(), uuid.uuid4()
    list_session = AsyncMock()
    list_result = MagicMock()
    list_result.fetchall.return_value = [(first_id,), (second_id,)]
    list_session.execute.return_value = list_result
    evaluation_sessions = [AsyncMock(), AsyncMock()]
    sessions = [list_session, *evaluation_sessions]

    class _SessionContext:
        def __init__(self, session):
            self.session = session

        async def __aenter__(self):
            return self.session

        async def __aexit__(self, *args):
            return None

    class _SessionFactory:
        def __call__(self):
            return _SessionContext(sessions.pop(0))

    observed_sessions = []

    async def _process_activation(self, activation_id):
        observed_sessions.append((self.db, activation_id))
        return 1

    monkeypatch.setattr(LiveStrategyEvaluatorService, "process_activation", _process_activation)

    total = await LiveStrategyEvaluatorService.process_active(_SessionFactory(), activation_limit=2)

    assert total == 2
    assert observed_sessions == [(evaluation_sessions[0], first_id), (evaluation_sessions[1], second_id)]
    assert observed_sessions[0][0] is not observed_sessions[1][0]


@pytest.mark.asyncio
async def test_active_processing_isolates_one_runtime_failure_and_keeps_the_next_runtime_live(monkeypatch):
    first_id, second_id = uuid.uuid4(), uuid.uuid4()
    list_session = AsyncMock()
    list_result = MagicMock()
    list_result.fetchall.return_value = [(first_id,), (second_id,)]
    list_session.execute.return_value = list_result
    evaluation_sessions = [AsyncMock(), AsyncMock()]
    sessions = [list_session, *evaluation_sessions]

    class _Context:
        def __init__(self, session):
            self.session = session

        async def __aenter__(self):
            return self.session

        async def __aexit__(self, *args):
            return None

    class _Factory:
        def __call__(self):
            return _Context(sessions.pop(0))

    async def _process(self, activation_id):
        if activation_id == first_id:
            raise RuntimeError("corrupt runtime")
        return 3

    monkeypatch.setattr(LiveStrategyEvaluatorService, "process_activation", _process)
    total = await LiveStrategyEvaluatorService.process_active(_Factory(), activation_limit=2)

    assert total == 3
    assert "last_evaluated_at" in str(list_session.execute.call_args.args[0])
