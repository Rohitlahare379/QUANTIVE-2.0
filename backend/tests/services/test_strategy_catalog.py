"""Unit and service-boundary integration tests for the canonical strategy catalog."""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from app.models.strategy import Strategy, StrategyVersion
from app.services.exceptions import StrategyDefinitionError
from app.services.strategy_catalog import StrategyCatalogService
from app.strategies.contracts import (
    CanonicalStrategyDefinition,
    InstrumentSnapshot,
    StrategyDefinitionInput,
    StrategyEvent,
    StrategyMetadata,
    StrategyVersionReference,
)


def _definition(**overrides) -> StrategyDefinitionInput:
    payload = {
        "timeframe": "1h",
        "universe_asset_ids": [2, 1],
        "parameters": {"slow_window": 30, "fast_window": 10},
        "signals": [
            {
                "signal_id": "exit-long",
                "role": "exit",
                "direction": "flat",
                "priority": 200,
                "definition": {"kind": "cross_below", "left": "fast", "right": "slow"},
            },
            {
                "signal_id": "enter-long",
                "role": "entry",
                "direction": "long",
                "priority": 100,
                "definition": {"kind": "cross_above", "left": "fast", "right": "slow"},
            },
        ],
        "position": {
            "mode": "long_only",
            "max_open_positions": 1,
            "sizing": {"kind": "fixed_notional", "quote_amount": 100.0},
        },
        "execution": {
            "timing": "next_bar_open",
            "order_type": "market",
            "fee_bps": 10.0,
            "slippage_bps": 2.5,
            "latency_ms": 250,
        },
        "implementation": {
            "kind": "declarative",
            "identifier": "moving-average-cross",
            "revision": "2026-09-13",
        },
        "benchmark_asset_id": 1,
    }
    payload.update(overrides)
    return StrategyDefinitionInput.model_validate(payload)


def _snapshots() -> dict[int, InstrumentSnapshot]:
    return {
        1: InstrumentSnapshot(asset_id=1, symbol="BTCUSDT", exchange="BINANCE", asset_type="SPOT"),
        2: InstrumentSnapshot(asset_id=2, symbol="ETHUSDT", exchange="BINANCE", asset_type="SPOT"),
    }


def _canonical(definition: StrategyDefinitionInput | None = None) -> CanonicalStrategyDefinition:
    return CanonicalStrategyDefinition.from_input(definition or _definition(), _snapshots())


def _result_with_first(value):
    result = MagicMock()
    result.scalars.return_value.first.return_value = value
    return result


def _result_with_all(values):
    result = MagicMock()
    result.scalars.return_value.all.return_value = values
    return result


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


def _asset(asset_id: int, symbol: str):
    return MagicMock(id=asset_id, symbol=symbol, exchange="BINANCE", asset_type="SPOT")


def test_definition_identity_is_deterministic_despite_input_mapping_and_set_order():
    first = _canonical(_definition())
    second = _canonical(
        _definition(
            universe_asset_ids=[1, 2],
            parameters={"fast_window": 10, "slow_window": 30},
            signals=list(reversed(_definition().signals)),
        )
    )

    assert first.canonical_payload()["universe"][0]["asset_id"] == 1
    assert first.canonical_payload()["signals"][0]["signal_id"] == "enter-long"
    assert first.canonical_json() == second.canonical_json()
    assert first.definition_hash() == second.definition_hash()


def test_parameter_change_creates_a_different_reproducible_definition_identity():
    original = _canonical(_definition())
    changed = _canonical(_definition(parameters={"slow_window": 31, "fast_window": 10}))

    assert original.definition_hash() != changed.definition_hash()
    assert original.canonical_payload()["execution"]["fee_bps"] == 10.0
    assert original.canonical_payload()["benchmark"]["symbol"] == "BTCUSDT"


@pytest.mark.asyncio
async def test_catalog_assigns_next_version_after_a_definition_change():
    db = _transactional_db()
    strategy = Strategy(id=uuid.uuid4(), strategy_key="moving-average-cross", name="MA Cross")
    db.execute.side_effect = [
        _result_with_first(strategy),
        _result_with_all([_asset(1, "BTCUSDT"), _asset(2, "ETHUSDT")]),
        _result_with_first(None),
        MagicMock(scalar_one_or_none=MagicMock(return_value=1)),
    ]

    version = await StrategyCatalogService(db).create_version(
        "moving-average-cross",
        _definition(parameters={"slow_window": 31, "fast_window": 10}),
    )

    assert version.version_number == 2
    assert version.strategy_id == strategy.id
    assert version.definition_hash == _canonical(
        _definition(parameters={"slow_window": 31, "fast_window": 10})
    ).definition_hash()
    assert version.canonical_definition["universe"][0]["symbol"] == "BTCUSDT"
    assert db.add.call_args.args[0] is version


@pytest.mark.asyncio
async def test_catalog_reuses_existing_version_for_the_same_definition():
    db = _transactional_db()
    strategy = Strategy(id=uuid.uuid4(), strategy_key="moving-average-cross", name="MA Cross")
    existing = StrategyVersion(
        strategy_id=strategy.id,
        version_number=1,
        schema_version="1",
        definition_hash=_canonical().definition_hash(),
        canonical_definition=_canonical().canonical_payload(),
    )
    db.execute.side_effect = [
        _result_with_first(strategy),
        _result_with_all([_asset(1, "BTCUSDT"), _asset(2, "ETHUSDT")]),
        _result_with_first(existing),
    ]

    returned = await StrategyCatalogService(db).create_version("moving-average-cross", _definition())

    assert returned is existing
    db.add.assert_not_called()
    db.flush.assert_not_awaited()


@pytest.mark.asyncio
async def test_concurrent_strategy_creation_returns_domain_error_not_raw_unique_violation():
    """An absent-key FOR UPDATE cannot serialize two first creates by itself."""
    from sqlalchemy.exc import IntegrityError

    db = _transactional_db()
    raced = Strategy(id=uuid.uuid4(), strategy_key="moving-average-cross", name="MA Cross")
    db.execute.side_effect = [_result_with_first(None), _result_with_first(raced)]
    db.flush.side_effect = IntegrityError("duplicate strategy", {}, Exception("unique"))

    with pytest.raises(StrategyDefinitionError, match="already exists"):
        await StrategyCatalogService(db).create_strategy(
            StrategyMetadata(strategy_key="moving-average-cross", name="MA Cross")
        )

    db.begin_nested.assert_called_once()


@pytest.mark.asyncio
async def test_catalog_rejects_unknown_instrument_before_persisting_a_version():
    db = _transactional_db()
    strategy = Strategy(id=uuid.uuid4(), strategy_key="moving-average-cross", name="MA Cross")
    db.execute.side_effect = [
        _result_with_first(strategy),
        _result_with_all([_asset(1, "BTCUSDT")]),
    ]

    with pytest.raises(StrategyDefinitionError, match=r"unknown assets: \[2\]"):
        await StrategyCatalogService(db).create_version("moving-average-cross", _definition())

    db.add.assert_not_called()


@pytest.mark.asyncio
async def test_catalog_load_verifies_stored_definition_hash_for_reproducibility():
    canonical = _canonical()
    strategy = Strategy(id=uuid.uuid4(), strategy_key="moving-average-cross", name="MA Cross")
    version = StrategyVersion(
        strategy_id=strategy.id,
        version_number=1,
        schema_version="1",
        definition_hash=canonical.definition_hash(),
        canonical_definition=canonical.canonical_payload(),
    )
    db = AsyncMock()
    result = MagicMock()
    result.first.return_value = (strategy, version)
    db.execute.return_value = result

    reference, loaded = await StrategyCatalogService(db).get_canonical_definition("moving-average-cross", 1)

    assert reference.identity.startswith("moving-average-cross@v1:")
    assert loaded.canonical_json() == canonical.canonical_json()

    version.definition_hash = "0" * 64
    with pytest.raises(StrategyDefinitionError, match="does not match"):
        await StrategyCatalogService(db).get_canonical_definition("moving-average-cross", 1)


def test_invalid_configuration_and_timeframe_position_conflicts_are_rejected():
    with pytest.raises(ValidationError):
        _definition(timeframe="2m")
    with pytest.raises(ValidationError, match="incompatible with long_only"):
        _definition(
            signals=[
                {
                    "signal_id": "enter-short",
                    "role": "entry",
                    "direction": "short",
                    "definition": {"kind": "threshold"},
                }
            ]
        )
    with pytest.raises(ValidationError, match="NaN or infinity"):
        _definition(parameters={"slow_window": float("nan")})


def test_event_contract_normalizes_timezone_and_enforces_entry_exit_position_state():
    reference = StrategyVersionReference(
        strategy_key="moving-average-cross",
        version_number=1,
        definition_hash="a" * 64,
    )
    event = StrategyEvent(
        strategy=reference,
        evaluation_mode="live",
        event_type="entry",
        event_time=datetime(2026, 9, 13, 9, 30, tzinfo=timezone(timedelta(hours=5, minutes=30))),
        observed_at=datetime(2026, 9, 13, 9, 30, 1, tzinfo=timezone(timedelta(hours=5, minutes=30))),
        asset_id=1,
        signal_id="enter-long",
        position_state="long",
        payload={"price": 100.0},
    )

    assert event.event_time == datetime(2026, 9, 13, 4, 0, tzinfo=timezone.utc)
    assert event.observed_at == datetime(2026, 9, 13, 4, 0, 1, tzinfo=timezone.utc)
    with pytest.raises(ValidationError, match="timezone-aware"):
        StrategyEvent(
            strategy=reference,
            evaluation_mode="historical",
            event_type="exit",
            event_time=datetime(2026, 9, 13, 4, 0),
            observed_at=datetime(2026, 9, 13, 4, 1, tzinfo=timezone.utc),
            asset_id=1,
            position_state="flat",
        )
    with pytest.raises(ValidationError, match="flat position_state"):
        StrategyEvent(
            strategy=reference,
            evaluation_mode="historical",
            event_type="exit",
            event_time=datetime(2026, 9, 13, 4, 0, tzinfo=timezone.utc),
            observed_at=datetime(2026, 9, 13, 4, 1, tzinfo=timezone.utc),
            asset_id=1,
            position_state="long",
        )
    with pytest.raises(ValidationError, match="cannot precede"):
        StrategyEvent(
            strategy=reference,
            evaluation_mode="live",
            event_type="position",
            event_time=datetime(2026, 9, 13, 4, 1, tzinfo=timezone.utc),
            observed_at=datetime(2026, 9, 13, 4, 0, tzinfo=timezone.utc),
            asset_id=1,
            position_state="flat",
        )


def test_strategy_metadata_has_a_stable_machine_identity_separate_from_display_metadata():
    metadata = StrategyMetadata(
        strategy_key="moving-average-cross",
        name=" Moving Average Cross ",
        description=" A user-facing description. ",
    )

    assert metadata.strategy_key == "moving-average-cross"
    assert metadata.name == "Moving Average Cross"
    assert metadata.description == "A user-facing description."


@pytest.mark.asyncio
async def test_catalog_persists_metadata_without_putting_it_in_the_execution_hash():
    db = _transactional_db()
    db.execute.return_value = _result_with_first(None)
    metadata = StrategyMetadata(
        strategy_key="moving-average-cross",
        name="Moving Average Cross",
        description="Display-only catalog metadata",
    )

    strategy = await StrategyCatalogService(db).create_strategy(metadata)

    assert strategy.strategy_key == "moving-average-cross"
    assert strategy.name == "Moving Average Cross"
    assert strategy.description == "Display-only catalog metadata"
    assert db.add.call_args.args[0] is strategy
    db.flush.assert_awaited_once()
