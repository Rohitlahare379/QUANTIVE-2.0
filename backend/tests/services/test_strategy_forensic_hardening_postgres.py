"""Real-database invariants added by strategy forensic hardening.

These are intentionally not mocked: they require PostgreSQL/TimescaleDB after
``alembic upgrade head``.  The strategy catalog is append-only, so every test
uses fresh random identities rather than deleting immutable forensic records.
"""

from __future__ import annotations

import uuid
import asyncio
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.models.asset_registry import AssetRegistry
from app.models.backtest import BacktestResult
from app.models.live_strategy import (
    LiveStrategyActivation,
    LiveStrategyEvent,
    LiveStrategyObservation,
    LiveStrategyState,
)
from app.models.divergence import StrategyComparisonRun
from app.models.strategy import Strategy, StrategyVersion
from app.models.strategy_monitoring import StrategyMonitoringBinding
from app.services.strategy_monitoring import _BindingClaim, StrategyMonitoringService


pytestmark = [pytest.mark.postgres, pytest.mark.timescaledb]

engine = create_async_engine(settings.sqlalchemy_database_uri, poolclass=NullPool)
Session = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)

UTC = timezone.utc
NOW = datetime(2026, 1, 1, tzinfo=UTC)
HASH_A = "a" * 64
HASH_B = "b" * 64


async def _strategy_version() -> tuple[Strategy, StrategyVersion]:
    strategy = Strategy(
        id=uuid.uuid4(),
        strategy_key=f"forensic-hardening-{uuid.uuid4().hex}",
        name="Forensic hardening test strategy",
    )
    version = StrategyVersion(
        id=uuid.uuid4(),
        strategy_id=strategy.id,
        version_number=1,
        schema_version="1",
        definition_hash=HASH_A,
        canonical_definition={},
    )
    async with Session() as session:
        session.add_all([strategy, version])
        await session.commit()
    return strategy, version


def _backtest_result(
    strategy: Strategy,
    version: StrategyVersion,
    *,
    version_number: int = 2,
    definition_hash: str = HASH_B,
) -> BacktestResult:
    return BacktestResult(
        run_id=uuid.uuid4().hex * 2,
        strategy_id=strategy.id,
        strategy_version_id=version.id,
        # Deliberately falsified snapshot: the database must reject it before
        # it can become an immutable, apparently reproducible result.
        strategy_version_number=version_number,
        strategy_definition_hash=definition_hash,
        universe=[],
        benchmark=None,
        timeframe="1h",
        parameters={},
        execution_assumptions={},
        data_source="canonical",
        data_version="test-v1",
        data_coverage_fingerprint="test-coverage",
        start_time=NOW,
        end_time=NOW + timedelta(hours=1),
        initial_capital=Decimal("100"),
        final_equity=Decimal("100"),
        total_return=Decimal("0"),
        max_drawdown=Decimal("0"),
        annualized_volatility=Decimal("0"),
        sharpe_ratio=None,
        sortino_ratio=None,
        calmar_ratio=None,
        value_at_risk=None,
        exposure=Decimal("0"),
        benchmark_return=None,
        benchmark_excess_return=None,
        trade_count=0,
        winning_trade_count=0,
        losing_trade_count=0,
        breakeven_trade_count=0,
        fees_paid=Decimal("0"),
        slippage_paid=Decimal("0"),
        evaluator_id="test",
        evaluator_revision="1",
        random_seed=None,
        reproducibility_metadata={},
        result_hash=uuid.uuid4().hex * 2,
        generated_at=NOW,
    )


@pytest.mark.asyncio
async def test_database_rejects_backtest_result_with_falsified_strategy_snapshot():
    strategy, version = await _strategy_version()
    async with Session() as session:
        session.add(_backtest_result(strategy, version))
        with pytest.raises(DBAPIError, match="backtest result version snapshot"):
            await session.flush()


@pytest.mark.asyncio
async def test_expired_monitoring_owner_cannot_renew_against_a_fresh_concurrent_claim():
    """The expiry predicate fences a stale worker at the database boundary."""
    strategy, version = await _strategy_version()
    reference = _backtest_result(
        strategy,
        version,
        version_number=version.version_number,
        definition_hash=version.definition_hash,
    )
    activation = LiveStrategyActivation(
        id=uuid.uuid4(),
        strategy_id=strategy.id,
        strategy_version_id=version.id,
        strategy_version_number=version.version_number,
        strategy_definition_hash=version.definition_hash,
        configuration={"evaluator_id": "threshold-v1"},
        configuration_hash=HASH_B,
        status="active",
        health="unknown",
        activated_at=NOW,
        open_position_count=0,
    )
    binding = StrategyMonitoringBinding(
        id=uuid.uuid4(),
        activation_id=activation.id,
        reference_run_id=reference.run_id,
        comparison_policy_id="test-comparison",
        comparison_policy_revision="1",
        comparison_policy_hash=HASH_A,
        comparison_policy={"policy_id": "test-comparison"},
        policy_id="test-monitoring",
        policy_revision="1",
        policy_hash=HASH_B,
        policy={"policy_id": "test-monitoring"},
        status="active",
        registered_at=NOW,
        lease_worker_id="stale-worker",
        lease_token="stale-token",
        # The deadline itself is stale; a strict fence must not grant the old
        # token one extra renewal at this exact timestamp.
        lease_expires_at=NOW,
        claimed_at=NOW - timedelta(minutes=1),
        claim_attempt_count=1,
    )
    async with Session() as session:
        session.add_all([reference, activation, binding])
        await session.commit()

    stale_claim = _BindingClaim(
        binding_id=binding.id,
        activation_id=activation.id,
        token="stale-token",
        worker_id="stale-worker",
    )
    claim_now = NOW

    async def renew_stale() -> bool:
        async with Session() as session:
            return await StrategyMonitoringService(session, now_fn=lambda: claim_now)._renew_claim(stale_claim)

    async def claim_fresh():
        async with Session() as session:
            return await StrategyMonitoringService(
                session,
                now_fn=lambda: claim_now,
            )._claim_binding(binding.id, worker_id="fresh-worker")

    renewed, fresh_claim = await asyncio.gather(renew_stale(), claim_fresh())

    assert renewed is False
    assert fresh_claim is not None
    assert fresh_claim.token != stale_claim.token
    assert fresh_claim.worker_id == "fresh-worker"


@pytest.mark.asyncio
async def test_database_rejects_comparison_reference_from_a_different_strategy():
    """Matching hashes are not enough: both sides must share a logical strategy."""
    live_strategy, live_version = await _strategy_version()
    reference_strategy, reference_version = await _strategy_version()
    foreign_reference = _backtest_result(
        reference_strategy,
        reference_version,
        version_number=reference_version.version_number,
        definition_hash=reference_version.definition_hash,
    )
    activation = LiveStrategyActivation(
        id=uuid.uuid4(),
        strategy_id=live_strategy.id,
        strategy_version_id=live_version.id,
        strategy_version_number=live_version.version_number,
        strategy_definition_hash=live_version.definition_hash,
        configuration={"evaluator_id": "threshold-v1"},
        configuration_hash=HASH_B,
        status="active",
        health="unknown",
        activated_at=NOW,
        open_position_count=0,
    )
    async with Session() as session:
        session.add_all([foreign_reference, activation])
        await session.commit()

    async with Session() as session:
        session.add(
            StrategyComparisonRun(
                run_id=uuid.uuid4().hex * 2,
                strategy_id=live_strategy.id,
                strategy_version_id=live_version.id,
                strategy_version_number=live_version.version_number,
                strategy_definition_hash=live_version.definition_hash,
                activation_id=activation.id,
                reference_run_id=foreign_reference.run_id,
                reference_result_hash=foreign_reference.result_hash,
                policy_id="test-policy",
                policy_revision="1",
                policy_hash=HASH_A,
                policy={},
                reference_source={"backtest_run_id": foreign_reference.run_id},
                observed_source={"activation_id": str(activation.id)},
                window_start=NOW,
                window_end=NOW + timedelta(hours=1),
                status="insufficient_reference",
            )
        )
        with pytest.raises(DBAPIError, match="comparison reference belongs to a different strategy"):
            await session.flush()


@pytest.mark.asyncio
async def test_database_rejects_event_attached_to_the_wrong_candle_observation():
    strategy, version = await _strategy_version()
    async with Session() as session:
        asset = AssetRegistry(
            symbol=f"FORENSIC{uuid.uuid4().hex[:16].upper()}",
            exchange="BINANCE",
            asset_type="SPOT",
            is_active=True,
        )
        session.add(asset)
        await session.flush()

        activation = LiveStrategyActivation(
            id=uuid.uuid4(),
            strategy_id=strategy.id,
            strategy_version_id=version.id,
            strategy_version_number=version.version_number,
            strategy_definition_hash=version.definition_hash,
            configuration={"evaluator_id": "threshold-v1"},
            configuration_hash=HASH_A,
            status="active",
            health="unknown",
            activated_at=NOW,
            open_position_count=0,
        )
        session.add(activation)
        await session.flush()
        state = LiveStrategyState(
            activation_id=activation.id,
            asset_id=asset.id,
            position_state="flat",
            health="unknown",
            consecutive_errors=0,
        )
        session.add(state)
        await session.flush()
        observation = LiveStrategyObservation(
            id=uuid.uuid4(),
            activation_id=activation.id,
            asset_id=asset.id,
            candle_timestamp=NOW,
            evaluation_attempt=1,
            observed_at=NOW,
            status="evaluated",
            position_before="flat",
            position_after="flat",
            evaluation_latency_ms=0.0,
        )
        session.add(observation)
        await session.flush()

        # All foreign keys still point at valid rows; only the cross-row
        # forensic scope trigger can reject this fabricated live event.
        session.add(
            LiveStrategyEvent(
                id=uuid.uuid4(),
                observation_id=observation.id,
                activation_id=activation.id,
                asset_id=asset.id,
                candle_timestamp=NOW + timedelta(hours=1),
                event_time=NOW,
                event_type="signal",
                signal_id="forged",
                position_state="flat",
            )
        )
        with pytest.raises(DBAPIError, match="event scope does not match"):
            await session.flush()
