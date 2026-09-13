"""backtest result persistence

Revision ID: 010_backtest_results
Revises: 009_strategy_catalog
Create Date: 2026-09-13 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "010_backtest_results"
down_revision = "009_strategy_catalog"
branch_labels = None
depends_on = None


_AMOUNT = sa.Numeric(24, 8)
_RATIO = sa.Numeric(24, 12)


def upgrade() -> None:
    # Allows the result's denormalized strategy_id to be checked against its
    # referenced immutable strategy-version row.
    op.create_unique_constraint(
        "uq_strategy_versions_id_strategy",
        "strategy_versions",
        ["id", "strategy_id"],
    )

    op.create_table(
        "backtest_results",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("strategy_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_version_number", sa.Integer(), nullable=False),
        sa.Column("strategy_definition_hash", sa.String(length=64), nullable=False),
        sa.Column("universe", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("benchmark", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("timeframe", sa.String(length=16), nullable=False),
        sa.Column("parameters", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("execution_assumptions", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("data_source", sa.String(length=128), nullable=False),
        sa.Column("data_version", sa.String(length=256), nullable=False),
        sa.Column("data_coverage_fingerprint", sa.String(length=256), nullable=True),
        sa.Column("start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("initial_capital", _AMOUNT, nullable=False),
        sa.Column("final_equity", _AMOUNT, nullable=False),
        sa.Column("total_return", _RATIO, nullable=True),
        sa.Column("max_drawdown", _RATIO, nullable=True),
        sa.Column("annualized_volatility", _RATIO, nullable=True),
        sa.Column("sharpe_ratio", _RATIO, nullable=True),
        sa.Column("sortino_ratio", _RATIO, nullable=True),
        sa.Column("calmar_ratio", _RATIO, nullable=True),
        sa.Column("value_at_risk", _RATIO, nullable=True),
        sa.Column("exposure", _RATIO, nullable=False),
        sa.Column("benchmark_return", _RATIO, nullable=True),
        sa.Column("benchmark_excess_return", _RATIO, nullable=True),
        sa.Column("trade_count", sa.Integer(), nullable=False),
        sa.Column("winning_trade_count", sa.Integer(), nullable=False),
        sa.Column("losing_trade_count", sa.Integer(), nullable=False),
        sa.Column("breakeven_trade_count", sa.Integer(), nullable=False),
        sa.Column("fees_paid", _AMOUNT, nullable=False),
        sa.Column("slippage_paid", _AMOUNT, nullable=False),
        sa.Column("evaluator_id", sa.String(length=128), nullable=False),
        sa.Column("evaluator_revision", sa.String(length=256), nullable=False),
        sa.Column("random_seed", sa.Integer(), nullable=True),
        sa.Column("reproducibility_metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("result_hash", sa.String(length=64), nullable=False),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("char_length(run_id) = 64", name="ck_backtest_results_run_id_length"),
        sa.CheckConstraint("run_id ~ '^[0-9a-f]{64}$'", name="ck_backtest_results_run_id_format"),
        sa.CheckConstraint("result_hash ~ '^[0-9a-f]{64}$'", name="ck_backtest_results_result_hash_format"),
        sa.CheckConstraint("end_time > start_time", name="ck_backtest_results_valid_window"),
        sa.CheckConstraint("initial_capital > 0", name="ck_backtest_results_positive_initial_capital"),
        sa.CheckConstraint("final_equity > 0", name="ck_backtest_results_positive_final_equity"),
        sa.CheckConstraint("exposure >= 0 AND exposure <= 1", name="ck_backtest_results_exposure_range"),
        sa.CheckConstraint(
            "max_drawdown IS NULL OR (max_drawdown >= 0 AND max_drawdown <= 1)",
            name="ck_backtest_results_drawdown_range",
        ),
        sa.CheckConstraint(
            "trade_count >= 0 AND winning_trade_count >= 0 AND losing_trade_count >= 0 "
            "AND breakeven_trade_count >= 0",
            name="ck_backtest_results_nonnegative_trade_counts",
        ),
        sa.CheckConstraint(
            "winning_trade_count + losing_trade_count + breakeven_trade_count = trade_count",
            name="ck_backtest_results_trade_count_breakdown",
        ),
        sa.CheckConstraint("fees_paid >= 0 AND slippage_paid >= 0", name="ck_backtest_results_nonnegative_costs"),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategies.id"]),
        sa.ForeignKeyConstraint(
            ["strategy_version_id", "strategy_id"],
            ["strategy_versions.id", "strategy_versions.strategy_id"],
            name="fk_backtest_results_strategy_version_strategy",
        ),
        sa.PrimaryKeyConstraint("run_id"),
    )
    op.create_index(op.f("ix_backtest_results_strategy_id"), "backtest_results", ["strategy_id"], unique=False)
    op.create_index(
        op.f("ix_backtest_results_strategy_version_id"),
        "backtest_results",
        ["strategy_version_id"],
        unique=False,
    )

    op.create_table(
        "backtest_trades",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("direction", sa.String(length=8), nullable=False),
        sa.Column("entry_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("exit_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("entry_price", _AMOUNT, nullable=False),
        sa.Column("exit_price", _AMOUNT, nullable=False),
        sa.Column("quantity", _AMOUNT, nullable=False),
        sa.Column("gross_pnl", _AMOUNT, nullable=False),
        sa.Column("net_pnl", _AMOUNT, nullable=False),
        sa.Column("fees_paid", _AMOUNT, nullable=False),
        sa.Column("slippage_paid", _AMOUNT, nullable=False),
        sa.Column("entry_signal_id", sa.String(length=64), nullable=True),
        sa.Column("exit_signal_id", sa.String(length=64), nullable=True),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.CheckConstraint("sequence > 0", name="ck_backtest_trades_positive_sequence"),
        sa.CheckConstraint("direction IN ('long', 'short')", name="ck_backtest_trades_direction"),
        sa.CheckConstraint("exit_time >= entry_time", name="ck_backtest_trades_valid_timeline"),
        sa.CheckConstraint(
            "entry_price > 0 AND exit_price > 0 AND quantity > 0",
            name="ck_backtest_trades_positive_execution_values",
        ),
        sa.CheckConstraint("fees_paid >= 0 AND slippage_paid >= 0", name="ck_backtest_trades_nonnegative_costs"),
        sa.ForeignKeyConstraint(["run_id"], ["backtest_results.run_id"]),
        sa.ForeignKeyConstraint(["asset_id"], ["asset_registry.id"]),
        sa.PrimaryKeyConstraint("run_id", "sequence"),
    )
    op.create_index(op.f("ix_backtest_trades_asset_id"), "backtest_trades", ["asset_id"], unique=False)

    op.create_table(
        "backtest_signals",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("event_type", sa.String(length=16), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("signal_id", sa.String(length=64), nullable=True),
        sa.Column("position_state", sa.String(length=8), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.CheckConstraint("sequence > 0", name="ck_backtest_signals_positive_sequence"),
        sa.CheckConstraint(
            "event_type IN ('signal', 'entry', 'exit', 'position')",
            name="ck_backtest_signals_event_type",
        ),
        sa.CheckConstraint("observed_at >= event_time", name="ck_backtest_signals_observation_time"),
        sa.CheckConstraint(
            "position_state IS NULL OR position_state IN ('long', 'short', 'flat')",
            name="ck_backtest_signals_position_state",
        ),
        sa.ForeignKeyConstraint(["run_id"], ["backtest_results.run_id"]),
        sa.ForeignKeyConstraint(["asset_id"], ["asset_registry.id"]),
        sa.PrimaryKeyConstraint("run_id", "sequence"),
    )
    op.create_index(op.f("ix_backtest_signals_asset_id"), "backtest_signals", ["asset_id"], unique=False)

    op.execute(
        """
        CREATE FUNCTION prevent_backtest_result_mutation() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'backtest results, trades, and signals are immutable';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    for table_name in ("backtest_results", "backtest_trades", "backtest_signals"):
        op.execute(
            f"""
            CREATE TRIGGER {table_name}_immutable
            BEFORE UPDATE OR DELETE ON {table_name}
            FOR EACH ROW EXECUTE FUNCTION prevent_backtest_result_mutation();
            """
        )


def downgrade() -> None:
    for table_name in ("backtest_signals", "backtest_trades", "backtest_results"):
        op.execute(f"DROP TRIGGER IF EXISTS {table_name}_immutable ON {table_name};")
    op.execute("DROP FUNCTION IF EXISTS prevent_backtest_result_mutation();")
    op.drop_index(op.f("ix_backtest_signals_asset_id"), table_name="backtest_signals")
    op.drop_table("backtest_signals")
    op.drop_index(op.f("ix_backtest_trades_asset_id"), table_name="backtest_trades")
    op.drop_table("backtest_trades")
    op.drop_index(op.f("ix_backtest_results_strategy_version_id"), table_name="backtest_results")
    op.drop_index(op.f("ix_backtest_results_strategy_id"), table_name="backtest_results")
    op.drop_table("backtest_results")
    op.drop_constraint("uq_strategy_versions_id_strategy", "strategy_versions", type_="unique")
