"""close strategy forensic persistence gaps

Revision ID: 016_strategy_forensic_hardening
Revises: 015_durable_worker_hardening
Create Date: 2026-09-14 00:00:00.000000

This migration makes retry attempts durable evidence, disables legacy monitor
bindings that have no recoverable comparator snapshot, and moves the critical
cross-row strategy/backtest/comparison invariants to PostgreSQL.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "016_strategy_forensic_hardening"
down_revision = "015_durable_worker_hardening"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Failed live evaluations are immutable forensic evidence.  A repaired
    # replay must not overwrite the original attempt or race it on the former
    # three-column uniqueness key.
    op.add_column(
        "live_strategy_observations",
        sa.Column("evaluation_attempt", sa.Integer(), nullable=False, server_default=sa.text("1")),
    )
    op.create_check_constraint(
        "ck_live_observations_positive_attempt",
        "live_strategy_observations",
        "evaluation_attempt >= 1",
    )
    op.drop_constraint(
        "uq_live_observations_activation_asset_candle",
        "live_strategy_observations",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_live_observations_activation_asset_candle",
        "live_strategy_observations",
        ["activation_id", "asset_id", "candle_timestamp", "evaluation_attempt"],
    )
    op.alter_column("live_strategy_observations", "evaluation_attempt", server_default=None)
    op.create_index(
        "ix_live_observations_activation_candle_status",
        "live_strategy_observations",
        ["activation_id", "candle_timestamp", "status"],
        unique=False,
    )

    # The application already treats a new activation as unknown.  The
    # database must accept that state too, otherwise a production activation
    # fails after validation has succeeded in application memory.
    op.drop_constraint("ck_live_activations_health", "live_strategy_activations", type_="check")
    op.create_check_constraint(
        "ck_live_activations_health",
        "live_strategy_activations",
        "health IN ('unknown', 'healthy', 'degraded', 'error', 'stopped')",
    )
    op.drop_constraint("ck_live_states_health", "live_strategy_states", type_="check")
    op.create_check_constraint(
        "ck_live_states_health",
        "live_strategy_states",
        "health IN ('unknown', 'healthy', 'degraded', 'error', 'stopped')",
    )

    # A hash/id/revision alone cannot recreate a comparator.  Retain legacy
    # bindings for audit history but fail them closed; an operator must
    # explicitly re-register with an actual frozen policy snapshot.
    op.add_column(
        "strategy_monitoring_bindings",
        sa.Column("comparison_policy", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.execute(
        """
        UPDATE strategy_monitoring_bindings
           SET status = 'error',
               lease_worker_id = NULL,
               lease_token = NULL,
               lease_expires_at = NULL,
               claimed_at = NULL
         WHERE status = 'active' AND comparison_policy IS NULL
        """
    )
    op.create_check_constraint(
        "ck_monitoring_bindings_active_comparison_policy",
        "strategy_monitoring_bindings",
        "status <> 'active' OR comparison_policy IS NOT NULL",
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION prevent_strategy_monitoring_binding_identity_mutation()
        RETURNS trigger AS $$
        BEGIN
            IF NEW.activation_id IS DISTINCT FROM OLD.activation_id
               OR NEW.reference_run_id IS DISTINCT FROM OLD.reference_run_id
               OR NEW.comparison_policy_id IS DISTINCT FROM OLD.comparison_policy_id
               OR NEW.comparison_policy_revision IS DISTINCT FROM OLD.comparison_policy_revision
               OR NEW.comparison_policy_hash IS DISTINCT FROM OLD.comparison_policy_hash
               OR NEW.comparison_policy IS DISTINCT FROM OLD.comparison_policy
               OR NEW.policy_id IS DISTINCT FROM OLD.policy_id
               OR NEW.policy_revision IS DISTINCT FROM OLD.policy_revision
               OR NEW.policy_hash IS DISTINCT FROM OLD.policy_hash
               OR NEW.policy IS DISTINCT FROM OLD.policy THEN
                RAISE EXCEPTION 'strategy monitoring binding policy and scope are immutable';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    # These are the actual query orderings used by bounded comparison loading;
    # without them a large live history turns a fixed-size application page into
    # an unbounded database scan.
    op.create_index(
        "ix_backtest_signals_run_event_time",
        "backtest_signals",
        ["run_id", "event_time", "sequence"],
        unique=False,
    )
    op.create_index(
        "ix_backtest_trades_run_entry_time",
        "backtest_trades",
        ["run_id", "entry_time", "sequence"],
        unique=False,
    )
    op.create_index(
        "ix_backtest_trades_run_exit_time",
        "backtest_trades",
        ["run_id", "exit_time", "sequence"],
        unique=False,
    )
    op.create_index(
        "ix_strategy_comparison_runs_activation_reference_policy_window",
        "strategy_comparison_runs",
        ["activation_id", "reference_run_id", "policy_hash", "window_end"],
        unique=False,
    )
    op.create_index(
        "ix_strategy_divergence_findings_run_created",
        "strategy_divergence_findings",
        ["comparison_run_id", "created_at", "id"],
        unique=False,
    )

    # Event rows are consumed as live signal evidence. A plain foreign key to
    # an observation is insufficient: direct SQL could otherwise attach an
    # event to a different activation/asset/candle and manufacture a false
    # strategy divergence.
    op.execute(
        """
        CREATE FUNCTION validate_live_strategy_observation_scope() RETURNS trigger AS $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                  FROM live_strategy_states
                 WHERE activation_id = NEW.activation_id
                   AND asset_id = NEW.asset_id
            ) THEN
                RAISE EXCEPTION 'live strategy observation asset is not configured for activation';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER live_strategy_observation_scope_valid
        BEFORE INSERT OR UPDATE OF activation_id, asset_id ON live_strategy_observations
        FOR EACH ROW EXECUTE FUNCTION validate_live_strategy_observation_scope();
        """
    )
    op.execute(
        """
        CREATE FUNCTION validate_live_strategy_event_scope() RETURNS trigger AS $$
        DECLARE
            observation_activation uuid;
            observation_asset integer;
            observation_candle timestamptz;
            observation_status text;
        BEGIN
            SELECT activation_id, asset_id, candle_timestamp, status
              INTO observation_activation, observation_asset, observation_candle, observation_status
              FROM live_strategy_observations
             WHERE id = NEW.observation_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'live strategy event references an unknown observation';
            END IF;
            IF NEW.activation_id <> observation_activation
               OR NEW.asset_id <> observation_asset
               OR NEW.candle_timestamp <> observation_candle THEN
                RAISE EXCEPTION 'live strategy event scope does not match its observation';
            END IF;
            IF observation_status <> 'evaluated' THEN
                RAISE EXCEPTION 'live strategy event requires a successful observation';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER live_strategy_event_scope_valid
        BEFORE INSERT ON live_strategy_events
        FOR EACH ROW EXECUTE FUNCTION validate_live_strategy_event_scope();
        """
    )

    # The ORM checks these relationships, but direct SQL, migrations, and a
    # future worker must not be able to create a result that cannot be replayed
    # against its stated data window/universe.
    op.execute(
        """
        CREATE FUNCTION validate_backtest_result_strategy_snapshot() RETURNS trigger AS $$
        DECLARE
            stored_version_number integer;
            stored_definition_hash text;
        BEGIN
            SELECT version_number, definition_hash
              INTO stored_version_number, stored_definition_hash
              FROM strategy_versions
             WHERE id = NEW.strategy_version_id
               AND strategy_id = NEW.strategy_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'backtest result references an unknown strategy version';
            END IF;
            IF NEW.strategy_version_number <> stored_version_number
               OR NEW.strategy_definition_hash <> stored_definition_hash THEN
                RAISE EXCEPTION 'backtest result version snapshot does not match strategy_versions';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER backtest_result_strategy_snapshot_valid
        BEFORE INSERT ON backtest_results
        FOR EACH ROW EXECUTE FUNCTION validate_backtest_result_strategy_snapshot();
        """
    )
    op.execute(
        """
        CREATE FUNCTION validate_backtest_trade_scope() RETURNS trigger AS $$
        DECLARE
            result_start timestamptz;
            result_end timestamptz;
            in_universe boolean;
        BEGIN
            SELECT start_time, end_time,
                   EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(universe) AS instrument
                        WHERE (instrument->>'asset_id')::integer = NEW.asset_id
                   )
              INTO result_start, result_end, in_universe
              FROM backtest_results WHERE run_id = NEW.run_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'backtest trade references unknown result';
            END IF;
            IF NOT in_universe THEN
                RAISE EXCEPTION 'backtest trade asset is outside result universe';
            END IF;
            IF NEW.entry_time < result_start OR NEW.entry_time > result_end
               OR NEW.exit_time < result_start OR NEW.exit_time > result_end THEN
                RAISE EXCEPTION 'backtest trade is outside result window';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER backtest_trade_scope_valid
        BEFORE INSERT OR UPDATE OF run_id, asset_id, entry_time, exit_time ON backtest_trades
        FOR EACH ROW EXECUTE FUNCTION validate_backtest_trade_scope();
        """
    )
    op.execute(
        """
        CREATE FUNCTION validate_backtest_signal_scope() RETURNS trigger AS $$
        DECLARE
            result_start timestamptz;
            result_end timestamptz;
            in_universe boolean;
        BEGIN
            SELECT start_time, end_time,
                   EXISTS (
                       SELECT 1
                         FROM jsonb_array_elements(universe) AS instrument
                        WHERE (instrument->>'asset_id')::integer = NEW.asset_id
                   )
              INTO result_start, result_end, in_universe
              FROM backtest_results WHERE run_id = NEW.run_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'backtest signal references unknown result';
            END IF;
            IF NOT in_universe THEN
                RAISE EXCEPTION 'backtest signal asset is outside result universe';
            END IF;
            IF NEW.event_time < result_start OR NEW.event_time > result_end THEN
                RAISE EXCEPTION 'backtest signal is outside result window';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER backtest_signal_scope_valid
        BEFORE INSERT OR UPDATE OF run_id, asset_id, event_time ON backtest_signals
        FOR EACH ROW EXECUTE FUNCTION validate_backtest_signal_scope();
        """
    )

    # A comparison is allowed to expose a version mismatch, so only assert
    # that each denormalized snapshot matches its own activation/reference—not
    # that the two versions are the same.
    op.execute(
        """
        CREATE FUNCTION validate_strategy_comparison_run_scope() RETURNS trigger AS $$
        DECLARE
            activation_strategy uuid;
            activation_version uuid;
            activation_number integer;
            activation_hash text;
            reference_strategy uuid;
            reference_hash text;
        BEGIN
            SELECT strategy_id, strategy_version_id, strategy_version_number, strategy_definition_hash
              INTO activation_strategy, activation_version, activation_number, activation_hash
              FROM live_strategy_activations WHERE id = NEW.activation_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'comparison references unknown activation';
            END IF;
            SELECT strategy_id, result_hash INTO reference_strategy, reference_hash
              FROM backtest_results WHERE run_id = NEW.reference_run_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'comparison references unknown backtest result';
            END IF;
            IF NEW.strategy_id <> activation_strategy
               OR NEW.strategy_version_id <> activation_version
               OR NEW.strategy_version_number <> activation_number
               OR NEW.strategy_definition_hash <> activation_hash THEN
                RAISE EXCEPTION 'comparison identity does not match live activation';
            END IF;
            IF reference_strategy <> activation_strategy THEN
                RAISE EXCEPTION 'comparison reference belongs to a different strategy';
            END IF;
            IF NEW.reference_result_hash <> reference_hash THEN
                RAISE EXCEPTION 'comparison reference hash does not match backtest result';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_comparison_run_scope_valid
        BEFORE INSERT ON strategy_comparison_runs
        FOR EACH ROW EXECUTE FUNCTION validate_strategy_comparison_run_scope();
        """
    )
    op.execute(
        """
        CREATE FUNCTION validate_strategy_divergence_finding_scope() RETURNS trigger AS $$
        DECLARE
            parent_strategy uuid;
            parent_version uuid;
            parent_number integer;
            parent_hash text;
            parent_start timestamptz;
            parent_end timestamptz;
        BEGIN
            SELECT strategy_id, strategy_version_id, strategy_version_number,
                   strategy_definition_hash, window_start, window_end
              INTO parent_strategy, parent_version, parent_number, parent_hash, parent_start, parent_end
              FROM strategy_comparison_runs WHERE run_id = NEW.comparison_run_id;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'finding references unknown comparison run';
            END IF;
            IF NEW.strategy_id <> parent_strategy
               OR NEW.strategy_version_id <> parent_version
               OR NEW.strategy_version_number <> parent_number
               OR NEW.strategy_definition_hash <> parent_hash
               OR NEW.window_start <> parent_start
               OR NEW.window_end <> parent_end THEN
                RAISE EXCEPTION 'finding scope does not match comparison run';
            END IF;
            IF NEW.event_timestamp IS NOT NULL
               AND (NEW.event_timestamp < parent_start OR NEW.event_timestamp > parent_end) THEN
                RAISE EXCEPTION 'finding timestamp is outside comparison window';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_divergence_finding_scope_valid
        BEFORE INSERT ON strategy_divergence_findings
        FOR EACH ROW EXECUTE FUNCTION validate_strategy_divergence_finding_scope();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS strategy_divergence_finding_scope_valid ON strategy_divergence_findings;")
    op.execute("DROP FUNCTION IF EXISTS validate_strategy_divergence_finding_scope();")
    op.execute("DROP TRIGGER IF EXISTS strategy_comparison_run_scope_valid ON strategy_comparison_runs;")
    op.execute("DROP FUNCTION IF EXISTS validate_strategy_comparison_run_scope();")
    op.execute("DROP TRIGGER IF EXISTS live_strategy_event_scope_valid ON live_strategy_events;")
    op.execute("DROP FUNCTION IF EXISTS validate_live_strategy_event_scope();")
    op.execute("DROP TRIGGER IF EXISTS live_strategy_observation_scope_valid ON live_strategy_observations;")
    op.execute("DROP FUNCTION IF EXISTS validate_live_strategy_observation_scope();")
    op.execute("DROP TRIGGER IF EXISTS backtest_signal_scope_valid ON backtest_signals;")
    op.execute("DROP FUNCTION IF EXISTS validate_backtest_signal_scope();")
    op.execute("DROP TRIGGER IF EXISTS backtest_trade_scope_valid ON backtest_trades;")
    op.execute("DROP FUNCTION IF EXISTS validate_backtest_trade_scope();")
    op.execute("DROP TRIGGER IF EXISTS backtest_result_strategy_snapshot_valid ON backtest_results;")
    op.execute("DROP FUNCTION IF EXISTS validate_backtest_result_strategy_snapshot();")
    for name, table in (
        ("ix_strategy_divergence_findings_run_created", "strategy_divergence_findings"),
        ("ix_strategy_comparison_runs_activation_reference_policy_window", "strategy_comparison_runs"),
        ("ix_backtest_trades_run_exit_time", "backtest_trades"),
        ("ix_backtest_trades_run_entry_time", "backtest_trades"),
        ("ix_backtest_signals_run_event_time", "backtest_signals"),
    ):
        op.drop_index(name, table_name=table)
    op.drop_constraint(
        "ck_monitoring_bindings_active_comparison_policy",
        "strategy_monitoring_bindings",
        type_="check",
    )
    # The trigger created in migration 013 remains installed after this
    # downgrade. Restore its pre-016 body before dropping the new column;
    # otherwise every later binding update would reference a missing field.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION prevent_strategy_monitoring_binding_identity_mutation()
        RETURNS trigger AS $$
        BEGIN
            IF NEW.activation_id IS DISTINCT FROM OLD.activation_id
               OR NEW.reference_run_id IS DISTINCT FROM OLD.reference_run_id
               OR NEW.comparison_policy_id IS DISTINCT FROM OLD.comparison_policy_id
               OR NEW.comparison_policy_revision IS DISTINCT FROM OLD.comparison_policy_revision
               OR NEW.comparison_policy_hash IS DISTINCT FROM OLD.comparison_policy_hash
               OR NEW.policy_id IS DISTINCT FROM OLD.policy_id
               OR NEW.policy_revision IS DISTINCT FROM OLD.policy_revision
               OR NEW.policy_hash IS DISTINCT FROM OLD.policy_hash
               OR NEW.policy IS DISTINCT FROM OLD.policy THEN
                RAISE EXCEPTION 'strategy monitoring binding policy and scope are immutable';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.drop_column("strategy_monitoring_bindings", "comparison_policy")

    # ``unknown`` is a valid and deliberately fail-closed state only after
    # this migration. Mapping it to a pre-016 value would fabricate health,
    # so refuse a destructive downgrade with a precise reason instead.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM live_strategy_states WHERE health = 'unknown')
               OR EXISTS (SELECT 1 FROM live_strategy_activations WHERE health = 'unknown') THEN
                RAISE EXCEPTION 'cannot downgrade while unknown live strategy health states exist';
            END IF;
        END;
        $$;
        """
    )
    op.drop_constraint("ck_live_states_health", "live_strategy_states", type_="check")
    op.create_check_constraint(
        "ck_live_states_health",
        "live_strategy_states",
        "health IN ('healthy', 'degraded', 'error', 'stopped')",
    )
    op.drop_constraint("ck_live_activations_health", "live_strategy_activations", type_="check")
    op.create_check_constraint(
        "ck_live_activations_health",
        "live_strategy_activations",
        "health IN ('healthy', 'degraded', 'error', 'stopped')",
    )
    op.drop_index("ix_live_observations_activation_candle_status", table_name="live_strategy_observations")
    op.drop_constraint(
        "ck_live_observations_positive_attempt",
        "live_strategy_observations",
        type_="check",
    )
    # Downgrade is intentionally blocked if repaired replays exist: collapsing
    # attempts would destroy immutable failure evidence.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM live_strategy_observations
                 GROUP BY activation_id, asset_id, candle_timestamp
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION 'cannot downgrade while multiple evaluation attempts exist';
            END IF;
        END;
        $$;
        """
    )
    op.drop_constraint(
        "uq_live_observations_activation_asset_candle",
        "live_strategy_observations",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_live_observations_activation_asset_candle",
        "live_strategy_observations",
        ["activation_id", "asset_id", "candle_timestamp"],
    )
    op.drop_column("live_strategy_observations", "evaluation_attempt")
