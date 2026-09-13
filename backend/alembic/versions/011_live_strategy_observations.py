"""live strategy observations

Revision ID: 011_live_strategy_observations
Revises: 010_backtest_results
Create Date: 2026-09-13 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "011_live_strategy_observations"
down_revision = "010_backtest_results"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_strategy_activations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_version_number", sa.Integer(), nullable=False),
        sa.Column("strategy_definition_hash", sa.String(length=64), nullable=False),
        sa.Column("configuration", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("configuration_hash", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("health", sa.String(length=16), nullable=False),
        sa.Column("last_error", sa.String(length=2000), nullable=True),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("deactivated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("asset_cursor", sa.Integer(), nullable=True),
        sa.Column("open_position_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("strategy_version_number > 0", name="ck_live_activations_positive_version"),
        sa.CheckConstraint(
            "strategy_definition_hash ~ '^[0-9a-f]{64}$'",
            name="ck_live_activations_definition_hash_format",
        ),
        sa.CheckConstraint(
            "configuration_hash ~ '^[0-9a-f]{64}$'",
            name="ck_live_activations_configuration_hash_format",
        ),
        sa.CheckConstraint(
            "status IN ('active', 'deactivated', 'error')",
            name="ck_live_activations_status",
        ),
        sa.CheckConstraint(
            "health IN ('healthy', 'degraded', 'error', 'stopped')",
            name="ck_live_activations_health",
        ),
        sa.CheckConstraint(
            "open_position_count >= 0",
            name="ck_live_activations_nonnegative_positions",
        ),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategies.id"]),
        sa.ForeignKeyConstraint(
            ["strategy_version_id", "strategy_id"],
            ["strategy_versions.id", "strategy_versions.strategy_id"],
            name="fk_live_activations_strategy_version_strategy",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_live_strategy_activations_strategy_id"),
        "live_strategy_activations",
        ["strategy_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_live_strategy_activations_strategy_version_id"),
        "live_strategy_activations",
        ["strategy_version_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_live_strategy_activations_status"),
        "live_strategy_activations",
        ["status"],
        unique=False,
    )
    op.create_index(
        "uq_live_strategy_activations_active_binding",
        "live_strategy_activations",
        ["strategy_version_id", "configuration_hash"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )

    op.create_table(
        "live_strategy_states",
        sa.Column("activation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("last_candle_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("position_state", sa.String(length=8), nullable=False),
        sa.Column("position_changed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("health", sa.String(length=16), nullable=False),
        sa.Column("consecutive_errors", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.String(length=2000), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "position_state IN ('flat', 'long', 'short')",
            name="ck_live_states_position_state",
        ),
        sa.CheckConstraint(
            "health IN ('healthy', 'degraded', 'error', 'stopped')",
            name="ck_live_states_health",
        ),
        sa.CheckConstraint("consecutive_errors >= 0", name="ck_live_states_nonnegative_errors"),
        sa.ForeignKeyConstraint(["activation_id"], ["live_strategy_activations.id"]),
        sa.ForeignKeyConstraint(["asset_id"], ["asset_registry.id"]),
        sa.PrimaryKeyConstraint("activation_id", "asset_id"),
    )

    op.create_table(
        "live_strategy_observations",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("activation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("candle_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("signal_id", sa.String(length=64), nullable=True),
        sa.Column("position_before", sa.String(length=8), nullable=False),
        sa.Column("position_after", sa.String(length=8), nullable=False),
        sa.Column("evaluation_latency_ms", sa.Float(), nullable=False),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("error_message", sa.String(length=2000), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "status IN ('evaluated', 'duplicate', 'out_of_order', 'gap_detected', 'invalid', 'error')",
            name="ck_live_observations_status",
        ),
        sa.CheckConstraint(
            "position_before IN ('flat', 'long', 'short') AND position_after IN ('flat', 'long', 'short')",
            name="ck_live_observations_position_state",
        ),
        sa.CheckConstraint("evaluation_latency_ms >= 0", name="ck_live_observations_nonnegative_latency"),
        sa.CheckConstraint(
            "(error_code IS NULL) = (error_message IS NULL)",
            name="ck_live_observations_error_pair",
        ),
        sa.ForeignKeyConstraint(["activation_id"], ["live_strategy_activations.id"]),
        sa.ForeignKeyConstraint(["asset_id"], ["asset_registry.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "activation_id",
            "asset_id",
            "candle_timestamp",
            name="uq_live_observations_activation_asset_candle",
        ),
    )
    op.create_index(
        op.f("ix_live_strategy_observations_activation_id"),
        "live_strategy_observations",
        ["activation_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_live_strategy_observations_asset_id"),
        "live_strategy_observations",
        ["asset_id"],
        unique=False,
    )

    op.create_table(
        "live_strategy_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("observation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("activation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=False),
        sa.Column("candle_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("event_type", sa.String(length=16), nullable=False),
        sa.Column("signal_id", sa.String(length=64), nullable=False),
        sa.Column("position_state", sa.String(length=8), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "event_type IN ('signal', 'entry', 'exit')",
            name="ck_live_events_event_type",
        ),
        sa.CheckConstraint(
            "position_state IN ('flat', 'long', 'short')",
            name="ck_live_events_position_state",
        ),
        sa.ForeignKeyConstraint(["observation_id"], ["live_strategy_observations.id"]),
        sa.ForeignKeyConstraint(["activation_id"], ["live_strategy_activations.id"]),
        sa.ForeignKeyConstraint(["asset_id"], ["asset_registry.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "activation_id",
            "asset_id",
            "candle_timestamp",
            "event_type",
            "signal_id",
            name="uq_live_events_activation_asset_candle_event",
        ),
    )
    op.create_index(
        op.f("ix_live_strategy_events_observation_id"),
        "live_strategy_events",
        ["observation_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_live_strategy_events_activation_id"),
        "live_strategy_events",
        ["activation_id"],
        unique=False,
    )

    op.execute(
        """
        CREATE FUNCTION validate_live_activation_strategy_snapshot() RETURNS trigger AS $$
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
                RAISE EXCEPTION 'live strategy activation references an unknown strategy version';
            END IF;
            IF NEW.strategy_version_number <> stored_version_number
               OR NEW.strategy_definition_hash <> stored_definition_hash THEN
                RAISE EXCEPTION 'live strategy activation version snapshot does not match strategy_versions';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER live_strategy_activation_version_snapshot_valid
        BEFORE INSERT ON live_strategy_activations
        FOR EACH ROW EXECUTE FUNCTION validate_live_activation_strategy_snapshot();
        """
    )
    op.execute(
        """
        CREATE FUNCTION prevent_live_activation_binding_mutation() RETURNS trigger AS $$
        BEGIN
            IF NEW.strategy_id IS DISTINCT FROM OLD.strategy_id
               OR NEW.strategy_version_id IS DISTINCT FROM OLD.strategy_version_id
               OR NEW.strategy_version_number IS DISTINCT FROM OLD.strategy_version_number
               OR NEW.strategy_definition_hash IS DISTINCT FROM OLD.strategy_definition_hash
               OR NEW.configuration IS DISTINCT FROM OLD.configuration
               OR NEW.configuration_hash IS DISTINCT FROM OLD.configuration_hash THEN
                RAISE EXCEPTION 'live strategy activation binding is immutable';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER live_strategy_activation_binding_immutable
        BEFORE UPDATE ON live_strategy_activations
        FOR EACH ROW EXECUTE FUNCTION prevent_live_activation_binding_mutation();
        """
    )
    op.execute(
        """
        CREATE FUNCTION prevent_live_observation_mutation() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'live strategy observations and events are immutable';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    for table_name in ("live_strategy_observations", "live_strategy_events"):
        op.execute(
            f"""
            CREATE TRIGGER {table_name}_immutable
            BEFORE UPDATE OR DELETE ON {table_name}
            FOR EACH ROW EXECUTE FUNCTION prevent_live_observation_mutation();
            """
        )


def downgrade() -> None:
    for table_name in ("live_strategy_events", "live_strategy_observations"):
        op.execute(f"DROP TRIGGER IF EXISTS {table_name}_immutable ON {table_name};")
    op.execute("DROP FUNCTION IF EXISTS prevent_live_observation_mutation();")
    op.execute("DROP TRIGGER IF EXISTS live_strategy_activation_binding_immutable ON live_strategy_activations;")
    op.execute("DROP FUNCTION IF EXISTS prevent_live_activation_binding_mutation();")
    op.execute("DROP TRIGGER IF EXISTS live_strategy_activation_version_snapshot_valid ON live_strategy_activations;")
    op.execute("DROP FUNCTION IF EXISTS validate_live_activation_strategy_snapshot();")
    op.drop_index(op.f("ix_live_strategy_events_activation_id"), table_name="live_strategy_events")
    op.drop_index(op.f("ix_live_strategy_events_observation_id"), table_name="live_strategy_events")
    op.drop_table("live_strategy_events")
    op.drop_index(op.f("ix_live_strategy_observations_asset_id"), table_name="live_strategy_observations")
    op.drop_index(op.f("ix_live_strategy_observations_activation_id"), table_name="live_strategy_observations")
    op.drop_table("live_strategy_observations")
    op.drop_table("live_strategy_states")
    op.drop_index(
        "uq_live_strategy_activations_active_binding",
        table_name="live_strategy_activations",
    )
    op.drop_index(op.f("ix_live_strategy_activations_status"), table_name="live_strategy_activations")
    op.drop_index(op.f("ix_live_strategy_activations_strategy_version_id"), table_name="live_strategy_activations")
    op.drop_index(op.f("ix_live_strategy_activations_strategy_id"), table_name="live_strategy_activations")
    op.drop_table("live_strategy_activations")
