"""strategy divergence comparison

Revision ID: 012_strategy_divergence
Revises: 011_live_strategy_observations
Create Date: 2026-09-13 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "012_strategy_divergence"
down_revision = "011_live_strategy_observations"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "strategy_comparison_runs",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("strategy_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_version_number", sa.Integer(), nullable=False),
        sa.Column("strategy_definition_hash", sa.String(length=64), nullable=False),
        sa.Column("activation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("reference_run_id", sa.String(length=64), nullable=False),
        sa.Column("reference_result_hash", sa.String(length=64), nullable=False),
        sa.Column("policy_id", sa.String(length=128), nullable=False),
        sa.Column("policy_revision", sa.String(length=128), nullable=False),
        sa.Column("policy_hash", sa.String(length=64), nullable=False),
        sa.Column("policy", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("reference_source", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("observed_source", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("char_length(run_id) = 64 AND run_id ~ '^[0-9a-f]{64}$'", name="ck_comparison_runs_id"),
        sa.CheckConstraint("strategy_version_number > 0", name="ck_comparison_runs_positive_version"),
        sa.CheckConstraint(
            "strategy_definition_hash ~ '^[0-9a-f]{64}$' AND reference_result_hash ~ '^[0-9a-f]{64}$' "
            "AND policy_hash ~ '^[0-9a-f]{64}$'",
            name="ck_comparison_runs_hashes",
        ),
        sa.CheckConstraint("window_end > window_start", name="ck_comparison_runs_window"),
        sa.CheckConstraint(
            "status IN ('healthy', 'divergent', 'insufficient_reference')",
            name="ck_comparison_runs_status",
        ),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategies.id"]),
        sa.ForeignKeyConstraint(
            ["strategy_version_id", "strategy_id"],
            ["strategy_versions.id", "strategy_versions.strategy_id"],
            name="fk_comparison_runs_strategy_version_strategy",
        ),
        sa.ForeignKeyConstraint(["activation_id"], ["live_strategy_activations.id"]),
        sa.ForeignKeyConstraint(["reference_run_id"], ["backtest_results.run_id"]),
        sa.PrimaryKeyConstraint("run_id"),
    )
    for name, column in (
        ("ix_strategy_comparison_runs_strategy_id", "strategy_id"),
        ("ix_strategy_comparison_runs_strategy_version_id", "strategy_version_id"),
        ("ix_strategy_comparison_runs_activation_id", "activation_id"),
        ("ix_strategy_comparison_runs_reference_run_id", "reference_run_id"),
    ):
        op.create_index(name, "strategy_comparison_runs", [column], unique=False)

    op.create_table(
        "strategy_divergence_findings",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("comparison_run_id", sa.String(length=64), nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=False),
        sa.Column("strategy_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_version_number", sa.Integer(), nullable=False),
        sa.Column("strategy_definition_hash", sa.String(length=64), nullable=False),
        sa.Column("asset_id", sa.Integer(), nullable=True),
        sa.Column("event_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("divergence_type", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), nullable=False),
        sa.Column("category", sa.String(length=128), nullable=False),
        sa.Column("expected_value", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("observed_value", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_reference", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("explanation", sa.String(length=2000), nullable=False),
        sa.Column("resolution_status", sa.String(length=32), nullable=False),
        sa.Column("resolution_note", sa.String(length=2000), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("fingerprint ~ '^[0-9a-f]{64}$'", name="ck_divergence_findings_fingerprint"),
        sa.CheckConstraint("strategy_version_number > 0", name="ck_divergence_findings_positive_version"),
        sa.CheckConstraint("strategy_definition_hash ~ '^[0-9a-f]{64}$'", name="ck_divergence_findings_definition_hash"),
        sa.CheckConstraint("window_end > window_start", name="ck_divergence_findings_window"),
        sa.CheckConstraint("severity IN ('info', 'warning', 'critical')", name="ck_divergence_findings_severity"),
        sa.CheckConstraint(
            "resolution_status IN ('open', 'acknowledged', 'resolved', 'not_applicable')",
            name="ck_divergence_findings_resolution",
        ),
        sa.ForeignKeyConstraint(["comparison_run_id"], ["strategy_comparison_runs.run_id"]),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategies.id"]),
        sa.ForeignKeyConstraint(
            ["strategy_version_id", "strategy_id"],
            ["strategy_versions.id", "strategy_versions.strategy_id"],
            name="fk_divergence_findings_strategy_version_strategy",
        ),
        sa.ForeignKeyConstraint(["asset_id"], ["asset_registry.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("comparison_run_id", "fingerprint", name="uq_divergence_findings_run_fingerprint"),
    )
    for name, column in (
        ("ix_strategy_divergence_findings_comparison_run_id", "comparison_run_id"),
        ("ix_strategy_divergence_findings_strategy_id", "strategy_id"),
        ("ix_strategy_divergence_findings_strategy_version_id", "strategy_version_id"),
        ("ix_strategy_divergence_findings_asset_id", "asset_id"),
        ("ix_strategy_divergence_findings_divergence_type", "divergence_type"),
    ):
        op.create_index(name, "strategy_divergence_findings", [column], unique=False)

    op.execute(
        """
        CREATE FUNCTION prevent_strategy_comparison_run_mutation() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'strategy comparison runs are immutable';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_comparison_runs_immutable
        BEFORE UPDATE OR DELETE ON strategy_comparison_runs
        FOR EACH ROW EXECUTE FUNCTION prevent_strategy_comparison_run_mutation();
        """
    )
    op.execute(
        """
        CREATE FUNCTION restrict_strategy_divergence_finding_mutation() RETURNS trigger AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'strategy divergence findings cannot be deleted';
            END IF;
            IF NEW.comparison_run_id IS DISTINCT FROM OLD.comparison_run_id
               OR NEW.fingerprint IS DISTINCT FROM OLD.fingerprint
               OR NEW.strategy_id IS DISTINCT FROM OLD.strategy_id
               OR NEW.strategy_version_id IS DISTINCT FROM OLD.strategy_version_id
               OR NEW.strategy_version_number IS DISTINCT FROM OLD.strategy_version_number
               OR NEW.strategy_definition_hash IS DISTINCT FROM OLD.strategy_definition_hash
               OR NEW.asset_id IS DISTINCT FROM OLD.asset_id
               OR NEW.event_timestamp IS DISTINCT FROM OLD.event_timestamp
               OR NEW.window_start IS DISTINCT FROM OLD.window_start
               OR NEW.window_end IS DISTINCT FROM OLD.window_end
               OR NEW.divergence_type IS DISTINCT FROM OLD.divergence_type
               OR NEW.severity IS DISTINCT FROM OLD.severity
               OR NEW.category IS DISTINCT FROM OLD.category
               OR NEW.expected_value IS DISTINCT FROM OLD.expected_value
               OR NEW.observed_value IS DISTINCT FROM OLD.observed_value
               OR NEW.source_reference IS DISTINCT FROM OLD.source_reference
               OR NEW.explanation IS DISTINCT FROM OLD.explanation THEN
                RAISE EXCEPTION 'forensic divergence evidence is immutable';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_divergence_findings_evidence_immutable
        BEFORE UPDATE OR DELETE ON strategy_divergence_findings
        FOR EACH ROW EXECUTE FUNCTION restrict_strategy_divergence_finding_mutation();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS strategy_divergence_findings_evidence_immutable ON strategy_divergence_findings;")
    op.execute("DROP FUNCTION IF EXISTS restrict_strategy_divergence_finding_mutation();")
    op.execute("DROP TRIGGER IF EXISTS strategy_comparison_runs_immutable ON strategy_comparison_runs;")
    op.execute("DROP FUNCTION IF EXISTS prevent_strategy_comparison_run_mutation();")
    for name in (
        "ix_strategy_divergence_findings_divergence_type",
        "ix_strategy_divergence_findings_asset_id",
        "ix_strategy_divergence_findings_strategy_version_id",
        "ix_strategy_divergence_findings_strategy_id",
        "ix_strategy_divergence_findings_comparison_run_id",
    ):
        op.drop_index(name, table_name="strategy_divergence_findings")
    op.drop_table("strategy_divergence_findings")
    for name in (
        "ix_strategy_comparison_runs_reference_run_id",
        "ix_strategy_comparison_runs_activation_id",
        "ix_strategy_comparison_runs_strategy_version_id",
        "ix_strategy_comparison_runs_strategy_id",
    ):
        op.drop_index(name, table_name="strategy_comparison_runs")
    op.drop_table("strategy_comparison_runs")
