"""strategy catalog

Revision ID: 009_strategy_catalog
Revises: 008_gap_repair_jobs
Create Date: 2026-09-13 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "009_strategy_catalog"
down_revision = "008_gap_repair_jobs"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "strategies",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_key", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("description", sa.String(length=2000), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("strategy_key"),
    )
    op.create_index(op.f("ix_strategies_strategy_key"), "strategies", ["strategy_key"], unique=True)

    op.create_table(
        "strategy_versions",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("strategy_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("schema_version", sa.String(length=16), nullable=False),
        sa.Column("definition_hash", sa.String(length=64), nullable=False),
        sa.Column("canonical_definition", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("version_number > 0", name="ck_strategy_versions_positive_version"),
        sa.CheckConstraint("definition_hash ~ '^[0-9a-f]{64}$'", name="ck_strategy_versions_hash_format"),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategies.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("strategy_id", "version_number", name="uq_strategy_versions_strategy_version"),
        sa.UniqueConstraint("strategy_id", "definition_hash", name="uq_strategy_versions_strategy_hash"),
    )
    op.create_index(op.f("ix_strategy_versions_strategy_id"), "strategy_versions", ["strategy_id"], unique=False)

    # Reproducibility depends on content-addressed versions remaining immutable.
    # Strategy display metadata may evolve, but its machine key may not.
    op.execute(
        """
        CREATE FUNCTION prevent_strategy_version_mutation() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'strategy_versions are immutable';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategy_versions_immutable
        BEFORE UPDATE OR DELETE ON strategy_versions
        FOR EACH ROW EXECUTE FUNCTION prevent_strategy_version_mutation();
        """
    )
    op.execute(
        """
        CREATE FUNCTION prevent_strategy_key_mutation() RETURNS trigger AS $$
        BEGIN
            IF NEW.strategy_key IS DISTINCT FROM OLD.strategy_key THEN
                RAISE EXCEPTION 'strategies.strategy_key is immutable';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        """
        CREATE TRIGGER strategies_key_immutable
        BEFORE UPDATE ON strategies
        FOR EACH ROW EXECUTE FUNCTION prevent_strategy_key_mutation();
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS strategies_key_immutable ON strategies;")
    op.execute("DROP FUNCTION IF EXISTS prevent_strategy_key_mutation();")
    op.execute("DROP TRIGGER IF EXISTS strategy_versions_immutable ON strategy_versions;")
    op.execute("DROP FUNCTION IF EXISTS prevent_strategy_version_mutation();")
    op.drop_index(op.f("ix_strategy_versions_strategy_id"), table_name="strategy_versions")
    op.drop_table("strategy_versions")
    op.drop_index(op.f("ix_strategies_strategy_key"), table_name="strategies")
    op.drop_table("strategies")
