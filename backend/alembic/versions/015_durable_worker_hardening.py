"""durable worker fencing, bounded scheduling, and retry state

Revision ID: 015_durable_worker_hardening
Revises: 014_data_integrity_hardening
Create Date: 2026-09-13 00:00:00.000000

Lease expiry without a distinct claim generation lets a paused worker resume
after another worker has recovered the same job.  This migration adds opaque
per-claim tokens, durable retry not-before timestamps, overlap backstops, and
per-day historical merge jobs.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "015_durable_worker_hardening"
down_revision = "014_data_integrity_hardening"
branch_labels = None
depends_on = None


def _retire_overlapping_active_jobs(table: str, scope: str, range_expr: str, enum_name: str) -> None:
    """Retire legacy duplicate active work before installing an exclusion key.

    The next bounded scanner will re-create any uncovered remainder.  Retiring
    rather than silently deleting retains a durable audit trail and avoids an
    irreversible data loss during migration.
    """
    op.execute(
        f"""
        WITH duplicate_jobs AS (
            SELECT DISTINCT newer.id
            FROM {table} AS newer
            JOIN {table} AS older
              ON older.id < newer.id
             AND older.status IN ('PENDING'::{enum_name}, 'PROCESSING'::{enum_name})
             AND newer.status IN ('PENDING'::{enum_name}, 'PROCESSING'::{enum_name})
             AND {scope}
             AND {range_expr}
        )
        UPDATE {table}
           SET status = 'FAILED'::{enum_name},
               error_message = COALESCE(error_message, 'retired by durable worker overlap migration'),
               lease_expires_at = NULL,
               worker_id = NULL
         WHERE id IN (SELECT id FROM duplicate_jobs)
        """
    )


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    # PostgreSQL enums cannot remove values safely during downgrade.  The
    # forward-only state parks historical REST work until raw promotion proves
    # coverage, avoiding a REST retry loop against staging-only rows.
    # PostgreSQL deliberately forbids use of an enum value in a constraint in
    # the same transaction that adds it.  The exclusion constraint below uses
    # AWAITING_MERGE, so this must be an Alembic autocommit boundary or a fresh
    # deployment cannot get past this migration.
    with op.get_context().autocommit_block():
        op.execute("ALTER TYPE gaprepairstatus ADD VALUE IF NOT EXISTS 'AWAITING_MERGE'")

    op.add_column("gap_repair_jobs", sa.Column("lease_token", sa.String(length=64), nullable=True))
    op.add_column("gap_repair_jobs", sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True))
    op.create_check_constraint(
        "ck_gap_repair_jobs_valid_window",
        "gap_repair_jobs",
        "start_time < end_time",
    )
    op.create_check_constraint(
        "ck_gap_repair_jobs_retry_count_nonnegative",
        "gap_repair_jobs",
        "retry_count >= 0",
    )
    op.create_check_constraint(
        "ck_gap_repair_jobs_max_retries_nonnegative",
        "gap_repair_jobs",
        "max_retries >= 0",
    )
    op.create_index(
        "ix_gap_repair_jobs_ready",
        "gap_repair_jobs",
        ["status", "next_attempt_at", "created_at", "id"],
        unique=False,
    )
    _retire_overlapping_active_jobs(
        "gap_repair_jobs",
        "older.asset_id = newer.asset_id",
        "tstzrange(older.start_time, older.end_time, '[)') && tstzrange(newer.start_time, newer.end_time, '[)')",
        "gaprepairstatus",
    )
    op.execute(
        """
        ALTER TABLE gap_repair_jobs
        ADD CONSTRAINT exclude_active_gap_repair_jobs
        EXCLUDE USING gist (
            asset_id WITH =,
            tstzrange(start_time, end_time, '[)') WITH &&
        ) WHERE (status IN (
            'PENDING'::gaprepairstatus,
            'PROCESSING'::gaprepairstatus,
            'AWAITING_MERGE'::gaprepairstatus
        ))
        """
    )

    op.add_column("cagg_refresh_jobs", sa.Column("lease_token", sa.String(length=64), nullable=True))
    op.add_column(
        "cagg_refresh_jobs",
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
    )
    op.add_column(
        "cagg_refresh_jobs",
        sa.Column("max_retries", sa.Integer(), nullable=False, server_default=sa.text("5")),
    )
    op.add_column("cagg_refresh_jobs", sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("cagg_refresh_jobs", sa.Column("error_category", sa.String(length=64), nullable=True))
    op.create_check_constraint(
        "ck_cagg_refresh_jobs_valid_window",
        "cagg_refresh_jobs",
        "window_start < window_end",
    )
    op.create_check_constraint(
        "ck_cagg_refresh_jobs_retry_count_nonnegative",
        "cagg_refresh_jobs",
        "retry_count >= 0",
    )
    op.create_check_constraint(
        "ck_cagg_refresh_jobs_max_retries_nonnegative",
        "cagg_refresh_jobs",
        "max_retries >= 0",
    )
    op.create_index(
        "ix_cagg_refresh_jobs_ready",
        "cagg_refresh_jobs",
        ["status", "next_attempt_at", "created_at", "id"],
        unique=False,
    )
    _retire_overlapping_active_jobs(
        "cagg_refresh_jobs",
        "TRUE",
        "tstzrange(older.window_start, older.window_end, '[)') && tstzrange(newer.window_start, newer.window_end, '[)')",
        "refreshstatus",
    )
    op.execute(
        """
        ALTER TABLE cagg_refresh_jobs
        ADD CONSTRAINT exclude_active_cagg_refresh_jobs
        EXCLUDE USING gist (
            tstzrange(window_start, window_end, '[)') WITH &&
        ) WHERE (status IN ('PENDING'::refreshstatus, 'PROCESSING'::refreshstatus))
        """
    )

    historical_merge_status = postgresql.ENUM(
        "PENDING", "PROCESSING", "COMPLETED", "FAILED",
        name="historicalmergestatus",
        create_type=False,
    )
    historical_merge_status.create(op.get_bind(), checkfirst=True)
    op.create_table(
        "historical_merge_jobs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("day_bucket", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", historical_merge_status, nullable=False, server_default="PENDING"),
        sa.Column("worker_id", sa.String(), nullable=True),
        sa.Column("lease_token", sa.String(length=64), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("max_retries", sa.Integer(), nullable=False, server_default=sa.text("5")),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.String(), nullable=True),
        sa.Column("error_category", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.CheckConstraint("retry_count >= 0", name="ck_historical_merge_jobs_retry_count_nonnegative"),
        sa.CheckConstraint("max_retries >= 0", name="ck_historical_merge_jobs_max_retries_nonnegative"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("day_bucket", name="uq_historical_merge_jobs_day_bucket"),
    )
    op.create_index(
        "ix_historical_merge_jobs_ready",
        "historical_merge_jobs",
        ["status", "next_attempt_at", "created_at", "id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_historical_merge_jobs_ready", table_name="historical_merge_jobs")
    op.drop_table("historical_merge_jobs")
    historical_merge_status = postgresql.ENUM(
        "PENDING", "PROCESSING", "COMPLETED", "FAILED",
        name="historicalmergestatus",
        create_type=False,
    )
    historical_merge_status.drop(op.get_bind(), checkfirst=True)

    op.execute(
        "ALTER TABLE cagg_refresh_jobs DROP CONSTRAINT IF EXISTS exclude_active_cagg_refresh_jobs"
    )
    op.drop_index("ix_cagg_refresh_jobs_ready", table_name="cagg_refresh_jobs")
    op.drop_constraint("ck_cagg_refresh_jobs_max_retries_nonnegative", "cagg_refresh_jobs", type_="check")
    op.drop_constraint("ck_cagg_refresh_jobs_retry_count_nonnegative", "cagg_refresh_jobs", type_="check")
    op.drop_constraint("ck_cagg_refresh_jobs_valid_window", "cagg_refresh_jobs", type_="check")
    op.drop_column("cagg_refresh_jobs", "error_category")
    op.drop_column("cagg_refresh_jobs", "next_attempt_at")
    op.drop_column("cagg_refresh_jobs", "max_retries")
    op.drop_column("cagg_refresh_jobs", "retry_count")
    op.drop_column("cagg_refresh_jobs", "lease_token")

    op.execute(
        "ALTER TABLE gap_repair_jobs DROP CONSTRAINT IF EXISTS exclude_active_gap_repair_jobs"
    )
    op.drop_index("ix_gap_repair_jobs_ready", table_name="gap_repair_jobs")
    op.drop_constraint("ck_gap_repair_jobs_max_retries_nonnegative", "gap_repair_jobs", type_="check")
    op.drop_constraint("ck_gap_repair_jobs_retry_count_nonnegative", "gap_repair_jobs", type_="check")
    op.drop_constraint("ck_gap_repair_jobs_valid_window", "gap_repair_jobs", type_="check")
    op.drop_column("gap_repair_jobs", "next_attempt_at")
    op.drop_column("gap_repair_jobs", "lease_token")
