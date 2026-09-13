"""make export jobs fenced, bounded, and recoverable

Revision ID: 020_export_durability
Revises: 019_ws_shard_persistence_fencing
Create Date: 2026-09-14 00:00:00.000000

An export previously changed ``PENDING`` to ``PROCESSING`` with an ordinary
read/update.  Concurrent Dramatiq deliveries could both run it, and a crash
could strand it in PROCESSING forever.  The durable worker now uses a
token-checked finite lease and bounded attempt count.  This migration supplies
the database state required to make that ownership boundary enforceable.
"""

from alembic import op
import sqlalchemy as sa


revision = "020_export_durability"
down_revision = "019_ws_shard_persistence_fencing"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("export_jobs", sa.Column("worker_id", sa.String(length=128), nullable=True))
    op.add_column("export_jobs", sa.Column("lease_token", sa.String(length=64), nullable=True))
    op.add_column("export_jobs", sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("export_jobs", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "export_jobs",
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
    )
    op.add_column(
        "export_jobs",
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default=sa.text("5")),
    )
    op.add_column("export_jobs", sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("export_jobs", sa.Column("error_category", sa.String(length=64), nullable=True))

    # Pre-lease PROCESSING rows have no owner that can be safely trusted after
    # upgrade.  Requeue them rather than preserving an unrecoverable state.
    op.execute(
        """
        UPDATE export_jobs
           SET status = 'PENDING'::exportstatus,
               worker_id = NULL,
               lease_token = NULL,
               claimed_at = NULL,
               lease_expires_at = NULL,
               next_attempt_at = NULL,
               error_category = COALESCE(error_category, 'legacy_processing_recovery'),
               error_message = COALESCE(error_message, 'requeued by export durability migration')
         WHERE status = 'PROCESSING'::exportstatus
        """
    )

    op.create_check_constraint(
        "ck_export_jobs_attempt_count_nonnegative",
        "export_jobs",
        "attempt_count >= 0",
    )
    op.create_check_constraint(
        "ck_export_jobs_max_attempts_positive",
        "export_jobs",
        "max_attempts > 0",
    )
    op.create_check_constraint(
        "ck_export_jobs_processing_claim_complete",
        "export_jobs",
        "status != 'PROCESSING'::exportstatus OR "
        "(worker_id IS NOT NULL AND lease_token IS NOT NULL AND claimed_at IS NOT NULL AND lease_expires_at IS NOT NULL)",
    )
    op.create_index(
        "ix_export_jobs_ready",
        "export_jobs",
        ["status", "next_attempt_at", "created_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_export_jobs_lease_expires_at",
        "export_jobs",
        ["lease_expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_export_jobs_lease_expires_at", table_name="export_jobs")
    op.drop_index("ix_export_jobs_ready", table_name="export_jobs")
    op.drop_constraint("ck_export_jobs_processing_claim_complete", "export_jobs", type_="check")
    op.drop_constraint("ck_export_jobs_max_attempts_positive", "export_jobs", type_="check")
    op.drop_constraint("ck_export_jobs_attempt_count_nonnegative", "export_jobs", type_="check")
    op.drop_column("export_jobs", "error_category")
    op.drop_column("export_jobs", "next_attempt_at")
    op.drop_column("export_jobs", "max_attempts")
    op.drop_column("export_jobs", "attempt_count")
    op.drop_column("export_jobs", "lease_expires_at")
    op.drop_column("export_jobs", "claimed_at")
    op.drop_column("export_jobs", "lease_token")
    op.drop_column("export_jobs", "worker_id")
