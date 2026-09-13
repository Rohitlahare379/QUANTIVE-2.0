"""add durable websocket shard persistence fencing

Revision ID: 019_ws_shard_persistence_fencing
Revises: 018_sync_range_delete_serialization
Create Date: 2026-09-14 00:00:00.000000

A Redis TTL lease is not a database write fence: a paused owner can resume
after expiry while a new owner is already active.  Each successful Redis lease
gets a monotonic fencing token and must register it here before it can persist
canonical candles.  Candle writes lock and verify this row in their own
transaction, so a lower/old generation cannot write after a higher generation
has registered.
"""

from alembic import op
import sqlalchemy as sa


revision = "019_ws_shard_persistence_fencing"
down_revision = "018_sync_range_delete_serialization"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ws_shard_persistence_fences",
        sa.Column("shard_id", sa.Integer(), nullable=False),
        sa.Column("fencing_token", sa.BigInteger(), nullable=False),
        sa.Column("claim_token", sa.String(length=64), nullable=False),
        sa.Column("worker_id", sa.String(length=255), nullable=False),
        sa.Column(
            "registered_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "fencing_token > 0", name="ck_ws_shard_persistence_fence_positive"
        ),
        sa.PrimaryKeyConstraint("shard_id"),
    )


def downgrade() -> None:
    op.drop_table("ws_shard_persistence_fences")
