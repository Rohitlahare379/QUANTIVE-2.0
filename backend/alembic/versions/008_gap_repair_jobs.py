"""gap_repair_jobs

Revision ID: 008_gap_repair_jobs
Revises: 007_cagg_refresh_lease
Create Date: 2026-08-16 00:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '008_gap_repair_jobs'
down_revision: Union[str, None] = '007_cagg_refresh_lease'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    gap_repair_status = postgresql.ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELLED', name='gaprepairstatus', create_type=False)
    gap_repair_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        'gap_repair_jobs',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('asset_id', sa.Integer(), nullable=False),
        sa.Column('symbol', sa.String(length=50), nullable=False),
        sa.Column('start_time', sa.DateTime(timezone=True), nullable=False),
        sa.Column('end_time', sa.DateTime(timezone=True), nullable=False),
        sa.Column('status', gap_repair_status, server_default='PENDING', nullable=False),
        sa.Column('worker_id', sa.String(), nullable=True),
        sa.Column('claimed_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('lease_expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('retry_count', sa.Integer(), server_default='0', nullable=False),
        sa.Column('max_retries', sa.Integer(), server_default='5', nullable=False),
        sa.Column('error_message', sa.String(), nullable=True),
        sa.Column('error_category', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['asset_id'], ['asset_registry.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_gap_repair_jobs_id'), 'gap_repair_jobs', ['id'], unique=False)
    op.create_index(op.f('ix_gap_repair_jobs_asset_id'), 'gap_repair_jobs', ['asset_id'], unique=False)
    op.create_index(op.f('ix_gap_repair_jobs_status'), 'gap_repair_jobs', ['status'], unique=False)
    op.create_index(op.f('ix_gap_repair_jobs_lease_expires_at'), 'gap_repair_jobs', ['lease_expires_at'], unique=False)
    op.create_index(op.f('ix_gap_repair_jobs_created_at'), 'gap_repair_jobs', ['created_at'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_gap_repair_jobs_created_at'), table_name='gap_repair_jobs')
    op.drop_index(op.f('ix_gap_repair_jobs_lease_expires_at'), table_name='gap_repair_jobs')
    op.drop_index(op.f('ix_gap_repair_jobs_status'), table_name='gap_repair_jobs')
    op.drop_index(op.f('ix_gap_repair_jobs_asset_id'), table_name='gap_repair_jobs')
    op.drop_index(op.f('ix_gap_repair_jobs_id'), table_name='gap_repair_jobs')
    op.drop_table('gap_repair_jobs')

    gap_repair_status = postgresql.ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', 'CANCELLED', name='gaprepairstatus', create_type=False)
    gap_repair_status.drop(op.get_bind(), checkfirst=True)
