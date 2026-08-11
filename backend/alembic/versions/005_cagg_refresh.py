"""cagg_refresh

Revision ID: 005_cagg_refresh
Revises: 004_gap_staging
Create Date: 2026-06-02 13:00:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '005_cagg_refresh'
down_revision = '004_gap_staging'
branch_labels = None
depends_on = None

def upgrade() -> None:
    # Create Enum
    refresh_status = postgresql.ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', name='refreshstatus')
    refresh_status.create(op.get_bind())
    
    op.create_table('cagg_refresh_jobs',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('window_start', sa.DateTime(timezone=True), nullable=False),
        sa.Column('window_end', sa.DateTime(timezone=True), nullable=False),
        sa.Column('status', refresh_status, nullable=False),
        sa.Column('error_message', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_cagg_refresh_jobs_id'), 'cagg_refresh_jobs', ['id'], unique=False)
    op.create_index(op.f('ix_cagg_refresh_jobs_status'), 'cagg_refresh_jobs', ['status'], unique=False)

def downgrade() -> None:
    op.drop_index(op.f('ix_cagg_refresh_jobs_status'), table_name='cagg_refresh_jobs')
    op.drop_index(op.f('ix_cagg_refresh_jobs_id'), table_name='cagg_refresh_jobs')
    op.drop_table('cagg_refresh_jobs')
    
    refresh_status = postgresql.ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', name='refreshstatus')
    refresh_status.drop(op.get_bind())
