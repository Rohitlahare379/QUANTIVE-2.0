"""export_jobs

Revision ID: 006_export_jobs
Revises: 005_cagg_refresh
Create Date: 2026-06-02 13:16:00.000000

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '006_export_jobs'
down_revision = '005_cagg_refresh'
branch_labels = None
depends_on = None

def upgrade() -> None:
    # Create Enum
    export_status = postgresql.ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', name='exportstatus')
    export_status.create(op.get_bind())
    
    op.create_table('export_jobs',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('asset_id', sa.Integer(), nullable=False),
        sa.Column('timeframe', sa.String(), nullable=False),
        sa.Column('start_time', sa.DateTime(timezone=True), nullable=False),
        sa.Column('end_time', sa.DateTime(timezone=True), nullable=False),
        sa.Column('status', export_status, nullable=False),
        sa.Column('s3_key', sa.String(), nullable=True),
        sa.Column('error_message', sa.String(), nullable=True),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['asset_id'], ['asset_registry.id'], ),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_export_jobs_id'), 'export_jobs', ['id'], unique=False)
    op.create_index(op.f('ix_export_jobs_status'), 'export_jobs', ['status'], unique=False)

def downgrade() -> None:
    op.drop_index(op.f('ix_export_jobs_status'), table_name='export_jobs')
    op.drop_index(op.f('ix_export_jobs_id'), table_name='export_jobs')
    op.drop_table('export_jobs')
    
    export_status = postgresql.ENUM('PENDING', 'PROCESSING', 'COMPLETED', 'FAILED', name='exportstatus')
    export_status.drop(op.get_bind())
