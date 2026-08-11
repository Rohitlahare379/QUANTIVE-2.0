"""gap_staging

Revision ID: 004_gap_staging
Revises: 003_api_keys
Create Date: 2026-06-02 12:49:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '004_gap_staging'
down_revision = '003_api_keys'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.create_table('gap_staging_candles',
        sa.Column('asset_id', sa.Integer(), nullable=False),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('open', sa.Float(), nullable=False),
        sa.Column('high', sa.Float(), nullable=False),
        sa.Column('low', sa.Float(), nullable=False),
        sa.Column('close', sa.Float(), nullable=False),
        sa.Column('volume', sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(['asset_id'], ['asset_registry.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('asset_id', 'timestamp')
    )

def downgrade() -> None:
    op.drop_table('gap_staging_candles')
