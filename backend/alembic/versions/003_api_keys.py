"""api_keys

Revision ID: 003_api_keys
Revises: 002_continuous_aggregates
Create Date: 2026-06-02 12:35:00.000000

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '003_api_keys'
down_revision = '002_continuous_aggregates'
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.create_table('api_keys',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('key_hash', sa.String(), nullable=False),
        sa.Column('owner_name', sa.String(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.text('now()')),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_api_keys_id'), 'api_keys', ['id'], unique=False)
    op.create_index(op.f('ix_api_keys_key_hash'), 'api_keys', ['key_hash'], unique=True)

def downgrade() -> None:
    op.drop_index(op.f('ix_api_keys_key_hash'), table_name='api_keys')
    op.drop_index(op.f('ix_api_keys_id'), table_name='api_keys')
    op.drop_table('api_keys')
