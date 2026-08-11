"""init schemas

Revision ID: 001_init_schemas
Revises: 
Create Date: 2026-06-01 10:35:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '001_init_schemas'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

def upgrade() -> None:
    # 1. Create asset_registry
    op.create_table(
        'asset_registry',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('symbol', sa.String(length=50), nullable=False),
        sa.Column('exchange', sa.String(length=50), nullable=False),
        sa.Column('asset_type', sa.String(length=50), nullable=False),
        sa.Column('is_active', sa.Boolean(), server_default='true', nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('symbol', 'exchange', name='uix_symbol_exchange')
    )
    op.create_index(op.f('ix_asset_registry_symbol'), 'asset_registry', ['symbol'], unique=False)

    # 2. Create sync_ranges
    op.execute('CREATE EXTENSION IF NOT EXISTS btree_gist;')
    op.create_table(
        'sync_ranges',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('asset_id', sa.Integer(), nullable=False),
        sa.Column('start_timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('end_timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('updated_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.CheckConstraint('start_timestamp <= end_timestamp', name='chk_valid_time_range'),
        sa.ForeignKeyConstraint(['asset_id'], ['asset_registry.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.execute(
        "ALTER TABLE sync_ranges ADD CONSTRAINT exclude_overlapping_ranges EXCLUDE USING gist (asset_id WITH =, tstzrange(start_timestamp, end_timestamp, '[]') WITH &&);"
    )
    op.create_index(
        'ix_sync_ranges_asset_time',
        'sync_ranges',
        ['asset_id', 'start_timestamp', 'end_timestamp'],
        unique=False
    )

    # 3. Create raw_1m_candles
    op.create_table(
        'raw_1m_candles',
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
    
    # 4. Convert raw_1m_candles to a TimescaleDB hypertable
    # The timestamp is the partitioning column. 1 day chunks are a good default for 1m candles.
    op.execute(
        "SELECT create_hypertable('raw_1m_candles', 'timestamp', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');"
    )
    
    # 5. Future Compression Policy Setup (Prepared but not enabled)
    # TimescaleDB best practices: segment by asset_id, order by timestamp DESC
    op.execute(
        "ALTER TABLE raw_1m_candles SET (timescaledb.compress, timescaledb.compress_segmentby = 'asset_id', timescaledb.compress_orderby = 'timestamp DESC');"
    )


def downgrade() -> None:
    op.drop_table('raw_1m_candles')
    op.drop_index('ix_sync_ranges_asset_time', table_name='sync_ranges')
    op.drop_table('sync_ranges')
    op.drop_index(op.f('ix_asset_registry_symbol'), table_name='asset_registry')
    op.drop_table('asset_registry')
