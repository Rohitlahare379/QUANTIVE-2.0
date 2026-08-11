"""continuous aggregates

Revision ID: 002_continuous_aggregates
Revises: 001_init_schemas
Create Date: 2026-06-02 11:29:35.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '002_continuous_aggregates'
down_revision: Union[str, None] = '001_init_schemas'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 5m Aggregate
    op.execute("""
    CREATE MATERIALIZED VIEW candles_5m
    WITH (timescaledb.continuous) AS
    SELECT
        asset_id,
        time_bucket('5 minutes', timestamp) AS timestamp,
        first(open, timestamp) AS open,
        max(high) AS high,
        min(low) AS low,
        last(close, timestamp) AS close,
        sum(volume) AS volume
    FROM raw_1m_candles
    GROUP BY asset_id, time_bucket('5 minutes', timestamp);
    """)
    op.execute("SELECT add_continuous_aggregate_policy('candles_5m', start_offset => NULL, end_offset => INTERVAL '1 min', schedule_interval => INTERVAL '1 min');")

    # 15m Aggregate
    op.execute("""
    CREATE MATERIALIZED VIEW candles_15m
    WITH (timescaledb.continuous) AS
    SELECT
        asset_id,
        time_bucket('15 minutes', timestamp) AS timestamp,
        first(open, timestamp) AS open,
        max(high) AS high,
        min(low) AS low,
        last(close, timestamp) AS close,
        sum(volume) AS volume
    FROM raw_1m_candles
    GROUP BY asset_id, time_bucket('15 minutes', timestamp);
    """)
    op.execute("SELECT add_continuous_aggregate_policy('candles_15m', start_offset => NULL, end_offset => INTERVAL '1 min', schedule_interval => INTERVAL '5 min');")

    # 1h Aggregate
    op.execute("""
    CREATE MATERIALIZED VIEW candles_1h
    WITH (timescaledb.continuous) AS
    SELECT
        asset_id,
        time_bucket('1 hour', timestamp) AS timestamp,
        first(open, timestamp) AS open,
        max(high) AS high,
        min(low) AS low,
        last(close, timestamp) AS close,
        sum(volume) AS volume
    FROM raw_1m_candles
    GROUP BY asset_id, time_bucket('1 hour', timestamp);
    """)
    op.execute("SELECT add_continuous_aggregate_policy('candles_1h', start_offset => NULL, end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '15 min');")

    # 4h Aggregate
    op.execute("""
    CREATE MATERIALIZED VIEW candles_4h
    WITH (timescaledb.continuous) AS
    SELECT
        asset_id,
        time_bucket('4 hours', timestamp) AS timestamp,
        first(open, timestamp) AS open,
        max(high) AS high,
        min(low) AS low,
        last(close, timestamp) AS close,
        sum(volume) AS volume
    FROM raw_1m_candles
    GROUP BY asset_id, time_bucket('4 hours', timestamp);
    """)
    op.execute("SELECT add_continuous_aggregate_policy('candles_4h', start_offset => NULL, end_offset => INTERVAL '4 hours', schedule_interval => INTERVAL '1 hour');")

    # 1d Aggregate
    op.execute("""
    CREATE MATERIALIZED VIEW candles_1d
    WITH (timescaledb.continuous) AS
    SELECT
        asset_id,
        time_bucket('1 day', timestamp) AS timestamp,
        first(open, timestamp) AS open,
        max(high) AS high,
        min(low) AS low,
        last(close, timestamp) AS close,
        sum(volume) AS volume
    FROM raw_1m_candles
    GROUP BY asset_id, time_bucket('1 day', timestamp);
    """)
    op.execute("SELECT add_continuous_aggregate_policy('candles_1d', start_offset => NULL, end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '4 hours');")


def downgrade() -> None:
    op.execute("SELECT remove_continuous_aggregate_policy('candles_1d', if_exists => true);")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS candles_1d;")
    
    op.execute("SELECT remove_continuous_aggregate_policy('candles_4h', if_exists => true);")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS candles_4h;")
    
    op.execute("SELECT remove_continuous_aggregate_policy('candles_1h', if_exists => true);")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS candles_1h;")
    
    op.execute("SELECT remove_continuous_aggregate_policy('candles_15m', if_exists => true);")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS candles_15m;")
    
    op.execute("SELECT remove_continuous_aggregate_policy('candles_5m', if_exists => true);")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS candles_5m;")
