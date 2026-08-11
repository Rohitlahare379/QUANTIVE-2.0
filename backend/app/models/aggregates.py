from sqlalchemy import Table, Column, Integer, Float, DateTime, MetaData
from .base import Base

metadata = Base.metadata

def create_aggregate_table(name: str) -> Table:
    return Table(
        name,
        metadata,
        Column('asset_id', Integer, primary_key=True),
        Column('timestamp', DateTime(timezone=True), primary_key=True),
        Column('open', Float),
        Column('high', Float),
        Column('low', Float),
        Column('close', Float),
        Column('volume', Float),
        extend_existing=True
    )

candles_5m = create_aggregate_table('candles_5m')
candles_15m = create_aggregate_table('candles_15m')
candles_1h = create_aggregate_table('candles_1h')
candles_4h = create_aggregate_table('candles_4h')
candles_1d = create_aggregate_table('candles_1d')
