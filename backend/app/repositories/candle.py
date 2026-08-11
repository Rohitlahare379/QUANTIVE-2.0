from typing import AsyncGenerator
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

class CandleRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_candles_stream(
        self,
        table_model,
        asset_id: int,
        start_time: datetime,
        end_time: datetime,
        chunk_size: int = 5000
    ) -> AsyncGenerator:
        """
        Streams candles from the specified table model to minimize memory usage.
        """
        stmt = (
            select(table_model)
            .where(
                table_model.c.asset_id == asset_id,
                table_model.c.timestamp >= start_time,
                table_model.c.timestamp < end_time
            )
            .order_by(table_model.c.timestamp.asc())
            # Execution options can set yield_per but stream_scalars handles it 
            # if we use yield_per on execution.
        ).execution_options(yield_per=chunk_size)
        
        # We use stream() for SQLAlchemy 2.0 Async
        result = await self.db.stream(stmt)
        
        # When querying a Table object, it yields Row objects (tuples essentially)
        async for row in result:
            yield row
