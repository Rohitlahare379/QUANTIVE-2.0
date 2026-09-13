"""PostgreSQL write fencing for Redis-owned WebSocket shards.

The Redis lease is intentionally short lived.  A worker can still be paused
long enough for another worker to take the lease, so persistence must verify a
monotonic ownership generation inside the same transaction as canonical candle
writes.  This module supplies that boundary without making ingestion depend on
Redis during its database transaction.
"""

from sqlalchemy import and_, func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.ws_shard_persistence_fence import WsShardPersistenceFence
from app.services.ws_sharding.lease import ShardLeaseClaim


class ShardPersistenceFenceLostError(RuntimeError):
    """Raised when an ownership generation is no longer permitted to write."""


class ShardPersistenceFencer:
    """Registers and verifies one immutable Redis lease claim generation."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        claim: ShardLeaseClaim,
    ) -> None:
        if claim.fencing_token <= 0:
            raise ValueError("persistence-fenced shard claims require a positive fencing token")
        self.session_factory = session_factory
        self.claim = claim

    async def register_claim(self) -> bool:
        """Make this generation current, but never regress a newer generation.

        ``ON CONFLICT ... WHERE`` is a single database decision.  A paused
        stale owner can therefore never overwrite a successor that has already
        registered its higher Redis fencing token.
        """
        async with self.session_factory() as session:
            async with session.begin():
                statement = insert(WsShardPersistenceFence).values(
                    shard_id=self.claim.shard_id,
                    fencing_token=self.claim.fencing_token,
                    claim_token=self.claim.claim_token,
                    worker_id=self.claim.worker_id,
                )
                excluded = statement.excluded
                statement = statement.on_conflict_do_update(
                    index_elements=[WsShardPersistenceFence.shard_id],
                    set_={
                        "fencing_token": excluded.fencing_token,
                        "claim_token": excluded.claim_token,
                        "worker_id": excluded.worker_id,
                        "registered_at": func.now(),
                    },
                    where=or_(
                        WsShardPersistenceFence.fencing_token < excluded.fencing_token,
                        and_(
                            WsShardPersistenceFence.fencing_token == excluded.fencing_token,
                            WsShardPersistenceFence.claim_token == excluded.claim_token,
                            WsShardPersistenceFence.worker_id == excluded.worker_id,
                        ),
                    ),
                ).returning(
                    WsShardPersistenceFence.fencing_token,
                    WsShardPersistenceFence.claim_token,
                    WsShardPersistenceFence.worker_id,
                )
                row = (await session.execute(statement)).one_or_none()

        return bool(
            row
            and row.fencing_token == self.claim.fencing_token
            and row.claim_token == self.claim.claim_token
            and row.worker_id == self.claim.worker_id
        )

    async def assert_current(self, session: AsyncSession) -> None:
        """Lock and prove this claim is the current persistence generation.

        Call only after the enclosing write transaction has begun.  The row
        lock serializes a successor's registration with this persistence unit:
        either the old write commits before the successor starts, or this old
        claim sees the successor and fails before it writes.
        """
        statement = (
            select(WsShardPersistenceFence.shard_id)
            .where(
                WsShardPersistenceFence.shard_id == self.claim.shard_id,
                WsShardPersistenceFence.fencing_token == self.claim.fencing_token,
                WsShardPersistenceFence.claim_token == self.claim.claim_token,
                WsShardPersistenceFence.worker_id == self.claim.worker_id,
            )
            .with_for_update()
        )
        row = (await session.execute(statement)).scalar_one_or_none()
        if row is None:
            raise ShardPersistenceFenceLostError(
                "WebSocket shard persistence ownership lost "
                f"(shard={self.claim.shard_id}, fencing_token={self.claim.fencing_token})"
            )
