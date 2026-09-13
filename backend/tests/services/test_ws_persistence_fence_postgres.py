"""Real PostgreSQL concurrency regressions for WebSocket write fencing.

These are deliberately infrastructure-backed.  fakeredis proves lease-token
generation but cannot prove that the successor registration and old database
write are serialized by PostgreSQL row locks.
"""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.models.ws_shard_persistence_fence import WsShardPersistenceFence
from app.services.ws_sharding.lease import ShardLeaseClaim
from app.services.ws_sharding.persistence_fence import (
    ShardPersistenceFenceLostError,
    ShardPersistenceFencer,
)


pytestmark = [pytest.mark.postgres, pytest.mark.timescaledb]

engine = create_async_engine(settings.sqlalchemy_database_uri, poolclass=NullPool)
Session = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


def claim(*, worker_id: str, token: str, fencing_token: int) -> ShardLeaseClaim:
    now = datetime.now(timezone.utc)
    return ShardLeaseClaim(
        shard_id=91,
        worker_id=worker_id,
        claim_token=token,
        claimed_at=now,
        lease_expires_at=now + timedelta(seconds=30),
        fencing_token=fencing_token,
    )


@pytest.fixture(autouse=True)
async def clean_persistence_fences():
    async with Session() as session:
        await session.execute(delete(WsShardPersistenceFence))
        await session.commit()
    yield
    async with Session() as session:
        await session.execute(delete(WsShardPersistenceFence))
        await session.commit()


@pytest.mark.asyncio
async def test_successor_registration_waits_for_old_write_then_fences_old_generation():
    """A paused old owner cannot write after a higher fencing generation starts.

    The old writer locks and validates the row as part of its simulated candle
    transaction.  The new owner must wait for that exact transaction, then it
    atomically advances the generation; subsequent old-owner transactions fail
    before any canonical persistence can occur.
    """
    old = ShardPersistenceFencer(Session, claim(worker_id="old", token="old-token", fencing_token=1))
    new = ShardPersistenceFencer(Session, claim(worker_id="new", token="new-token", fencing_token=2))
    assert await old.register_claim() is True

    old_session = Session()
    transaction = await old_session.begin()
    try:
        await old.assert_current(old_session)

        successor_task = asyncio.create_task(new.register_claim())
        await asyncio.sleep(0.1)
        # The successor cannot begin until an in-flight authenticated old
        # write commits or rolls back.  This is the no-overlap property.
        assert not successor_task.done()

        await transaction.commit()
        assert await asyncio.wait_for(successor_task, timeout=5) is True
    finally:
        if transaction.is_active:
            await transaction.rollback()
        await old_session.close()

    async with Session() as session:
        async with session.begin():
            with pytest.raises(ShardPersistenceFenceLostError, match="ownership lost"):
                await old.assert_current(session)

    async with Session() as session:
        async with session.begin():
            await new.assert_current(session)
