"""Durable persistence fence for WebSocket shard ownership generations.

Redis leases answer whether a worker may continue receiving a shard right now.
They cannot, by themselves, stop a process paused past the lease TTL from
resuming after a successor has begun writing.  This one-row-per-shard table is
the PostgreSQL-side fencing authority checked inside each candle transaction.
"""

from datetime import datetime

from sqlalchemy import BigInteger, CheckConstraint, DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class WsShardPersistenceFence(Base):
    __tablename__ = "ws_shard_persistence_fences"

    shard_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    fencing_token: Mapped[int] = mapped_column(BigInteger, nullable=False)
    claim_token: Mapped[str] = mapped_column(String(64), nullable=False)
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False)
    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("fencing_token > 0", name="ck_ws_shard_persistence_fence_positive"),
    )
