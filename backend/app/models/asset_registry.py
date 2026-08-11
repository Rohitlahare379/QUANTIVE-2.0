from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Boolean, DateTime, func, UniqueConstraint
from .base import Base

class AssetRegistry(Base):
    __tablename__ = "asset_registry"
    
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    exchange: Mapped[str] = mapped_column(String(50), nullable=False)
    asset_type: Mapped[str] = mapped_column(String(50), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint("symbol", "exchange", name="uix_symbol_exchange"),
    )
