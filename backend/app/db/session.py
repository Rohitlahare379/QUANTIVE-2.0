from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from app.core.config import settings

engine = create_async_engine(
    settings.sqlalchemy_database_uri,
    echo=settings.ENVIRONMENT == "development",
    future=True,
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False, autoflush=False
)
async_session_maker = AsyncSessionLocal

# Note: For bulk ingestion of millions of candles, DO NOT use ORM models.
# Instead, use SQLAlchemy Core with `insert().values()` and `executemany` for maximum performance.
