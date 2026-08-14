from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "Quantive API"
    ENVIRONMENT: str = "development"
    
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "quantive"
    POSTGRES_PORT: str = "5432"

    DB_POOL_SIZE: int = 50
    DB_MAX_OVERFLOW: int = 20

    # Binance Connector Settings (REST & WS)
    BINANCE_BASE_URL: str = "https://api.binance.com"
    BINANCE_TIMEOUT_SECONDS: float = 10.0
    BINANCE_MAX_RETRIES: int = 5
    BINANCE_RETRY_DELAY_SECONDS: float = 1.0
    BINANCE_WS_BASE_URL: str = "wss://stream.binance.com:9443"
    BINANCE_WS_RECONNECT_INITIAL_DELAY_SECONDS: float = 0.5
    BINANCE_WS_RECONNECT_MAX_DELAY_SECONDS: float = 30.0
    BINANCE_WS_RECONNECT_BACKOFF_FACTOR: float = 2.0
    BINANCE_WS_RECONNECT_JITTER_RATIO: float = 0.25
    BINANCE_WS_MAX_CONNECTION_LIFETIME_SECONDS: float = 82800.0  # 23 hours (Binance hard disconnects at 24h)
    BINANCE_WS_PING_INTERVAL_SECONDS: float = 180.0
    BINANCE_WS_PING_TIMEOUT_SECONDS: float = 20.0
    
    # Binance Rate Limiting (Token Bucket)
    # Binance limit: 1200 weight/min. We cap at 1000 for safety.
    BINANCE_GLOBAL_WEIGHT_CAPACITY: int = 1000
    BINANCE_GLOBAL_WEIGHT_REFILL_RATE: float = 16.0 # tokens per second

    # Redis/Worker Settings
    REDIS_URL: str = "redis://localhost:6379/0"
    DRAMATIQ_CONCURRENCY: int = 8
    
    # WebSocket Shard & Distributed Lease Settings (P0.2)
    WS_NUM_SHARDS: int = 8
    WS_LEASE_TTL_SECONDS: float = 15.0
    WS_HEARTBEAT_INTERVAL_SECONDS: float = 5.0

    # WebSocket Live Ingestion Pipeline Settings (P0.2 Phase 3)
    WS_QUEUE_MAXSIZE: int = 10000
    WS_BATCH_SIZE: int = 1000
    WS_BATCH_FLUSH_INTERVAL_MS: int = 1000
    WS_REGISTRY_CACHE_TTL_SECONDS: float = 60.0
    WS_QUEUE_WARNING_THRESHOLD: float = 0.75
    WS_QUEUE_DEGRADED_THRESHOLD: float = 0.90
    
    # S3 / MinIO Settings for Historical Exports
    AWS_ACCESS_KEY_ID: str = "minioadmin"
    AWS_SECRET_ACCESS_KEY: str = "minioadmin"
    AWS_REGION: str = "us-east-1"
    S3_ENDPOINT_URL: str = "http://localhost:9000" # Use MinIO by default for dev
    S3_EXPORT_BUCKET: str = "quantive-exports"
    S3_PRESIGNED_EXPIRY_SECONDS: int = 3600 # 1 hour
    WORKER_LOCK_TIMEOUT_SECONDS: int = 3600

    # Security Settings
    API_KEY_PEPPER: str = "default_development_pepper"

    @property
    def sqlalchemy_database_uri(self) -> str:
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    from pydantic import model_validator
    @model_validator(mode="after")
    def validate_ws_sharding_settings(self) -> "Settings":
        if self.WS_HEARTBEAT_INTERVAL_SECONDS >= self.WS_LEASE_TTL_SECONDS:
            raise ValueError(
                f"WS_HEARTBEAT_INTERVAL_SECONDS ({self.WS_HEARTBEAT_INTERVAL_SECONDS}) "
                f"must be strictly less than WS_LEASE_TTL_SECONDS ({self.WS_LEASE_TTL_SECONDS})"
            )
        if self.WS_NUM_SHARDS <= 0:
            raise ValueError(f"WS_NUM_SHARDS must be a positive integer, got {self.WS_NUM_SHARDS}")
        if self.WS_QUEUE_MAXSIZE <= 0:
            raise ValueError(f"WS_QUEUE_MAXSIZE must be a positive integer, got {self.WS_QUEUE_MAXSIZE}")
        if self.WS_BATCH_SIZE <= 0:
            raise ValueError(f"WS_BATCH_SIZE must be a positive integer, got {self.WS_BATCH_SIZE}")
        if self.WS_BATCH_FLUSH_INTERVAL_MS <= 0:
            raise ValueError(f"WS_BATCH_FLUSH_INTERVAL_MS must be a positive integer, got {self.WS_BATCH_FLUSH_INTERVAL_MS}")
        return self

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
