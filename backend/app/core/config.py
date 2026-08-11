from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PROJECT_NAME: str = "Quantive API"
    ENVIRONMENT: str = "development"
    
    POSTGRES_SERVER: str
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_DB: str
    POSTGRES_PORT: str

    DB_POOL_SIZE: int = 50
    DB_MAX_OVERFLOW: int = 20

    # Binance Connector Settings
    BINANCE_BASE_URL: str = "https://api.binance.com"
    BINANCE_TIMEOUT_SECONDS: float = 10.0
    BINANCE_MAX_RETRIES: int = 5
    BINANCE_RETRY_DELAY_SECONDS: float = 1.0
    
    # Binance Rate Limiting (Token Bucket)
    # Binance limit: 1200 weight/min. We cap at 1000 for safety.
    BINANCE_GLOBAL_WEIGHT_CAPACITY: int = 1000
    BINANCE_GLOBAL_WEIGHT_REFILL_RATE: float = 16.0 # tokens per second

    # Redis/Worker Settings
    REDIS_URL: str = "redis://localhost:6379/0"
    DRAMATIQ_CONCURRENCY: int = 8
    
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

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
