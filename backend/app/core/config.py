from ipaddress import ip_network

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
    BINANCE_WS_SUBSCRIPTION_ACK_TIMEOUT_SECONDS: float = 10.0
    
    # Binance Rate Limiting (Token Bucket)
    # Binance limit: 1200 weight/min. We cap at 1000 for safety.
    BINANCE_GLOBAL_WEIGHT_CAPACITY: int = 1000
    BINANCE_GLOBAL_WEIGHT_REFILL_RATE: float = 16.0 # tokens per second

    # Redis/Worker Settings
    REDIS_URL: str = "redis://localhost:6379/0"
    # Redis owns distributed leases and the global Binance rate limiter.  A
    # black-holed TCP connection must become a bounded failure so owners fence
    # themselves rather than continuing past a lease TTL.
    REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS: float = 2.0
    REDIS_SOCKET_TIMEOUT_SECONDS: float = 5.0
    # Every independently constructed Redis client (broker, API limiter,
    # async lease/rate-limit client) must have a finite pool.  Otherwise an
    # outage combined with public readiness/metrics traffic can consume file
    # descriptors and hide the original dependency failure.
    REDIS_MAX_CONNECTIONS: int = 32
    REDIS_PROBE_MAX_CONCURRENCY: int = 4
    REDIS_PROBE_ACQUIRE_TIMEOUT_SECONDS: float = 1.0
    DRAMATIQ_CONCURRENCY: int = 8
    MAINTENANCE_INTERVAL_SECONDS: float = 60.0
    LIVE_STRATEGY_MAX_ACTIVATIONS_PER_CYCLE: int = 100
    LIVE_STRATEGY_MAX_ASSETS_PER_CYCLE: int = 100
    LIVE_STRATEGY_MAX_CANDLES_PER_ASSET_CYCLE: int = 100
    LIVE_STRATEGY_DISPATCH_LEASE_SECONDS: int = 300
    STRATEGY_COMPARISON_MAX_RECORDS: int = 10_000
    STRATEGY_COMPARISON_MAX_WINDOW_SECONDS: int = 3_600
    STRATEGY_COMPARISON_DISPATCH_LEASE_SECONDS: int = 300
    # Strategy monitoring consumes persisted comparison results.  These caps
    # bound one dispatch cycle even when a deployment has many active
    # strategies or an interrupted worker leaves a backlog behind.
    STRATEGY_MONITORING_MAX_BINDINGS_PER_CYCLE: int = 100
    STRATEGY_MONITORING_MAX_COMPARISONS_PER_BINDING: int = 100
    STRATEGY_MONITORING_FINDING_PAGE_SIZE: int = 1_000
    STRATEGY_MONITORING_DISPATCH_LEASE_SECONDS: int = 300
    STRATEGY_MONITORING_BINDING_LEASE_SECONDS: int = 300
    # Every durable maintenance actor is deliberately bounded.  Additional work
    # remains in PostgreSQL and is picked up by the next maintenance wake-up.
    GAP_REPAIR_MAX_JOBS_PER_RUN: int = 25
    GAP_REPAIR_MAX_ASSETS_PER_SCAN: int = 100
    GAP_REPAIR_MAX_JOBS_PER_SCAN: int = 500
    # Coverage metadata can be highly fragmented after an extended outage.
    # Never materialize an arbitrary number of sync ranges or repair gaps in a
    # worker invocation; the detector treats the unscanned suffix as
    # conservatively unresolved instead.
    GAP_REPAIR_MAX_SYNC_RANGES_PER_DETECTION: int = 1_000
    GAP_REPAIR_MAX_GAPS_PER_DETECTION: int = 500
    GAP_REPAIR_MAX_ACTIVE_OVERLAPS: int = 500
    CAGG_REFRESH_MAX_JOBS_PER_RUN: int = 25
    HISTORICAL_MERGE_MAX_JOBS_PER_RUN: int = 8
    HISTORICAL_MERGE_MAX_SCHEDULE_DAYS: int = 32
    HISTORICAL_MERGE_PAGE_SIZE: int = 1000
    
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
    WS_MAX_PENDING_PER_ASSET: int = 2000
    
    # S3 / MinIO Settings for Historical Exports
    AWS_ACCESS_KEY_ID: str = "minioadmin"
    AWS_SECRET_ACCESS_KEY: str = "minioadmin"
    AWS_REGION: str = "us-east-1"
    S3_ENDPOINT_URL: str = "http://localhost:9000" # Use MinIO by default for dev
    S3_EXPORT_BUCKET: str = "quantive-exports"
    S3_PRESIGNED_EXPIRY_SECONDS: int = 3600 # 1 hour
    S3_CONNECT_TIMEOUT_SECONDS: float = 5.0
    S3_READ_TIMEOUT_SECONDS: float = 60.0
    S3_MAX_ATTEMPTS: int = 3
    WORKER_LOCK_TIMEOUT_SECONDS: int = 3600

    # Export jobs write a local stream and then perform remote object storage
    # I/O.  They use a database-backed lease so duplicate/redelivered actor
    # messages cannot publish the same export concurrently, and a crashed
    # worker becomes reclaimable by the bounded maintenance sweep.
    EXPORT_LEASE_SECONDS: int = 300
    EXPORT_HEARTBEAT_INTERVAL_SECONDS: float = 30.0
    EXPORT_MAX_ATTEMPTS: int = 5
    EXPORT_MAX_JOBS_PER_RUN: int = 5
    EXPORT_RECOVERY_DISPATCH_LEASE_SECONDS: int = 300

    # Security Settings
    API_KEY_PEPPER: str = "default_development_pepper"
    # Comma-separated source networks allowed to supply forwarding headers.
    # Do not trust CF-Connecting-IP/X-Real-IP from arbitrary Internet clients:
    # they could choose a new rate-limit key on every request.  Production
    # deployments must list their actual ingress proxy networks explicitly.
    TRUSTED_PROXY_CIDRS: str = "127.0.0.1/32,::1/128"

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
        if self.BINANCE_WS_SUBSCRIPTION_ACK_TIMEOUT_SECONDS <= 0:
            raise ValueError(
                "BINANCE_WS_SUBSCRIPTION_ACK_TIMEOUT_SECONDS must be positive, "
                f"got {self.BINANCE_WS_SUBSCRIPTION_ACK_TIMEOUT_SECONDS}"
            )
        if self.BINANCE_GLOBAL_WEIGHT_CAPACITY <= 0:
            raise ValueError(
                "BINANCE_GLOBAL_WEIGHT_CAPACITY must be a positive integer, "
                f"got {self.BINANCE_GLOBAL_WEIGHT_CAPACITY}"
            )
        if self.BINANCE_GLOBAL_WEIGHT_REFILL_RATE <= 0:
            raise ValueError(
                "BINANCE_GLOBAL_WEIGHT_REFILL_RATE must be positive, "
                f"got {self.BINANCE_GLOBAL_WEIGHT_REFILL_RATE}"
            )
        if self.REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS <= 0:
            raise ValueError(
                "REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS must be positive, "
                f"got {self.REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS}"
            )
        if self.REDIS_SOCKET_TIMEOUT_SECONDS <= 0:
            raise ValueError(
                "REDIS_SOCKET_TIMEOUT_SECONDS must be positive, "
                f"got {self.REDIS_SOCKET_TIMEOUT_SECONDS}"
            )
        if self.REDIS_MAX_CONNECTIONS <= 0:
            raise ValueError(
                "REDIS_MAX_CONNECTIONS must be a positive integer, "
                f"got {self.REDIS_MAX_CONNECTIONS}"
            )
        if self.REDIS_PROBE_MAX_CONCURRENCY <= 0:
            raise ValueError(
                "REDIS_PROBE_MAX_CONCURRENCY must be a positive integer, "
                f"got {self.REDIS_PROBE_MAX_CONCURRENCY}"
            )
        if self.REDIS_PROBE_ACQUIRE_TIMEOUT_SECONDS <= 0:
            raise ValueError(
                "REDIS_PROBE_ACQUIRE_TIMEOUT_SECONDS must be positive, "
                f"got {self.REDIS_PROBE_ACQUIRE_TIMEOUT_SECONDS}"
            )
        if self.EXPORT_LEASE_SECONDS <= 0:
            raise ValueError(f"EXPORT_LEASE_SECONDS must be positive, got {self.EXPORT_LEASE_SECONDS}")
        if self.EXPORT_HEARTBEAT_INTERVAL_SECONDS <= 0:
            raise ValueError(
                "EXPORT_HEARTBEAT_INTERVAL_SECONDS must be positive, "
                f"got {self.EXPORT_HEARTBEAT_INTERVAL_SECONDS}"
            )
        if self.EXPORT_HEARTBEAT_INTERVAL_SECONDS >= self.EXPORT_LEASE_SECONDS:
            raise ValueError(
                "EXPORT_HEARTBEAT_INTERVAL_SECONDS must be strictly less than EXPORT_LEASE_SECONDS"
            )
        if self.S3_CONNECT_TIMEOUT_SECONDS <= 0 or self.S3_READ_TIMEOUT_SECONDS <= 0:
            raise ValueError("S3_CONNECT_TIMEOUT_SECONDS and S3_READ_TIMEOUT_SECONDS must be positive")
        if self.S3_MAX_ATTEMPTS <= 0:
            raise ValueError(f"S3_MAX_ATTEMPTS must be positive, got {self.S3_MAX_ATTEMPTS}")
        for name, value in (
            ("EXPORT_MAX_ATTEMPTS", self.EXPORT_MAX_ATTEMPTS),
            ("EXPORT_MAX_JOBS_PER_RUN", self.EXPORT_MAX_JOBS_PER_RUN),
            ("EXPORT_RECOVERY_DISPATCH_LEASE_SECONDS", self.EXPORT_RECOVERY_DISPATCH_LEASE_SECONDS),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be a positive integer, got {value}")
        proxy_networks = [value.strip() for value in self.TRUSTED_PROXY_CIDRS.split(",") if value.strip()]
        for network in proxy_networks:
            try:
                ip_network(network, strict=False)
            except ValueError as exc:
                raise ValueError(f"TRUSTED_PROXY_CIDRS contains invalid network {network!r}") from exc
        for name, value in (
            ("LIVE_STRATEGY_MAX_ACTIVATIONS_PER_CYCLE", self.LIVE_STRATEGY_MAX_ACTIVATIONS_PER_CYCLE),
            ("LIVE_STRATEGY_MAX_ASSETS_PER_CYCLE", self.LIVE_STRATEGY_MAX_ASSETS_PER_CYCLE),
            ("LIVE_STRATEGY_MAX_CANDLES_PER_ASSET_CYCLE", self.LIVE_STRATEGY_MAX_CANDLES_PER_ASSET_CYCLE),
            ("LIVE_STRATEGY_DISPATCH_LEASE_SECONDS", self.LIVE_STRATEGY_DISPATCH_LEASE_SECONDS),
            ("STRATEGY_COMPARISON_MAX_RECORDS", self.STRATEGY_COMPARISON_MAX_RECORDS),
            ("STRATEGY_COMPARISON_MAX_WINDOW_SECONDS", self.STRATEGY_COMPARISON_MAX_WINDOW_SECONDS),
            ("STRATEGY_COMPARISON_DISPATCH_LEASE_SECONDS", self.STRATEGY_COMPARISON_DISPATCH_LEASE_SECONDS),
            ("STRATEGY_MONITORING_MAX_BINDINGS_PER_CYCLE", self.STRATEGY_MONITORING_MAX_BINDINGS_PER_CYCLE),
            ("STRATEGY_MONITORING_MAX_COMPARISONS_PER_BINDING", self.STRATEGY_MONITORING_MAX_COMPARISONS_PER_BINDING),
            ("STRATEGY_MONITORING_FINDING_PAGE_SIZE", self.STRATEGY_MONITORING_FINDING_PAGE_SIZE),
            ("STRATEGY_MONITORING_DISPATCH_LEASE_SECONDS", self.STRATEGY_MONITORING_DISPATCH_LEASE_SECONDS),
            ("STRATEGY_MONITORING_BINDING_LEASE_SECONDS", self.STRATEGY_MONITORING_BINDING_LEASE_SECONDS),
            ("GAP_REPAIR_MAX_JOBS_PER_RUN", self.GAP_REPAIR_MAX_JOBS_PER_RUN),
            ("GAP_REPAIR_MAX_ASSETS_PER_SCAN", self.GAP_REPAIR_MAX_ASSETS_PER_SCAN),
            ("GAP_REPAIR_MAX_JOBS_PER_SCAN", self.GAP_REPAIR_MAX_JOBS_PER_SCAN),
            ("GAP_REPAIR_MAX_SYNC_RANGES_PER_DETECTION", self.GAP_REPAIR_MAX_SYNC_RANGES_PER_DETECTION),
            ("GAP_REPAIR_MAX_GAPS_PER_DETECTION", self.GAP_REPAIR_MAX_GAPS_PER_DETECTION),
            ("GAP_REPAIR_MAX_ACTIVE_OVERLAPS", self.GAP_REPAIR_MAX_ACTIVE_OVERLAPS),
            ("CAGG_REFRESH_MAX_JOBS_PER_RUN", self.CAGG_REFRESH_MAX_JOBS_PER_RUN),
            ("HISTORICAL_MERGE_MAX_JOBS_PER_RUN", self.HISTORICAL_MERGE_MAX_JOBS_PER_RUN),
            ("HISTORICAL_MERGE_MAX_SCHEDULE_DAYS", self.HISTORICAL_MERGE_MAX_SCHEDULE_DAYS),
            ("HISTORICAL_MERGE_PAGE_SIZE", self.HISTORICAL_MERGE_PAGE_SIZE),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be a positive integer, got {value}")
        # Development defaults are intentionally convenient for local unit
        # tests, but a production process must refuse to boot with them.  A
        # silent fallback here would turn a missing deployment secret into
        # publicly guessable credentials or API-key hashes.
        if self.ENVIRONMENT.strip().lower() not in {"development", "test", "testing"}:
            insecure_defaults = {
                "POSTGRES_PASSWORD": (self.POSTGRES_PASSWORD, "postgres"),
                "API_KEY_PEPPER": (self.API_KEY_PEPPER, "default_development_pepper"),
                "AWS_ACCESS_KEY_ID": (self.AWS_ACCESS_KEY_ID, "minioadmin"),
                "AWS_SECRET_ACCESS_KEY": (self.AWS_SECRET_ACCESS_KEY, "minioadmin"),
                "REDIS_URL": (self.REDIS_URL, "redis://localhost:6379/0"),
            }
            insecure_names = [
                name for name, (actual, default) in insecure_defaults.items() if actual == default
            ]
            if insecure_names:
                raise ValueError(
                    "non-development environment cannot use development defaults for "
                    + ", ".join(insecure_names)
                )
        return self

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
