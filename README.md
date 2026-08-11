# QUANTIVE 2.0 ⚡

**High-Throughput Time-Series Market Data & Continuous Aggregates Engine**

QUANTIVE 2.0 is a robust, production-grade backend infrastructure designed for ingesting, staging, aggregating, and querying high-frequency crypto market data (OHLCV candles). Powered by **TimescaleDB Continuous Aggregates (CAGGs)**, **FastAPI**, **Dramatiq (Redis)**, and **Prometheus/Grafana** observability, it provides resilient real-time ingestion, deterministic multi-timeframe rollup processing, gap staging, and distributed lease-managed aggregate refreshes.

---

## 🏛 Architecture Overview

```
                        ┌──────────────────────────────┐
                        │      Binance REST API        │
                        └──────────────┬───────────────┘
                                       │ (Rate-Limited Connector)
                                       ▼
                        ┌──────────────────────────────┐
                        │  Dramatiq Ingestion Workers  │
                        └──────────────┬───────────────┘
                                       │
                ┌──────────────────────┴──────────────────────┐
                ▼                                             ▼
┌──────────────────────────────┐              ┌──────────────────────────────┐
│       TimescaleDB            │              │      Redis / Dramatiq        │
│  - raw_1m_candles (Hypertable)              │  - Ingestion Tasks           │
│  - Gap Staging Tables        │              │  - Distributed Locks/Leases  │
│  - Continuous Aggregates     │              │  - Export Pipeline           │
│    (5m, 15m, 1h, 4h, 1d)     │              └──────────────────────────────┘
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐              ┌──────────────────────────────┐
│     FastAPI Core API         │ ───────────► │    Prometheus & Grafana      │
│  - Authenticated Endpoints   │              │  - Metrics & DLQ Alerts      │
│  - Parquet/CSV Export Engine │              │  - Heartbeat & Liveness      │
└──────────────────────────────┘              └──────────────────────────────┘
```

---

## ✨ Key Features

- **Continuous Aggregate (CAGG) Architecture**: Real-time rollups across multiple timeframe horizons (5m, 15m, 1h, 4h, 1d) with zero-gap backfilling and window tracking.
- **Resilient Distributed Ingestion**: Multi-tiered Binance market connector with exponential backoff, rate-limit governance, and Dead Letter Queue (DLQ) recovery runbooks.
- **Heartbeat & Lease Management**: Concurrency-safe CAGG refresh workers with robust lease renewal, heartbeat liveness verification, and distributed worker failover.
- **Bulk Export Pipeline**: High-performance Parquet and CSV export engine with AWS S3 integration.
- **Enterprise Observability**: Native Prometheus metrics, custom error tracking, latency timing middlewares, and pre-configured Grafana dashboards.
- **Modern Async Core**: Built on Python 3.11+, SQLAlchemy 2.0 (AsyncPG), Alembic migrations, and Pydantic v2.

---

## 📁 Repository Structure

```
QUANTIVE 2.0/
├── backend/
│   ├── alembic/              # Database schema migrations (CAGGs, hypertables, leases)
│   ├── app/
│   │   ├── api/              # FastAPI routers, middleware, auth & dependencies
│   │   ├── connectors/       # Exchange connectors & rate limiters (Binance)
│   │   ├── core/             # Configuration & environment management
│   │   ├── db/               # Async database sessions & engine
│   │   ├── models/           # SQLAlchemy ORM definitions & TimescaleDB hypertables
│   │   ├── repositories/     # Data access layer for candles & assets
│   │   ├── services/         # Core business logic (Ingestion, CAGG Refresh, Exports)
│   │   └── workers/          # Dramatiq background workers & scheduled tasks
│   ├── grafana/              # Dashboards & alert configurations
│   ├── prometheus/           # Alert rules and scraping configuration
│   ├── runbooks/             # Operational runbooks (e.g., DLQ recovery)
│   ├── tests/                # Comprehensive async test suites (API, Workers, Services)
│   ├── Dockerfile            # Container definition
│   ├── docker-compose.yml    # Full stack orchestration (App, TimescaleDB, Redis, Grafana)
│   └── requirements.txt      # Python dependencies
└── README.md
```

---

## 🚀 Quick Start

### 1. Prerequisites
- Docker & Docker Compose
- Python 3.11+
- Node.js 18+ (for frontend consumers)

### 2. Environment Setup
Create a `.env` file in the `backend/` directory:
```env
PROJECT_NAME="QUANTIVE 2.0"
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/quantive
REDIS_URL=redis://localhost:6379/0
API_KEY=your_secure_api_key
```

### 3. Launch Services with Docker Compose
```bash
cd backend
docker-compose up -d
```

### 4. Run Migrations & Tests
```bash
# Run database migrations
cd backend
alembic upgrade head

# Run full test suite
pytest -v
```

---

## 🧪 Testing & Validation

The codebase includes comprehensive unit and integration tests covering:
- CAGG Refresh worker concurrency and lease heartbeat liveness.
- Ingestion and historical candle gap backfilling.
- Exchange rate limiting and mock network responses.
- API authentication, rate limiting, and export lifecycles.

```bash
pytest backend/tests/ -v
```

---

## 📄 License
MIT License. Built for high-frequency algorithmic finance.
