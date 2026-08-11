from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.core.config import settings
from app.api.dependencies import limiter
from app.api.errors import register_exception_handlers
from app.api.middleware import TimingMiddleware
from app.api.auth import verify_api_key
from fastapi import Depends

from app.api.routes.health import router as health_router
from app.api.routes.metrics import router as metrics_router
from app.api.routes.assets import router as assets_router
from app.api.routes.candles import router as candles_router
from app.api.routes.sync_status import router as sync_status_router
from app.api.routes.exports import router as exports_router

app = FastAPI(
    title=settings.PROJECT_NAME,
    description="Quantive Market Data API",
    version="1.0.0"
)

# Apply Rate Limiter
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Apply Middleware
app.add_middleware(TimingMiddleware)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Register Custom Exception Handlers
register_exception_handlers(app)

# Include Routers
app.include_router(health_router)
app.include_router(metrics_router)
app.include_router(assets_router, dependencies=[Depends(verify_api_key)])
app.include_router(candles_router, dependencies=[Depends(verify_api_key)])
app.include_router(sync_status_router, dependencies=[Depends(verify_api_key)])
app.include_router(exports_router, dependencies=[Depends(verify_api_key)])
