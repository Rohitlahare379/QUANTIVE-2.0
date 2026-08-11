from fastapi import Request
from fastapi.responses import JSONResponse
from slowapi.errors import RateLimitExceeded

from app.services.exceptions import (
    AssetNotFoundError,
    UnsupportedTimeframeError,
    InvalidDateRangeError
)
from app.connectors.exceptions import PayloadCorruptionError

def register_exception_handlers(app):
    @app.exception_handler(AssetNotFoundError)
    async def asset_not_found_handler(request: Request, exc: AssetNotFoundError):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(UnsupportedTimeframeError)
    async def unsupported_timeframe_handler(request: Request, exc: UnsupportedTimeframeError):
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.exception_handler(InvalidDateRangeError)
    async def invalid_date_range_handler(request: Request, exc: InvalidDateRangeError):
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.exception_handler(PayloadCorruptionError)
    async def payload_corruption_handler(request: Request, exc: PayloadCorruptionError):
        return JSONResponse(status_code=500, content={"detail": str(exc)})

    @app.exception_handler(RateLimitExceeded)
    async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
        return JSONResponse(status_code=429, content={"detail": "Rate limit exceeded"})
