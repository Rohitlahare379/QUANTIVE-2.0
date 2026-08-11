import time
import logging
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)

class TimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start_time = time.perf_counter()
        response = await call_next(request)
        process_time = time.perf_counter() - start_time
        
        logger.info(
            f"method={request.method} path={request.url.path} "
            f"status={response.status_code} latency_ms={process_time * 1000:.2f}"
        )
        return response
