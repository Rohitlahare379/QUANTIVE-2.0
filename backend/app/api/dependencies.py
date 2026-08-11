from fastapi import Request
from slowapi import Limiter
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal
from app.core.config import settings

def get_trusted_client_ip(request: Request) -> str:
    """
    Extracts the real client IP accurately when deployed behind trusted reverse proxies
    (AWS ALB, Cloudflare, NGINX).
    """
    cf_ip = request.headers.get("CF-Connecting-IP")
    if cf_ip:
        return cf_ip
        
    x_real_ip = request.headers.get("X-Real-IP")
    if x_real_ip:
        return x_real_ip
        
    if request.client and request.client.host:
        return request.client.host
        
    return "127.0.0.1"

limiter = Limiter(
    key_func=get_trusted_client_ip,
    storage_uri=settings.REDIS_URL,
    storage_options={"socket_connect_timeout": 2}
)

async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session

from app.api.auth import verify_api_key
get_api_key = verify_api_key
