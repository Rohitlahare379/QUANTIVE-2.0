from ipaddress import ip_address, ip_network

from fastapi import Request
from slowapi import Limiter
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.session import AsyncSessionLocal
from app.core.config import settings

def get_trusted_client_ip(request: Request) -> str:
    """
    Extract the client IP without allowing an arbitrary client to spoof its
    rate-limit identity through a forwarding header.  CF-Connecting-IP and
    X-Real-IP are honored only when the direct peer is an explicitly configured
    trusted proxy.
    """
    peer_ip = request.client.host if request.client and request.client.host else None
    if peer_ip and _is_trusted_proxy(peer_ip):
        cf_ip = request.headers.get("CF-Connecting-IP")
        if cf_ip:
            return cf_ip

        x_real_ip = request.headers.get("X-Real-IP")
        if x_real_ip:
            return x_real_ip

    if peer_ip:
        return peer_ip
    return "127.0.0.1"


def _is_trusted_proxy(peer_ip: str) -> bool:
    """Return False for malformed peer addresses or unconfigured proxies."""
    try:
        address = ip_address(peer_ip)
    except ValueError:
        return False
    return any(
        address in ip_network(network.strip(), strict=False)
        for network in settings.TRUSTED_PROXY_CIDRS.split(",")
        if network.strip()
    )

limiter = Limiter(
    key_func=get_trusted_client_ip,
    storage_uri=settings.REDIS_URL,
    storage_options={
        "socket_connect_timeout": settings.REDIS_SOCKET_CONNECT_TIMEOUT_SECONDS,
        "socket_timeout": settings.REDIS_SOCKET_TIMEOUT_SECONDS,
        "max_connections": settings.REDIS_MAX_CONNECTIONS,
    },
)

async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session

from app.api.auth import verify_api_key
get_api_key = verify_api_key
