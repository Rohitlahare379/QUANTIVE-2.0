import hashlib
import hmac
from fastapi import Security, HTTPException, Depends
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.api.dependencies import get_db
from app.models.api_keys import ApiKey
from app.core.config import settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

async def verify_api_key(
    api_key: str = Security(api_key_header),
    db: AsyncSession = Depends(get_db)
):
    if not api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header")
        
    legacy_hash = hashlib.sha256(api_key.encode()).hexdigest()
    peppered_hash = hashlib.sha256((settings.API_KEY_PEPPER + api_key).encode()).hexdigest()
    
    stmt = select(ApiKey).where(ApiKey.key_hash.in_([legacy_hash, peppered_hash]))
    result = await db.execute(stmt)
    key_record = result.scalars().first()
    
    if not key_record or not key_record.is_active:
        raise HTTPException(status_code=401, detail="Invalid or inactive API Key")
        
    is_legacy_match = hmac.compare_digest(key_record.key_hash, legacy_hash)
    is_peppered_match = hmac.compare_digest(key_record.key_hash, peppered_hash)
    
    if not is_legacy_match and not is_peppered_match:
        raise HTTPException(status_code=401, detail="Invalid or inactive API Key")
        
    return key_record
