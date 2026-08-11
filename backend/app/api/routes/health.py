from fastapi import APIRouter, Depends, status, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.api.dependencies import get_db

router = APIRouter(tags=["Health"])

@router.get("/health", status_code=status.HTTP_200_OK)
async def health_check():
    return {"status": "healthy"}

@router.get("/health/db", status_code=status.HTTP_200_OK)
async def db_health_check(db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(text("SELECT 1"))
        return {"status": "healthy", "database": "reachable"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database connection failed: {str(e)}"
        )
