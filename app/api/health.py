from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    # Simply return a healthy status
    return {"status": "healthy"}
