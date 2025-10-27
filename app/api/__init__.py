from fastapi import APIRouter

from app.api.health import router as health_router
from app.capacity.api import router as capacity_router

api_router = APIRouter()

api_router.include_router(capacity_router)
api_router.include_router(health_router)
