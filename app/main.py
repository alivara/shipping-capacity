import logging
from typing import Annotated

from fastapi import Depends, FastAPI

from app.api.api import router
from app.config import Environment, Settings, get_settings
from app.logging_config import LOGGING_CONFIG

# Configure logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("APP")

# Get application settings
setting = get_settings()

# TODO add the lifespan manager to handle startup and shutdown events

app = FastAPI(
    title="Shipping Capacity",
    version="0.0.1",
    docs_url="/docs" if setting.BASE.ENV != Environment.PRODUCTION else None,
    redoc_url="/redoc" if setting.BASE.ENV != Environment.PRODUCTION else None,
    redirect_slashes=True,
)


# Include routers
app.include_router(router)


@app.get(
    "/info",
)
async def info(settings: Annotated[Settings, Depends(get_settings)]):
    return {
        "app_name": settings.BASE.APP_NAME,
        "admin_email": settings.BASE.ADMIN_EMAIL,
        "version": settings.BASE.VERSION,
    }
