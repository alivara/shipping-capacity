import logging
import logging.config
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI

from app.api import api_router
from app.config import Environment, Settings, get_settings
from app.logging_config import LOGGING_CONFIG

# Configure logging
logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("APP")

# Get application settings
setting = get_settings()


# Application lifespan
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager.

    Data loading is handled externally via ETL scripts.
    This keeps application startup fast and decouples data management from app lifecycle.
    """
    logger.info("Application starting...")
    logger.info(f"Environment: {setting.BASE.ENV}")

    yield

    logger.info("Application shutting down...")


app = FastAPI(
    title="Shipping Capacity",
    version="0.0.1",
    docs_url="/docs" if setting.BASE.ENV != Environment.PRODUCTION else None,
    redoc_url="/redoc" if setting.BASE.ENV != Environment.PRODUCTION else None,
    redirect_slashes=True,
    lifespan=lifespan,
)


# Include routers
app.include_router(api_router)


@app.get(
    "/info",
)
async def info(settings: Annotated[Settings, Depends(get_settings)]):
    return {
        "app_name": settings.BASE.APP_NAME,
        "admin_email": settings.BASE.ADMIN_EMAIL,
        "version": settings.BASE.VERSION,
    }
