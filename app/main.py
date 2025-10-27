import logging
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI

from app.api import api_router
from app.config import Environment, Settings, get_settings
from app.database.utils import clear_table, load_csv_to_database
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
    Create the tables and load data from CSV file on startup,
    """
    try:
        await load_csv_to_database(
            csv_file_path=setting.DATABASE.CSV_FILE_PATH,
        )
    except Exception as e:
        logger.error(f"Error loading CSV data: {e}")
        raise e
    logger.info("Hello from lifespan!")
    yield
    await clear_table()
    logger.info("Shutting down application...")


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
