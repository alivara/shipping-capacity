import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.session import get_db_session

from .schemas import CAPACITY_ENDPOINT_RESPONSES, CapacityFilterParams, CapacityResponse
from .service import CapacityService

router = APIRouter(tags=["Capacity"])
logger = logging.getLogger("APP")


@router.get(
    "/capacity",
    response_model=list[CapacityResponse],
    summary="Get shipping capacity with 4-week rolling average",
    description="""
    Calculate and return the 4-week rolling average of offered shipping capacity (TEU)
    for the China Main to North Europe Main trade lane.

    The calculation process:
    1. Identifies unique vessel/service combinations using three identifiers
    2. Determines the latest departure from China for each unique journey
    3. Aggregates capacity by calendar week
    4. Computes 4-week rolling average for smoothing

    **Date Format**: YYYY-MM-DD (e.g., 2024-01-15)

    **Week Convention**: Weeks start on Monday (ISO 8601 standard)
    """,
    responses=CAPACITY_ENDPOINT_RESPONSES,
)
async def get_capacity(
    filter_query: Annotated[CapacityFilterParams, Query()],
    session: AsyncSession = Depends(get_db_session),
) -> list[dict]:
    """
    Get 4-week rolling average of offered shipping capacity (TEU) per week.

    This endpoint returns weekly capacity data with a 4-week rolling average applied,
    providing a smoothed view of shipping capacity trends over time.

    Args:
        filter_query: Query parameters containing date_from, date_to, from_origin, and to_destination
        session: Database session (injected by FastAPI)

    Returns:
        List of capacity records, each containing:
            - week_start_date: First day of the week (Monday)
            - week_no: ISO week number (1-53)
            - offered_capacity_teu: 4-week rolling average capacity in TEU

    Raises:
        HTTPException: 422 if validation fails (handled by Pydantic)
        HTTPException: 500 if database query fails

    Example:
        GET /capacity?date_from=2024-01-01&date_to=2024-03-31
        GET /capacity?date_from=2024-01-01&date_to=2024-03-31&from_origin=china_main&to_destination=north_europe_main
    """
    try:
        logger.info(
            f"Fetching capacity data: {filter_query.date_from} to {filter_query.date_to} "
            f"({filter_query.from_origin} -> {filter_query.to_destination})"
        )

        # calculate capacity
        capacity_service = CapacityService(session)
        result = await capacity_service.calculate_capacity(filter_query)
        logger.info(f"Successfully returned {len(result)} weeks of capacity data")
        return result
    except SQLAlchemyError as e:
        logger.error(f"Database error while fetching shipping capacity: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail="Database error occurred while fetching capacity data"
        )
    except Exception as e:
        logger.error(f"Unexpected error while fetching shipping capacity: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred while processing your request {e}",
        )
