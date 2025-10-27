import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.session import get_db_session

from .schemas import CapacityFilterParams, CapacityResponse
from .util import get_shipping_capacity_by_data

router = APIRouter()
logger = logging.getLogger("APP")


@router.get("/capacity", response_model=List[CapacityResponse])
async def get_capacity(
    filter_query: CapacityFilterParams = Depends(),
    session: AsyncSession = Depends(get_db_session),
) -> List[dict]:
    """
    Get 4-week rolling average of offered shipping capacity (TEU) per week
    between the specified date range.
    """
    if filter_query.date_from > filter_query.date_to:
        raise HTTPException(status_code=400, detail="date_from cannot be after date_to")

    try:
        result = await get_shipping_capacity_by_data(
            session, filter_query.date_from, filter_query.date_to
        )
    except Exception as e:
        logger.error(f"Error fetching shipping capacity: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    return result
