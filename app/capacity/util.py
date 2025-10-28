"""
Utility functions for calculating shipping capacity metrics.

This module provides functions to calculate the 4-week rolling average
of offered shipping capacity (TEU) for trade lanes.
"""

import logging
from typing import List

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from .schemas import CapacityFilterParams, CapacityResponse

logger = logging.getLogger("APP")


async def get_shipping_capacity_by_data(
    session: AsyncSession,
    filters: CapacityFilterParams,
) -> List[CapacityResponse]:
    """
    Calculate 4-week rolling average of offered capacity (TEU) per week.

    This function calculates the shipping capacity for trade lanes using a three-step process:

    1. **Identify Unique Journeys**: Group sailings by service/vessel combination using
       three unique identifiers. For each unique journey, find the latest departure from
       the origin region to determine the week assignment.

    2. **Aggregate by Week**: Sum the capacity of all unique journeys that depart in each
       calendar week. This gives us the total weekly offered capacity.

    3. **Calculate Rolling Average**: For each week, compute the average capacity over a
       4-week window (current week plus 3 preceding weeks). This smooths out weekly
       fluctuations and provides a more stable capacity metric.

    Args:
        session: Active database session for executing queries
        filters: CapacityFilterParams containing date range and origin/destination filters.
                 Includes date_from, date_to, from_origin, and to_destination.

    Returns:
        List of CapacityResponse objects containing week_start_date, week_no,
        and offered_capacity_teu for each week in the specified range.
        Returns empty list if no data exists for the specified period.

    Raises:
        SQLAlchemyError: If database query fails

    Example:
        >>> from app.capacity.schemas import CapacityFilterParams
        >>> from datetime import date
        >>> session = get_db_session()
        >>> filters = CapacityFilterParams(
        ...     date_from=date(2024, 1, 1),
        ...     date_to=date(2024, 3, 31),
        ...     from_origin="china_main",
        ...     to_destination="north_europe_main"
        ... )
        >>> result = await get_shipping_capacity_by_data(session, filters)
        >>> for week in result:
        ...     print(f"Week {week.week_no}: {week.offered_capacity_teu} TEU")

    Notes:
        - Week start dates follow PostgreSQL's DATE_TRUNC('week') convention (Monday start)
        - Week numbers follow ISO 8601 standard (1-53)
        - Rolling average uses up to 4 weeks of history (current + 3 preceding)
        - For early weeks with less than 4 weeks of history, average uses available data
        - Origin/destination filters default to 'china_main' and 'north_europe_main'
    """
    try:
        sql = text(
            """
            WITH unique_journeys AS (
                -- Step 1: Get unique service/vessel combinations
                -- Note: DISTINCT ON is kept here as a defensive measure to handle any edge cases
                SELECT DISTINCT ON (
                    service_version_and_roundtrip_identfiers,
                    origin_service_version_and_master,
                    destination_service_version_and_master
                )
                    service_version_and_roundtrip_identfiers,
                    origin_service_version_and_master,
                    destination_service_version_and_master,
                    origin_at_utc AS latest_departure,
                    offered_capacity_teu AS capacity_teu
                FROM sailings
                WHERE
                    origin = :from_origin
                    AND destination = :to_destination
                ORDER BY
                    service_version_and_roundtrip_identfiers,
                    origin_service_version_and_master,
                    destination_service_version_and_master,
                    origin_at_utc DESC
            ),
            weekly_capacity AS (
                -- Step 2: Aggregate capacity by calendar week
                -- Week starts on Monday (PostgreSQL default for DATE_TRUNC('week'))
                SELECT
                    DATE_TRUNC('week', latest_departure)::date AS week_start_date,
                    EXTRACT(WEEK FROM latest_departure)::int AS week_no,
                    SUM(capacity_teu) AS weekly_capacity_teu
                FROM unique_journeys
                GROUP BY 1, 2
            ),
            rolling_avg AS (
                -- Step 3: Calculate 4-week rolling average
                -- Window includes current row and 3 preceding rows (4 weeks total)
                SELECT
                    week_start_date,
                    week_no,
                    ROUND(AVG(weekly_capacity_teu) OVER (
                        ORDER BY week_start_date
                        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                    ))::int AS offered_capacity_teu
                FROM weekly_capacity
            )
            SELECT
                week_start_date,
                week_no,
                offered_capacity_teu
            FROM rolling_avg
            WHERE week_start_date BETWEEN :from_date AND :to_date
            ORDER BY week_start_date;
            """
        )

        logger.debug(
            f"Executing capacity query for date range: {filters.date_from} to {filters.date_to} "
            f"on route {filters.from_origin} -> {filters.to_destination}"
        )
        result = await session.execute(
            sql,
            {
                "from_origin": filters.from_origin,
                "to_destination": filters.to_destination,
                "from_date": filters.date_from,
                "to_date": filters.date_to,
            },
        )
        rows = result.fetchall()

        logger.info(
            f"Successfully retrieved {len(rows)} weeks of capacity data "
            f"for period {filters.date_from} to {filters.date_to} "
            f"({filters.from_origin} -> {filters.to_destination})"
        )

        return rows

    except SQLAlchemyError as e:
        logger.error(f"Database error while fetching capacity data: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Unexpected error while fetching capacity data: {e}", exc_info=True)
        raise
