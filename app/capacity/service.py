import logging

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from .query_builder import CapacityQueryBuilder
from .schemas import CapacityFilterParams, CapacityResponse

logger = logging.getLogger("APP")


class CapacityService:
    """
    Service for calculating shipping capacity metrics.

    This service provides methods to calculate various capacity metrics,
    with support for both raw SQL and ORM implementations.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the capacity service.

        Args:
            session: Database session for executing queries
        """
        self.session = session

    def __repr__(self):
        return f"<{type(self).__name__}>"

    async def calculate_capacity(
        self,
        filters: CapacityFilterParams,
    ) -> list[CapacityResponse]:
        """
        Calculate 4-week rolling average of offered capacity.

        Args:
            filters: Date range and route filters

        Returns:
            List of CapacityResponse objects with weekly rolling averages

        Raises:
            SQLAlchemyError: If database query fails
        """
        return await self._calculate_with_orm(filters)

    async def _calculate_with_orm(
        self,
        filters: CapacityFilterParams,
    ) -> list[CapacityResponse]:
        """
        Calculate capacity using SQLAlchemy ORM with Builder pattern.

        Uses fluent interface for clean, readable query construction.
        Much more maintainable than nested subqueries!
        """
        try:
            # build query using query builder
            query = (
                CapacityQueryBuilder(filters)
                .filter_by_route()
                .deduplicate_journeys()
                .aggregate_by_week()
                .apply_rolling_average(window_size=4)
                .filter_by_date_range()
                .build()
            )

            logger.debug(
                f"Executing ORM query (Builder) for {filters.date_from} to {filters.date_to}"
            )

            result = await self.session.execute(query)
            rows = result.fetchall()

            logger.info(f"Successfully retrieved {len(rows)} weeks (ORM Builder)")

            return rows

        except SQLAlchemyError as e:
            logger.error(f"Database error (ORM): {e}", exc_info=True)
            raise
