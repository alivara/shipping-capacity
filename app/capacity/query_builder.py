from sqlalchemy import CTE, Date, Integer, Select, and_, between, cast, func, select

from app.database.model import SailingTable

from .schemas import CapacityFilterParams


class CapacityQueryBuilder:
    """
    Builder for constructing capacity calculation queries.

    Uses the Builder pattern to make complex query construction readable
    and maintainable. Each method returns self for method chaining.

    Example:
        builder = CapacityQueryBuilder(filters)
        query = (builder
                 .filter_by_route()
                 .deduplicate_journeys()
                 .aggregate_by_week()
                 .apply_rolling_average()
                 .filter_by_date_range()
                 .build())
    """

    def __init__(self, filters: CapacityFilterParams):
        """
        Initialize the query builder.

        Args:
            filters: Capacity filter parameters (dates, routes)
        """
        self.filters: CapacityFilterParams = filters
        self._base_query: CTE | None = None
        self._query: CTE | None = None
        self._unique_journeys: CTE | None = None
        self._weekly_capacity: CTE | None = None
        self._rolling_avg: CTE | None = None

    def __repr__(self):
        return f"<{type(self).__name__}(filters={self.filters!r})>"

    def filter_by_route(self) -> "CapacityQueryBuilder":
        """
        Step 1: Filter sailings by origin and destination.

        Returns:
            self for method chaining
        """
        self._base_query = select(
            SailingTable.service_version_and_roundtrip_identfiers,
            SailingTable.origin_service_version_and_master,
            SailingTable.destination_service_version_and_master,
            SailingTable.origin_at_utc,
            SailingTable.offered_capacity_teu,
        ).where(
            and_(
                SailingTable.origin == self.filters.from_origin,
                SailingTable.destination == self.filters.to_destination,
            )
        )
        return self

    def deduplicate_journeys(self) -> "CapacityQueryBuilder":
        """
        Step 2: Keep only the latest departure for each unique journey.

        ## ** point to check **
        # here i have used distinct on to get the unique journeys
        # but also there is possibility to use window function with row number
        # and qualify too, it depends on the database support and clustering.

        Returns:
            self for method chaining
        """
        if self._base_query is None:
            raise ValueError("Must call filter_by_route() first")

        self._unique_journeys = (
            self._base_query.distinct(
                SailingTable.service_version_and_roundtrip_identfiers,
                SailingTable.origin_service_version_and_master,
                SailingTable.destination_service_version_and_master,
            ).order_by(
                SailingTable.service_version_and_roundtrip_identfiers,
                SailingTable.origin_service_version_and_master,
                SailingTable.destination_service_version_and_master,
                SailingTable.origin_at_utc.desc(),
            )
        ).subquery("unique_journeys")

        return self

    def aggregate_by_week(self) -> "CapacityQueryBuilder":
        """
        Step 3: Aggregate capacity by calendar week.

        Groups unique journeys by week and sums their capacity.

        Returns:
            self for method chaining
        """
        if self._unique_journeys is None:
            raise ValueError("Must call deduplicate_journeys() first")

        week_trunc = func.date_trunc("week", self._unique_journeys.c.origin_at_utc).label(
            "week_start_date_ts"
        )

        week_no = func.extract("week", self._unique_journeys.c.origin_at_utc).label("week_no")

        self._weekly_capacity = (
            select(
                week_trunc,
                week_no,
                func.sum(self._unique_journeys.c.offered_capacity_teu).label("weekly_capacity_teu"),
            ).group_by(week_trunc, week_no)
        ).subquery("weekly_capacity")

        return self

    def apply_rolling_average(self, window_size: int = 4) -> "CapacityQueryBuilder":
        """
        Step 4: Calculate rolling average over specified window.

        Args:
            window_size: Number of weeks to include in average (default: 4)

        Returns:
            self for method chaining
        """
        if self._weekly_capacity is None:
            raise ValueError("Must call aggregate_by_week() first")

        preceding_rows = window_size - 1  # 4-week window = current + 3 preceding

        self._rolling_avg = (
            select(
                cast(self._weekly_capacity.c.week_start_date_ts, Date).label("week_start_date"),
                cast(self._weekly_capacity.c.week_no, Integer).label("week_no"),
                cast(
                    func.round(
                        func.avg(self._weekly_capacity.c.weekly_capacity_teu).over(
                            order_by=self._weekly_capacity.c.week_start_date_ts,
                            rows=(-preceding_rows, 0),  # PRECEDING to CURRENT ROW
                        )
                    ),
                    Integer,
                ).label("offered_capacity_teu"),
            )
        ).subquery("rolling_avg")

        return self

    def filter_by_date_range(self) -> "CapacityQueryBuilder":
        """
        Step 5: Filter results to requested date range.

        Returns:
            self for method chaining
        """
        if self._rolling_avg is None:
            raise ValueError("Must call apply_rolling_average() first")

        self._query = (
            select(
                self._rolling_avg.c.week_start_date,
                self._rolling_avg.c.week_no,
                self._rolling_avg.c.offered_capacity_teu,
            )
            .where(
                between(
                    self._rolling_avg.c.week_start_date,
                    self.filters.date_from,
                    self.filters.date_to,
                )
            )
            .order_by(self._rolling_avg.c.week_start_date)
        )

        return self

    def build(self) -> Select:
        """
        Build and return the final query.

        Returns:
            Constructed SQLAlchemy select query

        Raises:
            ValueError: If build() called before completing all steps
        """
        if self._query is None:
            raise ValueError(
                "Must complete all steps before build(): "
                "filter_by_route() → deduplicate_journeys() → "
                "aggregate_by_week() → apply_rolling_average() → filter_by_date_range()"
            )

        return self._query
