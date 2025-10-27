from datetime import date

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .schemas import CapacityResponse


async def get_shipping_capacity_by_data(
    session: AsyncSession, start_date: date, end_date: date
) -> CapacityResponse:
    """
    Return 4-week rolling average of offered capacity (TEU) per week.

    First we identify the latest sailing from China to North Europe for each unique
    service/vessel combination. We then aggregate these sailings by week, summing their
    offered capacity. Finally, we compute a 4-week rolling average of the weekly capacities.
    """

    sql = text(
        """
        WITH unique_journeys AS (
            SELECT
                service_version_and_roundtrip_identfiers,
                origin_service_version_and_master,
                destination_service_version_and_master,
                MAX(origin_at_utc) AS latest_china_departure,
                MAX(offered_capacity_teu) AS capacity_teu
            FROM sailings
            WHERE
                origin = 'china_main'
                AND destination = 'north_europe_main'
            GROUP BY 1, 2, 3
        ),
        weekly_capacity AS (
            SELECT
                DATE_TRUNC('week', latest_china_departure)::date AS week_start_date,
                EXTRACT(WEEK FROM latest_china_departure)::int AS week_no,
                SUM(capacity_teu) AS weekly_capacity_teu
            FROM unique_journeys
            GROUP BY 1, 2
        ),
        rolling_avg AS (
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
        WHERE week_start_date BETWEEN :start_date AND :end_date
        ORDER BY week_start_date;
        """
    )

    result = await session.execute(sql, {"start_date": start_date, "end_date": end_date})
    return result.fetchall()
