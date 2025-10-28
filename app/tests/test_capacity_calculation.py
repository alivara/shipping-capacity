from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.capacity.schemas import CapacityFilterParams
from app.capacity.util import get_shipping_capacity_by_data
from app.database.model import SailingTable


def utc_datetime(*args) -> datetime:
    """Helper to create UTC-aware datetime for tests."""
    return datetime(*args, tzinfo=timezone.utc)


@pytest.mark.integration
class TestCapacityCalculationLogic:
    """Tests for the core capacity calculation SQL logic"""

    @pytest.mark.asyncio
    async def test_unique_journey_identification(self, test_db_session: AsyncSession):
        """
        Test that unique journeys are correctly identified by the three identifiers.

        Multiple sailings with the same service/vessel combination should be
        grouped together, and only the latest departure from China is used.
        """
        # Create multiple sailings for the same service/vessel at different times
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)

        sailings = [
            SailingTable(
                origin="china_main",
                destination="north_europe_main",
                origin_port_code="CNSHA",
                destination_port_code="NLRTM",
                service_version_and_roundtrip_identfiers="SERVICE_1",
                origin_service_version_and_master="MASTER_1_ORIGIN",
                destination_service_version_and_master="MASTER_1_DEST",
                origin_at_utc=base_date,
                offered_capacity_teu=15000,
            ),
            SailingTable(
                origin="china_main",
                destination="north_europe_main",
                origin_port_code="CNSHA",
                destination_port_code="DEHAM",  # Different port
                service_version_and_roundtrip_identfiers="SERVICE_1",
                origin_service_version_and_master="MASTER_1_ORIGIN",
                destination_service_version_and_master="MASTER_1_DEST",
                origin_at_utc=base_date + timedelta(days=2),  # Later departure
                offered_capacity_teu=16000,
            ),
        ]

        test_db_session.add_all(sailings)
        await test_db_session.commit()

        # Query to get unique journeys (same as in util.py)
        sql = text(
            """
            SELECT COUNT(*) as count
            FROM (
                SELECT
                    service_version_and_roundtrip_identfiers,
                    origin_service_version_and_master,
                    destination_service_version_and_master,
                    MAX(origin_at_utc) AS latest_china_departure
                FROM sailings
                WHERE origin = 'china_main' AND destination = 'north_europe_main'
                GROUP BY 1, 2, 3
            ) AS unique_journeys
        """
        )

        result = await test_db_session.execute(sql)
        count = result.scalar()

        # Should have only 1 unique journey despite 2 sailings
        assert count == 1

    @pytest.mark.asyncio
    async def test_weekly_aggregation(self, test_db_session: AsyncSession):
        """
        Test that sailings are correctly aggregated by week.

        Multiple unique journeys in the same week should have their
        capacities summed.
        """
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)

        # Create 3 different services in the same week
        sailings = [
            SailingTable(
                origin="china_main",
                destination="north_europe_main",
                origin_port_code="CNSHA",
                destination_port_code="NLRTM",
                service_version_and_roundtrip_identfiers=f"SERVICE_{i}",
                origin_service_version_and_master=f"MASTER_{i}_ORIGIN",
                destination_service_version_and_master=f"MASTER_{i}_DEST",
                origin_at_utc=base_date + timedelta(days=i),
                offered_capacity_teu=10000,
            )
            for i in range(3)
        ]

        test_db_session.add_all(sailings)
        await test_db_session.commit()

        # Get capacity data
        filters = CapacityFilterParams(date_from=date(2024, 1, 1), date_to=date(2024, 1, 7))
        result = await get_shipping_capacity_by_data(test_db_session, filters)

        # Should have one week with combined capacity
        assert len(result) == 1
        # Capacity should be sum of all three vessels (before rolling average)
        # Note: This tests the weekly aggregation, the rolling avg will be same
        # since there's only one week
        assert result[0].offered_capacity_teu == 30000

    @pytest.mark.asyncio
    async def test_rolling_average_calculation(self, test_db_session: AsyncSession):
        """
        Test that 4-week rolling average is calculated correctly.

        For each week, the average should include the current week plus
        the 3 preceding weeks (4 weeks total).
        """
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)

        # Create data for 5 weeks with known capacities
        weekly_capacities = [10000, 20000, 30000, 40000, 50000]

        for week_idx, capacity in enumerate(weekly_capacities):
            sailing = SailingTable(
                origin="china_main",
                destination="north_europe_main",
                origin_port_code="CNSHA",
                destination_port_code="NLRTM",
                service_version_and_roundtrip_identfiers=f"SERVICE_{week_idx}",
                origin_service_version_and_master=f"MASTER_{week_idx}_ORIGIN",
                destination_service_version_and_master=f"MASTER_{week_idx}_DEST",
                origin_at_utc=base_date + timedelta(weeks=week_idx),
                offered_capacity_teu=capacity,
            )
            test_db_session.add(sailing)

        await test_db_session.commit()

        # Get capacity for week 5 (should average weeks 2, 3, 4, 5)
        filters = CapacityFilterParams(
            date_from=date(2024, 1, 29), date_to=date(2024, 2, 4)  # Week 5
        )
        result = await get_shipping_capacity_by_data(test_db_session, filters)

        assert len(result) == 1
        # Rolling average of weeks 2-5: (20000 + 30000 + 40000 + 50000) / 4 = 35000
        assert result[0].offered_capacity_teu == 35000

        # Test week 1 (only 1 week of history)
        filters_week1 = CapacityFilterParams(date_from=date(2024, 1, 1), date_to=date(2024, 1, 7))
        result_week1 = await get_shipping_capacity_by_data(test_db_session, filters_week1)
        assert len(result_week1) == 1
        assert result_week1[0].offered_capacity_teu == 10000  # Only week 1

        # Test week 3 (should average weeks 1, 2, 3)
        filters_week3 = CapacityFilterParams(date_from=date(2024, 1, 15), date_to=date(2024, 1, 21))
        result_week3 = await get_shipping_capacity_by_data(test_db_session, filters_week3)
        assert len(result_week3) == 1
        # Average of weeks 1, 2, 3: (10000 + 20000 + 30000) / 3 = 20000
        assert result_week3[0].offered_capacity_teu == 20000

    @pytest.mark.asyncio
    async def test_filters_out_wrong_routes(self, test_db_session: AsyncSession):
        """
        Test that only China Main to North Europe Main route is included.

        Sailings on other routes should be filtered out.
        """
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)

        sailings = [
            # Correct route
            SailingTable(
                origin="china_main",
                destination="north_europe_main",
                origin_port_code="CNSHA",
                destination_port_code="NLRTM",
                service_version_and_roundtrip_identfiers="SERVICE_CORRECT",
                origin_service_version_and_master="MASTER_CORRECT_ORIGIN",
                destination_service_version_and_master="MASTER_CORRECT_DEST",
                origin_at_utc=base_date,
                offered_capacity_teu=15000,
            ),
            # Wrong destination
            SailingTable(
                origin="china_main",
                destination="us_west_coast",
                origin_port_code="CNSHA",
                destination_port_code="USLAX",
                service_version_and_roundtrip_identfiers="SERVICE_WRONG_DEST",
                origin_service_version_and_master="MASTER_WRONG_DEST_ORIGIN",
                destination_service_version_and_master="MASTER_WRONG_DEST_DEST",
                origin_at_utc=base_date,
                offered_capacity_teu=20000,
            ),
            # Wrong origin
            SailingTable(
                origin="southeast_asia",
                destination="north_europe_main",
                origin_port_code="SGSIN",
                destination_port_code="NLRTM",
                service_version_and_roundtrip_identfiers="SERVICE_WRONG_ORIGIN",
                origin_service_version_and_master="MASTER_WRONG_ORIGIN_ORIGIN",
                destination_service_version_and_master="MASTER_WRONG_ORIGIN_DEST",
                origin_at_utc=base_date,
                offered_capacity_teu=18000,
            ),
        ]

        test_db_session.add_all(sailings)
        await test_db_session.commit()

        filters = CapacityFilterParams(date_from=date(2024, 1, 1), date_to=date(2024, 1, 7))
        result = await get_shipping_capacity_by_data(test_db_session, filters)

        # Should only have data from the correct route
        assert len(result) == 1
        assert result[0].offered_capacity_teu == 15000

    @pytest.mark.asyncio
    async def test_uses_latest_departure_from_china(self, test_db_session: AsyncSession):
        """
        Test that the latest departure from China is used for week assignment.

        A vessel may have multiple sailings in China before heading to Europe.
        Only the latest one should determine the week.
        """
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)

        # Same service/vessel but different times and capacities
        sailings = [
            SailingTable(
                origin="china_main",
                destination="north_europe_main",
                origin_port_code="CNSHA",  # First port
                destination_port_code="NLRTM",
                service_version_and_roundtrip_identfiers="SERVICE_1",
                origin_service_version_and_master="MASTER_1_ORIGIN",
                destination_service_version_and_master="MASTER_1_DEST",
                origin_at_utc=base_date,
                offered_capacity_teu=15000,
            ),
            SailingTable(
                origin="china_main",
                destination="north_europe_main",
                origin_port_code="CNYTN",  # Second port (later)
                destination_port_code="NLRTM",
                service_version_and_roundtrip_identfiers="SERVICE_1",
                origin_service_version_and_master="MASTER_1_ORIGIN",
                destination_service_version_and_master="MASTER_1_DEST",
                origin_at_utc=base_date + timedelta(days=10),  # Next week
                offered_capacity_teu=16000,
            ),
        ]

        test_db_session.add_all(sailings)
        await test_db_session.commit()

        # Check which week the sailing is assigned to
        filters = CapacityFilterParams(
            date_from=date(2024, 1, 8), date_to=date(2024, 1, 14)  # Week 2
        )
        result = await get_shipping_capacity_by_data(test_db_session, filters)

        # Should be assigned to week 2 based on the latest departure
        assert len(result) == 1
        # Should use the MAX capacity from the latest sailing
        assert result[0].offered_capacity_teu == 16000

    @pytest.mark.asyncio
    async def test_empty_result_for_no_data(self, test_db_session: AsyncSession):
        """
        Test that query returns empty result when no data exists for the range.
        """
        filters = CapacityFilterParams(date_from=date(2025, 1, 1), date_to=date(2025, 1, 31))
        result = await get_shipping_capacity_by_data(test_db_session, filters)

        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_week_number_extraction(self, test_db_session: AsyncSession):
        """
        Test that week numbers are correctly extracted from dates.

        Week numbers should follow ISO 8601 standard.
        """
        # Create sailings in different weeks
        sailings = [
            SailingTable(
                origin="china_main",
                destination="north_europe_main",
                origin_port_code="CNSHA",
                destination_port_code="NLRTM",
                service_version_and_roundtrip_identfiers=f"SERVICE_{i}",
                origin_service_version_and_master=f"MASTER_{i}_ORIGIN",
                destination_service_version_and_master=f"MASTER_{i}_DEST",
                origin_at_utc=utc_datetime(2024, 1, i * 7 + 1, 0, 0, 0),
                offered_capacity_teu=10000,
            )
            for i in range(4)
        ]

        test_db_session.add_all(sailings)
        await test_db_session.commit()

        filters = CapacityFilterParams(date_from=date(2024, 1, 1), date_to=date(2024, 1, 31))
        result = await get_shipping_capacity_by_data(test_db_session, filters)

        # Verify week numbers are sequential and reasonable
        week_numbers = [row.week_no for row in result]
        assert all(1 <= wn <= 53 for wn in week_numbers)
        # Week numbers should be monotonically increasing
        assert week_numbers == sorted(week_numbers)


@pytest.mark.integration
class TestEdgeCases:
    """Tests for edge cases and boundary conditions"""

    @pytest.mark.asyncio
    async def test_very_large_capacity_values(self, test_db_session: AsyncSession):
        """Test handling of very large capacity values"""
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)

        sailing = SailingTable(
            origin="china_main",
            destination="north_europe_main",
            origin_port_code="CNSHA",
            destination_port_code="NLRTM",
            service_version_and_roundtrip_identfiers="SERVICE_LARGE",
            origin_service_version_and_master="MASTER_LARGE_ORIGIN",
            destination_service_version_and_master="MASTER_LARGE_DEST",
            origin_at_utc=base_date,
            offered_capacity_teu=999999999,  # Very large value
        )

        test_db_session.add(sailing)
        await test_db_session.commit()

        filters = CapacityFilterParams(date_from=date(2024, 1, 1), date_to=date(2024, 1, 7))
        result = await get_shipping_capacity_by_data(test_db_session, filters)

        assert len(result) == 1
        assert result[0].offered_capacity_teu == 999999999

    @pytest.mark.asyncio
    async def test_zero_capacity_values(self, test_db_session: AsyncSession):
        """Test handling of zero capacity values"""
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)

        sailing = SailingTable(
            origin="china_main",
            destination="north_europe_main",
            origin_port_code="CNSHA",
            destination_port_code="NLRTM",
            service_version_and_roundtrip_identfiers="SERVICE_ZERO",
            origin_service_version_and_master="MASTER_ZERO_ORIGIN",
            destination_service_version_and_master="MASTER_ZERO_DEST",
            origin_at_utc=base_date,
            offered_capacity_teu=0,
        )

        test_db_session.add(sailing)
        await test_db_session.commit()

        filters = CapacityFilterParams(date_from=date(2024, 1, 1), date_to=date(2024, 1, 7))
        result = await get_shipping_capacity_by_data(test_db_session, filters)

        assert len(result) == 1
        assert result[0].offered_capacity_teu == 0

    @pytest.mark.asyncio
    async def test_custom_origin_destination_filters(self, test_db_session: AsyncSession):
        """Test that custom origin/destination filters work correctly"""
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)

        # Create sailings on different routes
        sailings = [
            SailingTable(
                origin="southeast_asia",
                destination="us_west_coast",
                origin_port_code="SGSIN",
                destination_port_code="USLAX",
                service_version_and_roundtrip_identfiers="SERVICE_CUSTOM",
                origin_service_version_and_master="MASTER_CUSTOM_ORIGIN",
                destination_service_version_and_master="MASTER_CUSTOM_DEST",
                origin_at_utc=base_date,
                offered_capacity_teu=25000,
            ),
            SailingTable(
                origin="china_main",
                destination="north_europe_main",
                origin_port_code="CNSHA",
                destination_port_code="NLRTM",
                service_version_and_roundtrip_identfiers="SERVICE_DEFAULT",
                origin_service_version_and_master="MASTER_DEFAULT_ORIGIN",
                destination_service_version_and_master="MASTER_DEFAULT_DEST",
                origin_at_utc=base_date,
                offered_capacity_teu=20000,
            ),
        ]

        test_db_session.add_all(sailings)
        await test_db_session.commit()

        # Test with custom route
        filters_custom = CapacityFilterParams(
            date_from=date(2024, 1, 1),
            date_to=date(2024, 1, 7),
            from_origin="southeast_asia",
            to_destination="us_west_coast",
        )
        result_custom = await get_shipping_capacity_by_data(test_db_session, filters_custom)

        assert len(result_custom) == 1
        assert result_custom[0].offered_capacity_teu == 25000

        # Test with default route
        filters_default = CapacityFilterParams(date_from=date(2024, 1, 1), date_to=date(2024, 1, 7))
        result_default = await get_shipping_capacity_by_data(test_db_session, filters_default)

        assert len(result_default) == 1
        assert result_default[0].offered_capacity_teu == 20000
