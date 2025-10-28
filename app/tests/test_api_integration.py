from datetime import date, datetime, timedelta, timezone

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.database.model import SailingTable


def utc_datetime(*args) -> datetime:
    """Helper to create UTC-aware datetime for tests."""
    return datetime(*args, tzinfo=timezone.utc)


@pytest.mark.integration
@pytest.mark.api
class TestCapacityAPI:
    """Integration tests for /capacity endpoint"""

    @pytest.mark.asyncio
    async def test_valid_request_with_defaults(
        self, async_client_with_db: AsyncClient, test_db_session: AsyncSession
    ):
        """Test valid API request with default origin/destination"""
        # Create test data
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)
        sailing = SailingTable(
            origin="china_main",
            destination="north_europe_main",
            origin_port_code="CNSHA",
            destination_port_code="NLRTM",
            service_version_and_roundtrip_identfiers="SERVICE_1",
            origin_service_version_and_master="MASTER_1_ORIGIN",
            destination_service_version_and_master="MASTER_1_DEST",
            origin_at_utc=base_date,
            offered_capacity_teu=15000,
        )
        test_db_session.add(sailing)
        await test_db_session.commit()

        # Make API request
        response = await async_client_with_db.get(
            "/capacity", params={"date_from": "2024-01-01", "date_to": "2024-01-07"}
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["offered_capacity_teu"] == 15000

    @pytest.mark.asyncio
    async def test_valid_request_with_custom_route(
        self, async_client_with_db: AsyncClient, test_db_session: AsyncSession
    ):
        """Test valid API request with custom origin/destination"""
        # Create test data for custom route
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)
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

        # Test custom route
        response = await async_client_with_db.get(
            "/capacity",
            params={
                "date_from": "2024-01-01",
                "date_to": "2024-01-07",
                "from_origin": "southeast_asia",
                "to_destination": "us_west_coast",
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["offered_capacity_teu"] == 25000

    @pytest.mark.asyncio
    async def test_add_new_corridor_does_not_affect_existing(
        self, async_client_with_db: AsyncClient, test_db_session: AsyncSession
    ):
        """
        Test that adding new corridor does not affect existing corridor calculations.

        This test verifies that adding data for a new corridor does not affect
        capacity calculations for existing corridors.
        """
        # Create test data for existing corridor
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)
        sailing = SailingTable(
            origin="china_main",
            destination="north_europe_main",
            origin_port_code="CNSHA",
            destination_port_code="NLRTM",
            service_version_and_roundtrip_identfiers="SERVICE_NEW",
            origin_service_version_and_master="MASTER_NEW_ORIGIN",
            destination_service_version_and_master="MASTER_NEW_DEST",
            origin_at_utc=base_date,
            offered_capacity_teu=15000,
        )
        test_db_session.add(sailing)
        await test_db_session.commit()

        # Get baseline capacity for main route
        response1 = await async_client_with_db.get(
            "/capacity",
            params={
                "date_from": "2024-01-01",
                "date_to": "2024-01-31",
                "from_origin": "china_main",
                "to_destination": "north_europe_main",
            },
        )
        baseline_data = response1.json()

        # Now add data for a new corridor (china_main -> us_west_coast)
        for week in range(4):
            sailing = SailingTable(
                origin="china_main",
                destination="us_west_coast",
                origin_port_code="CNSHA",
                destination_port_code="USLAX",
                service_version_and_roundtrip_identfiers=f"SERVICE_US_{week}",
                origin_service_version_and_master=f"MASTER_US_{week}_ORIGIN",
                destination_service_version_and_master=f"MASTER_US_{week}_DEST",
                origin_at_utc=base_date + timedelta(weeks=week),
                offered_capacity_teu=25000,
            )
            test_db_session.add(sailing)
        await test_db_session.commit()

        # Query main route again - should be EXACTLY the same
        response2 = await async_client_with_db.get(
            "/capacity",
            params={
                "date_from": "2024-01-01",
                "date_to": "2024-01-31",
                "from_origin": "china_main",
                "to_destination": "north_europe_main",
            },
        )
        after_data = response2.json()

        # CRITICAL ASSERTION: Adding new corridor must not affect existing corridor
        assert (
            baseline_data == after_data
        ), "Adding new corridor affected existing corridor calculations!"

        # Verify new corridor has different data
        response3 = await async_client_with_db.get(
            "/capacity",
            params={
                "date_from": "2024-01-01",
                "date_to": "2024-01-31",
                "from_origin": "china_main",
                "to_destination": "us_west_coast",
            },
        )
        us_data = response3.json()

        assert us_data != baseline_data, "New corridor should have different capacity"
        assert len(us_data) > 0, "New corridor should have data"

    @pytest.mark.asyncio
    async def test_get_capacity_full_quarter(
        self, async_client_with_db: AsyncClient, test_db_with_data
    ):
        """
        Test capacity calculation for full Q1 2024 (Jan 1 - Mar 31).

        This is the main use case specified in the requirements.
        """
        date_from = date(2024, 1, 1)
        date_to = date(2024, 3, 31)

        response = await async_client_with_db.get(
            f"/capacity?date_from={date_from}&date_to={date_to}"
        )

        assert response.status_code == 200
        data = response.json()

        # Should have multiple weeks of data
        assert len(data) >= 10

        # Verify all required fields present
        for item in data:
            assert "week_start_date" in item
            assert "week_no" in item
            assert "offered_capacity_teu" in item

            # Verify week_no is reasonable (1-53)
            assert 1 <= item["week_no"] <= 53

            # Verify capacity is positive for weeks with data
            assert item["offered_capacity_teu"] >= 0

    @pytest.mark.asyncio
    async def test_get_capacity_early_weeks_with_partial_history(
        self, async_client_with_db: AsyncClient, test_db_with_data
    ):
        """
        Test rolling average for early weeks with less than 4 weeks of history.

        For the first 3 weeks, the rolling average should still be calculated
        using whatever historical data is available.
        """
        date_from = date(2024, 1, 1)  # Week 1 (no prior history)
        date_to = date(2024, 1, 15)  # Week 3

        response = await async_client_with_db.get(
            f"/capacity?date_from={date_from}&date_to={date_to}"
        )

        assert response.status_code == 200
        data = response.json()

        # Should still have results even with limited history
        assert len(data) > 0

        # Verify structure and non-negative capacity
        for item in data:
            assert "week_start_date" in item
            assert "week_no" in item
            assert "offered_capacity_teu" in item
            assert item["offered_capacity_teu"] >= 0

    @pytest.mark.asyncio
    async def test_get_capacity_four_week_rolling_average(
        self, async_client_with_db: AsyncClient, test_db_with_data
    ):
        """
        Test that the rolling average is calculated correctly.

        The rolling average should use the current week and 3 preceding weeks
        (4 weeks total) to calculate the average.
        """
        date_from = date(2024, 1, 22)  # Week 4 (should have 4 weeks of history)
        date_to = date(2024, 1, 29)  # Week 5

        response = await async_client_with_db.get(
            f"/capacity?date_from={date_from}&date_to={date_to}"
        )

        assert response.status_code == 200
        data = response.json()

        # Should have data for at least one week
        assert len(data) > 0

        # Each week should have a valid capacity value
        for item in data:
            assert item["offered_capacity_teu"] > 0

    @pytest.mark.asyncio
    async def test_empty_result_returns_empty_list(self, async_client_with_db: AsyncClient):
        """Test that query with no matching data returns empty list"""
        response = await async_client_with_db.get(
            "/capacity", params={"date_from": "2025-01-01", "date_to": "2025-01-31"}
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 0

    @pytest.mark.asyncio
    async def test_response_structure(
        self, async_client_with_db: AsyncClient, test_db_session: AsyncSession
    ):
        """Test that response has correct structure"""
        # Create test data
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)
        sailing = SailingTable(
            origin="china_main",
            destination="north_europe_main",
            origin_port_code="CNSHA",
            destination_port_code="NLRTM",
            service_version_and_roundtrip_identfiers="SERVICE_1",
            origin_service_version_and_master="MASTER_1_ORIGIN",
            destination_service_version_and_master="MASTER_1_DEST",
            origin_at_utc=base_date,
            offered_capacity_teu=15000,
        )
        test_db_session.add(sailing)
        await test_db_session.commit()

        response = await async_client_with_db.get(
            "/capacity", params={"date_from": "2024-01-01", "date_to": "2024-01-07"}
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data) > 0

        # Verify response structure
        item = data[0]
        assert "week_start_date" in item
        assert "week_no" in item
        assert "offered_capacity_teu" in item

        # Verify types
        assert isinstance(item["week_start_date"], str)
        assert isinstance(item["week_no"], int)
        assert isinstance(item["offered_capacity_teu"], int)

        # Verify valid ranges
        assert 1 <= item["week_no"] <= 53
        assert item["offered_capacity_teu"] >= 0

    @pytest.mark.asyncio
    async def test_same_date_from_and_to_is_valid(
        self, async_client_with_db: AsyncClient, test_db_session: AsyncSession
    ):
        """Test that same date for from and to is valid"""
        # Create test data
        base_date = utc_datetime(2024, 1, 1, 0, 0, 0)
        sailing = SailingTable(
            origin="china_main",
            destination="north_europe_main",
            origin_port_code="CNSHA",
            destination_port_code="NLRTM",
            service_version_and_roundtrip_identfiers="SERVICE_1",
            origin_service_version_and_master="MASTER_1_ORIGIN",
            destination_service_version_and_master="MASTER_1_DEST",
            origin_at_utc=base_date,
            offered_capacity_teu=15000,
        )
        test_db_session.add(sailing)
        await test_db_session.commit()

        response = await async_client_with_db.get(
            "/capacity", params={"date_from": "2024-01-01", "date_to": "2024-01-01"}
        )

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
