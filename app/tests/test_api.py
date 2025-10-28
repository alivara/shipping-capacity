import pytest
from httpx import AsyncClient


class TestCapacityEndpoint:
    """Tests for the capacity calculation endpoint"""

    @pytest.mark.asyncio
    async def test_get_capacity_date_from_after_date_to(
        self, async_client_with_db: AsyncClient, test_db_with_data
    ):
        """
        Test that API returns 422 when date_from is after date_to.

        This validation happens at the Pydantic schema level.
        """
        date_from = "2024-03-01"
        date_to = "2024-02-01"

        response = await async_client_with_db.get(
            f"/capacity?date_from={date_from}&date_to={date_to}"
        )
        print(response.status_code)
        # Pydantic validation returns 422 (not 400)
        assert response.status_code == 422
        # Check that error mentions date validation
        error_detail = response.json()["detail"]
        assert any("date" in str(err).lower() for err in error_detail)

    @pytest.mark.asyncio
    async def test_get_capacity_missing_dates(self, async_client_with_db: AsyncClient):
        """Test that API returns 422 when date_from parameter is missing"""
        # Missing date_from
        response = await async_client_with_db.get("/capacity?date_to=2024-02-01")
        assert response.status_code == 422

        # Missing date_to
        response = await async_client_with_db.get("/capacity?date_from=2024-01-01")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_get_capacity_invalid_date_format(self, async_client_with_db: AsyncClient):
        """
        Test that API returns 422 when date format is invalid.

        API expects dates in YYYY-MM-DD format.
        """
        response = await async_client_with_db.get(
            "/capacity?date_from=01-01-2024&date_to=2024-02-01"
        )
        assert response.status_code == 422
