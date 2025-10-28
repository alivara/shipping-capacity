"""
Pydantic schemas for capacity API requests and responses.

This module defines the data models used for API input validation
and response serialization.
"""

from datetime import date

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CapacityResponse(BaseModel):
    """
    Response model for weekly shipping capacity data.

    Attributes:
        week_start_date: The first day of the week (Monday) in ISO 8601 format
        week_no: ISO week number (1-53)
        offered_capacity_teu: 4-week rolling average of offered capacity in TEU

    Example:
        {
            "week_start_date": "2024-01-08",
            "week_no": 2,
            "offered_capacity_teu": 123000
        }
    """

    week_start_date: date = Field(
        ...,
        description="First day of the week (Monday) in YYYY-MM-DD format",
        examples=["2024-01-08", "2024-01-15"],
    )
    week_no: int = Field(..., description="ISO week number (1-53)", ge=1, le=53, examples=[1, 2, 3])
    offered_capacity_teu: int = Field(
        ...,
        description="4-week rolling average of offered capacity in Twenty-foot Equivalent Units",
        ge=0,
        examples=[123000, 125000, 130000],
    )

    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "example": {
                "week_start_date": "2024-01-08",
                "week_no": 2,
                "offered_capacity_teu": 123000,
            }
        },
    )


class CapacityFilterParams(BaseModel):
    """
    Query parameters for capacity API endpoint.

    Used to filter capacity data requests.

    Attributes:
        date_from: Start date of the period (inclusive)
        date_to: End date of the period (inclusive)

    Validation:
        - date_from and date_to must be in YYYY-MM-DD format
        - No extra fields are allowed
    """

    date_from: date = Field(
        ..., description="Start date in YYYY-MM-DD format (inclusive)", examples=["2024-01-01"]
    )
    date_to: date = Field(
        ..., description="End date in YYYY-MM-DD format (inclusive)", examples=["2024-03-31"]
    )
    from_origin: str = Field(
        default="china_main",
        description="Origin region (default: china_main)",
        examples=["china_main"],
    )
    to_destination: str = Field(
        default="north_europe_main",
        description="Destination region (default: north_europe_main)",
        examples=["north_europe_main"],
    )
    # TODO: better to have pagination
    # limit: int = Field(
    #     default=100,
    #     gt=0,
    #     le=100,
    #     description="Maximum number of results to return (pagination, not currently applied)"
    # )
    # offset: int = Field(
    #     default=0,
    #     ge=0,
    #     description="Number of results to skip (pagination, not currently applied)"
    # )

    @model_validator(mode="after")
    def validate_dates(self):
        if self.date_to < self.date_from:
            raise ValueError("date_to cannot be before date_from")
        return self

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "date_from": "2024-01-01",
                "date_to": "2024-03-31",
                # TODO: better to have pagination
                # "limit": 100,
                # "offset": 0,
            }
        },
    )
