from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class CapacityResponse(BaseModel):
    week_start_date: date
    week_no: int
    offered_capacity_teu: int

    model_config = ConfigDict(from_attributes=True)


class CapacityFilterParams(BaseModel):
    """Query parameters for capacity API."""

    date_from: date = Field(..., description="Start date in YYYY-MM-DD format")
    date_to: date = Field(..., description="End date in YYYY-MM-DD format")
    limit: int = Field(default=100, gt=0, le=100)
    offset: int = Field(default=0, ge=0)

    model_config = ConfigDict(extra="forbid")
