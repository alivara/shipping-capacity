import datetime

from pydantic import BaseModel, Field


class CapacityFilterParams(BaseModel):
    model_config = {"extra": "forbid"}

    # TODO check if the date format is correct
    date_from: datetime = (Field(..., description="Start date in YYYY-MM-DD format"),)
    date_to: datetime = (Field(..., description="End date in YYYY-MM-DD format"),)
    limit: int = Field(100, gt=0, le=100)  # TODO if needed ?
    offset: int = Field(0, ge=0)  # TODO if needed ?
