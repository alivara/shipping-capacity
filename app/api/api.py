from typing import Annotated

from fastapi import APIRouter, Query, Security

from app.api.schemas import CapacityFilterParams


class AuthenticatedUser:
    """
    Dependency to ensure the user is authenticated.
    """

    def __call__(self):
        # Authentication logic goes here
        pass


router = APIRouter(prefix="/")


@router.get(
    "/capacity",
    response_model=list(dict),
    dependencies=[Security(AuthenticatedUser)],
)
def get_capacity(
    filter_query: Annotated[CapacityFilterParams, Query()],
) -> list[dict]:
    """
    Get shipping capacity for a specific date range.
    """
    return []
