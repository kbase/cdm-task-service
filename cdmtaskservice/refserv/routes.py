"""
CDM Refdata service endpoints.
"""

from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    Request,
    Response,
    status,
    Path as FastPath
)
from cdmtaskservice.exceptions import UnauthorizedError
from cdmtaskservice.http_bearer import KBaseHTTPBearer
from cdmtaskservice import sites
from cdmtaskservice.user import CTSUser


ROUTER_REFDATA = APIRouter(tags=["Refdata"], prefix="/refdata")

# TODO CODE this is duplicated in the standard routes.py. Make a common class?
_AUTH = KBaseHTTPBearer()


def _ensure_cts(user: CTSUser, err_msg: str):
    if not user.is_cdm_task_service:
        raise UnauthorizedError(err_msg)


# TODO CODE this is duplicated in the standard routes.py. Make a common class?
_ANN_REFDATA_ID = Annotated[str, FastPath(
    openapi_examples={"refdata id": {"value": "f0c24820-d792-4efa-a38b-2458ed8ec88f"}},
    description="The reference data ID.",
    pattern=r"^[\w-]+$",
    min_length=1,
    max_length=50,
)]


@ROUTER_REFDATA.post(
    "/{refdata_id}/{cluster}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Stage reference data",
    description="Stage reference data from a record in the CDM task service."
)
async def create_refdata(
    r: Request,
    refdata_id: _ANN_REFDATA_ID,
    cluster: Annotated[sites.Cluster, FastPath(
        description="The cluster for which the refdata should be staged."
    )],
    user: CTSUser=Depends(_AUTH)
):
    # TODO NEXT REMOVE logging
    import logging
    logging.getLogger(__name__).info(f"Refdata call {refdata_id}, {cluster}, {user}")
    _ensure_cts(user, "Only the CTS service can create reference data.")
