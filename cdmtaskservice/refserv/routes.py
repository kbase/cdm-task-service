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

from cdmtaskservice import app_state
from cdmtaskservice.exceptions import UnauthorizedError
from cdmtaskservice.http_bearer import KBaseHTTPBearer
from cdmtaskservice import sites
from cdmtaskservice.refserv import models
from cdmtaskservice.user import CTSUser


ROUTER_REFDATA = APIRouter(tags=["Refdata"], prefix="/refdata")

# TODO CODE this is duplicated in the standard routes.py. Make a common class?
_AUTH = KBaseHTTPBearer()


def _ensure_admin(user: CTSUser, err_msg: str):
    if not user.is_full_admin():
        raise UnauthorizedError(err_msg)


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
    _ensure_cts(user, "Only the CTS service can create reference data.")
    refman = app_state.get_app_state(r).refdata_manager
    await refman.stage_refdata(refdata_id, cluster)


@ROUTER_REFDATA.get(
    "/{refdata_id}/",
    response_model=models.RefdataLocalStatus,
    response_model_exclude_none=True,
    summary="Get reference data state",
    description="This is an admin oriented sort-of-hacky method to get information about a "
        + "local refdata staging process."
)
async def get_refdata_status(
    r: Request,
    refdata_id: _ANN_REFDATA_ID,
    user: CTSUser=Depends(_AUTH)
):
    _ensure_admin(user, "Only service admins can get refdata state.")
    refman = app_state.get_app_state(r).refdata_manager
    return await refman.get_refdata_state(refdata_id)
