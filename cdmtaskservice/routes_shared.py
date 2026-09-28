"""
Route-layer code shared between cdmtaskservice.routes and cdmtaskservice.pipelines.routes.
"""

from fastapi import Query, Request
from kbase.auth import InvalidUserError
from pydantic import AwareDatetime
from typing import Annotated

from cdmtaskservice import app_state
from cdmtaskservice import models
from cdmtaskservice import sites
from cdmtaskservice.exceptions import UnauthorizedError
from cdmtaskservice.user import CTSUser


def ensure_admin(user: CTSUser, err_msg: str):
    if not user.is_full_admin():
        raise UnauthorizedError(err_msg)


def ensure_admin_or_executor(user: CTSUser, err_msg: str):
    if not user.is_full_admin() and not user.is_external_executor:
        raise UnauthorizedError(err_msg)


ANN_JOB_SITE = Annotated[sites.Cluster | None, Query(
    description="Filter jobs by the site where the job ran."
)]
ANN_JOB_STATE = Annotated[models.JobState | None, Query(
    description="Filter jobs by the state of the job."
)]
ANN_JOB_AFTER = Annotated[AwareDatetime | None, Query(
    openapi_examples={"isodate": {"value": "2024-10-24T22:35:40Z"}},
    description="Filter jobs where the last update time is newer than the provided date, "
        + "inclusive",
)]
ANN_JOB_BEFORE = Annotated[AwareDatetime | None, Query(
    openapi_examples={"isodate": {"value": "2024-10-24T22:35:59.999Z"}},
    description="Filter jobs where the last update time is older than the provided date, "
        + "exclusive",
)]
ANN_JOB_LIMIT = Annotated[int | None, Query(
    openapi_examples={"max value": {"value": 1000}},
    description="The maximum number of jobs to return",
    ge=1,
    le=1000,
)]
ANN_JOB_ADMIN_USER = Annotated[str, Query(
    openapi_examples={"kbasehelp user": {"value": "kbasehelp"}},
    description="Filter jobs by the owner of the job.",
    min_length=1,
    max_length=100,
    pattern=r"^[a-z][a-z\d_]*$",
)]


async def ensure_valid_kbase_user(r: Request, user: str | None):
    """ Check that the given user, if any, is a valid KBase user. """
    auth = app_state.get_app_state(r).auth
    if user and not await auth.is_valid_kbase_user(user, app_state.get_request_token(r)):
        raise InvalidUserError(f"No such user: {user}")
