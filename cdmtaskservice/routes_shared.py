"""
Route-layer code shared between cdmtaskservice.routes and cdmtaskservice.pipelines.routes.
"""

from enum import Enum
from fastapi import Query, Request
from kbase.auth import InvalidUserError
from pydantic import AwareDatetime
from typing import Annotated

from cdmtaskservice import app_state
from cdmtaskservice import models
from cdmtaskservice import sites
from cdmtaskservice.exceptions import NoSuchJobError, UnauthorizedError
from cdmtaskservice.jobflows.flowmanager import JobFlow
from cdmtaskservice.pipelines import models as pipe_models
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


class JobIDType(Enum):
    """ Which kind(s) of job a job ID is expected to refer to. """
    STANDARD = "standard"
    """ The job ID must refer to a standard (non-pipeline) job. """
    PIPELINE = "pipeline"
    """ The job ID must refer to a pipeline job. """
    EITHER = "either"
    """ The job ID may refer to a standard or a pipeline job, distinguished by ID prefix. """


async def get_job_and_flow(
    r: Request,
    job_id: str,
    user: CTSUser,
    *,
    job_id_type: JobIDType = JobIDType.STANDARD,
    as_admin: bool = False,
) -> tuple[models.AdminJobDetails | pipe_models.AdminPipelineJob, JobFlow]:
    """
    Fetch a job with admin-level details and its associated job flow. The returned job is an
    AdminJobDetails or AdminPipelineJob instance and MUST NOT be returned directly to
    non-admin users.

    job_id_type - which kind(s) of job job_id is expected to refer to. If EITHER, the job ID's
        prefix determines whether a standard or pipeline job is fetched. If PIPELINE or
        STANDARD, job_id's prefix is checked up front and NoSuchJobError is raised immediately,
        without a database round trip, if it doesn't match the expected job type.
    as_admin - True if the user should always have access to the job and should access
        additional job details.
    """
    is_pipeline_id = pipe_models.is_pipeline_job_id(job_id)
    if job_id_type is JobIDType.STANDARD and is_pipeline_id:
        raise NoSuchJobError(f"No job with ID '{job_id}' exists")
    if job_id_type is JobIDType.PIPELINE and not is_pipeline_id:
        raise NoSuchJobError(f"No pipeline job with ID '{job_id}' exists")
    job_state = app_state.get_app_state(r).job_state
    if is_pipeline_id:
        job = await job_state.get_pipeline_job(job_id, user, as_admin=as_admin, admin_details=True)
    else:
        job = await job_state.get_job(job_id, user, as_admin=as_admin, admin_details=True)
    flow = await app_state.get_app_state(r).jobflow_manager.get_flow(job.get_cluster())
    return job, flow


async def admin_get_job_and_flow(
    r: Request,
    job_id: str,
    user: CTSUser,
    action: str,
    *,
    job_id_type: JobIDType = JobIDType.STANDARD,
) -> tuple[models.AdminJobDetails | pipe_models.AdminPipelineJob, JobFlow]:
    """
    Verify the user is a full admin, then fetch the job with admin-level details and its
    associated job flow. The returned job is an AdminJobDetails or AdminPipelineJob instance
    and MUST NOT be returned directly to non-admin users.

    action - the action being performed, appended to "Only service administrators can ".
    job_id_type - see get_job_and_flow.
    """
    ensure_admin(user, f"Only service administrators can {action}")
    return await get_job_and_flow(r, job_id, user, job_id_type=job_id_type, as_admin=True)


async def ensure_valid_kbase_user(r: Request, user: str | None):
    """ Check that the given user, if any, is a valid KBase user. """
    auth = app_state.get_app_state(r).auth
    if user and not await auth.is_valid_kbase_user(user, app_state.get_request_token(r)):
        raise InvalidUserError(f"No such user: {user}")
