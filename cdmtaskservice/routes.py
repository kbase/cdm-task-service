"""
CDM task service endpoints.
"""

import datetime
import logging
from fastapi import (
    APIRouter,
    Depends,
    Request,
    Query,
    Path as FastPath
)
from pydantic import BaseModel, Field
from typing import Annotated

from cdmtaskservice import app_state
from cdmtaskservice import kb_auth
from cdmtaskservice import models
from cdmtaskservice.callback_url_paths import (
    get_download_complete_callback,
    get_job_complete_callback,
)
from cdmtaskservice.exceptions import UnauthorizedError
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice.http_bearer import KBaseHTTPBearer
from cdmtaskservice.version import VERSION
from cdmtaskservice.timestamp import timestamp, utcdatetime

SERVICE_NAME = "CDM Task Service Prototype"

# may need to split these into different files if this file gets too big
ROUTER_GENERAL = APIRouter(tags=["General"])
ROUTER_JOBS = APIRouter(tags=["Jobs"], prefix="/jobs")
ROUTER_ADMIN = APIRouter(tags=["Admin"], prefix="/admin")
ROUTER_CALLBACKS = APIRouter(tags=["Callbacks"])

_AUTH = KBaseHTTPBearer()

# * isn't allowed in KBase user names
_SERVICE_USER = "***SERVICE***"


def _ensure_admin(user: kb_auth.KBaseUser, err_msg: str):
    if user.admin_perm != kb_auth.AdminPermission.FULL:
        raise UnauthorizedError(err_msg)


class Root(BaseModel):
    """ General information about the service """
    service_name: Annotated[
        str, Field(example=SERVICE_NAME, description="The name of the service."
    )]
    version: Annotated[
        str, Field(example=VERSION, description="The semantic version of the service."
    )]
    git_hash: Annotated[str, Field(
        example="b78f6e15e85381a7df71d6005d99e866f3f868dc",
        description="The git commit of the service code."
    )]
    server_time: Annotated[str, Field(
        example="2022-10-07T17:58:53.188698+00:00",
        description="The server's time as an ISO8601 string."
    )]


@ROUTER_GENERAL.get(
    "/",
    response_model=Root,
    summary="General service info",
    description="General information about the service.")
async def root() -> Root:
    return {
        "service_name": SERVICE_NAME,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": timestamp()
    }


class WhoAmI(BaseModel):
    """ The username associated with the provided user token and the users's admin state. """
    user: Annotated[str, Field(example="kbasehelp", description="The users's username.")]
    is_service_admin: Annotated[bool, Field(
        example=False, description="Whether the user is a service administrator."
    )]
    

@ROUTER_GENERAL.get(
    "/whoami/",
    response_model=WhoAmI,
    summary="Who am I? What does it all mean?",
    description="Information about the current user."
)
async def whoami(user: kb_auth.KBaseUser=Depends(_AUTH)) -> WhoAmI:
    return {
        "user": user.user,
        "is_service_admin": kb_auth.AdminPermission.FULL == user.admin_perm
    }


class SubmitJobResponse(BaseModel):
    """ The response to a successful job submission request. """
    job_id: Annotated[str, Field(description="An opaque job ID.")]


@ROUTER_JOBS.post(
    "/",
    response_model=SubmitJobResponse,
    summary="Submit a job",
    description="Submit a job to the system."
)
async def submit_job(
    r: Request,
    job_input: models.JobInput,
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> SubmitJobResponse:
    job_submit = app_state.get_app_state(r).job_submit
    return SubmitJobResponse(job_id=await job_submit.submit(job_input, user))


_ANN_JOB_ID = Annotated[str, FastPath(
    example="f0c24820-d792-4efa-a38b-2458ed8ec88f",
    description="The job ID.",
    pattern=r"^[\w-]+$",
    min_length=1,
    max_length=50,
)]


@ROUTER_JOBS.get(
    "/{job_id}",
    response_model=models.Job,
    summary="Get a job",
    description="Get a job. Only the submitting user may view the job."
)
async def get_job(
    r: Request,
    job_id: _ANN_JOB_ID,
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> models.Job:
    job_state = app_state.get_app_state(r).job_state
    return await job_state.get_job(job_id, user.user)


@ROUTER_ADMIN.post(
    "/images/{image_id:path}",
    response_model=models.Image,
    summary="Approve an image",
    description="Approve a Docker image for use with this service. "
        + "The image must be publicly acessible and have an entrypoint. "
        + "The image may not already exist in the system."
        
)
async def approve_image(
    r: Request,
    image_id: Annotated[str, FastPath(
        example="ghcr.io/kbase/collections:checkm2_0.1.6"
            + "@sha256:c9291c94c382b88975184203100d119cba865c1be91b1c5891749ee02193d380",
        description="The Docker image to run for the job. Include the SHA to ensure the "
            + "exact code requested is run.",
        # Don't bother validating other than some basic checks, validation will occur when
        # checking / getting the image SHA from the remote repository
        min_length=1,
        max_length=1000,
    )],
    user: kb_auth.KBaseUser=Depends(_AUTH)
) -> models.Image:
    _ensure_admin(user, "Only service administrators can approve images.")
    images = app_state.get_app_state(r).images
    return await images.register(image_id)


@ROUTER_ADMIN.get(
    "/jobs/{job_id}",
    response_model=models.AdminJobDetails,
    summary="Get a job as an admin",
    description="Get any job, regardless of ownership, with additional details about the job run."
)
async def get_job_admin(
    r: Request,
    job_id: _ANN_JOB_ID,
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> models.AdminJobDetails:
    _ensure_admin(user, "Only service administrators can get jobs as an admin.")
    job_state = app_state.get_app_state(r).job_state
    return await job_state.get_job(job_id, user.user, as_admin=True)


class NERSCClientInfo(BaseModel):
    """ Provides information about the NERSC SFAPI client. """
    id: Annotated[str, Field(description="The opaque ID of the client.")]
    expires_at: Annotated[datetime.datetime, Field(
        example="2024-10-24T22:35:40Z",
        description="The time the client expires in ISO8601 format."
    )]
    expires_in: Annotated[datetime.timedelta, Field(
        example="PT12H30M5S",
        description="The time duration until the client expires as an ISO8601 duration string."
    )]


@ROUTER_ADMIN.get(
    "/clients/nersc",
    response_model=NERSCClientInfo,
    summary="NERSC SFAPI client info",
    description="Get information about the NERSC Superfacility API client "
        + "including the expiration time,\n"
        + "Administrator credentials are required."
)
async def get_nersc_client_info(
    r: Request,
    require_lifetime: Annotated[datetime.timedelta, Query(
        example="P30DT12H30M5S",
        description="The required remaining lifetime of the client as an "
            + "ISO8601 duration string. If the lifetime is shorter than this value, an error "
            + "will be returned.",
        ge=1
    )] = None,
    user: kb_auth.KBaseUser=Depends(_AUTH)
) -> NERSCClientInfo:
    _ensure_admin(user, "Only service administrators may view NERSC client information.")
    nersc_cli = app_state.get_app_state(r).sfapi_client
    expires = nersc_cli.expiration()
    expires_in = expires - utcdatetime()
    if require_lifetime and expires_in < require_lifetime:
        raise ClientLifeTimeError(f"The client lifetime, {expires_in}, is less than the "
                                  + f"required lifetime, {require_lifetime}")
    return NERSCClientInfo(
        id=nersc_cli.get_client_id(),
        expires_at=expires,
        expires_in=expires_in,
    )


@ROUTER_CALLBACKS.get(
    f"/{get_download_complete_callback()}/{{job_id}}",
    summary="Report data download complete",
    description="Report that data download for a job is complete. This method is not expected "
        + "to be called by users."
)
async def download_complete(
    r: Request,
    job_id: _ANN_JOB_ID
):
    logging.getLogger(__name__).info(f"Download reported as complete for job {job_id}")
    appstate = app_state.get_app_state(r)
    job = await appstate.job_state.get_job(job_id, _SERVICE_USER, as_admin=True)
    await appstate.runners[job.job_input.cluster].download_complete(job)


@ROUTER_CALLBACKS.get(
    f"/{get_job_complete_callback()}/{{job_id}}",
    summary="Report job complete",
    description="Report a remote job is complete. This method is not expected "
        + "to be called by users."
)
async def job_complete(
    r: Request,
    job_id: _ANN_JOB_ID
):
    logging.getLogger(__name__).info(f"Remote job reported as complete for job {job_id}")
    # TODO JOBS implement when job is complete
    raise NotImplementedError()


class ClientLifeTimeError(Exception):
    """ An error thrown when a client's lifetime is less than required. """
