"""
CDM task service endpoints.
"""

import datetime
import logging
from fastapi import (
    APIRouter,
    Depends,
    Request,
    Response,
    status,
    Query,
    Path as FastPath
)
from pydantic import BaseModel, Field
from typing import Annotated

from cdmtaskservice import app_state, logfields
from cdmtaskservice import kb_auth
from cdmtaskservice import models
from cdmtaskservice.callback_url_paths import (
    get_download_complete_callback,
    get_refdata_download_complete_callback,
    get_job_complete_callback,
    get_upload_complete_callback,
    get_error_log_upload_complete_callback,
)
from cdmtaskservice.exceptions import UnauthorizedError
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice.http_bearer import KBaseHTTPBearer
from cdmtaskservice.jobflows.flowmanager import JobFlow
from cdmtaskservice.version import VERSION
from cdmtaskservice.timestamp import utcdatetime

SERVICE_NAME = "CDM Task Service Prototype"

# may need to split these into different files if this file gets too big
ROUTER_GENERAL = APIRouter(tags=["General"])
ROUTER_JOBS = APIRouter(tags=["Jobs"], prefix="/jobs")
ROUTER_IMAGES = APIRouter(tags=["Images"], prefix="/images")
ROUTER_REFDATA = APIRouter(tags=["Reference Data"], prefix="/refdata")
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
    server_time: Annotated[datetime.datetime, Field(
        example="2022-10-07T17:58:53.188698Z",
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
        "server_time": utcdatetime()
    }


class WhoAmI(BaseModel):
    """ The username associated with the provided user token and the user's admin state. """
    user: Annotated[str, Field(example="kbasehelp", description="The user's username.")]
    is_service_admin: Annotated[bool, Field(
        example=False, description="Whether the user is a service administrator."
    )]
    headers: Dict[str, str] = Field(
        example={
            "X-Forwarded-For": "203.0.113.5",
            "X-Real-IP": "192.0.2.1",
            "User-Agent": "Mozilla/5.0"
        },
        description="Request headers received from the client."
    )

@ROUTER_GENERAL.get(
    "/whoami/",
    response_model=WhoAmI,
    summary="Who am I? What does it all mean?",
    description="Information about the current user and request headers."
)
async def whoami(
    request: Request, 
    user: kb_auth.KBaseUser = Depends(_AUTH)
) -> WhoAmI:
    headers = dict(request.headers)
    return WhoAmI(
        user=user.user,
        is_service_admin=kb_auth.AdminPermission.FULL == user.admin_perm,
        headers=headers
    )


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
    job_state = app_state.get_app_state(r).job_state
    return SubmitJobResponse(job_id=await job_state.submit(job_input, user))


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


_ANN_IMAGE_ID = Annotated[str, FastPath(
    example="ghcr.io/kbase/collections:checkm2_0.1.6"
        + "@sha256:c9291c94c382b88975184203100d119cba865c1be91b1c5891749ee02193d380",
    description="The name of a docker image. Include the SHA to ensure "
        + "referencing the correct image.",
    # Don't bother validating other than some basic checks, validation will occur when
    # checking / getting the image SHA from the remote repository
    min_length=1,
    max_length=1000,
)]


@ROUTER_IMAGES.get(
    "/{image_id:path}",
    response_model=models.Image,
    summary="Get an image by name",
    description="Get an image previously registered to the system by the image name."
)
async def get_image(
    r: Request,
    image_id: _ANN_IMAGE_ID,
    # Public for now - any reason for these to be private to KBase staff?
):
    return await app_state.get_app_state(r).images.get_image(image_id)


_ANN_REFDATA_ID = Annotated[str, FastPath(
    example="f0c24820-d792-4efa-a38b-2458ed8ec88f",
    description="The reference data ID.",
    pattern=r"^[\w-]+$",
    min_length=1,
    max_length=50,
)]


@ROUTER_REFDATA.get(
    "/id/{refdata_id}",
    response_model=models.ReferenceData,
    summary="Get reference data information by ID.",
    description="Get information about reference data available for containers, including its "
        + "location and staging status, by the reference data's unique ID."
)
async def get_refdata_by_id(
    r: Request,
    refdata_id: _ANN_REFDATA_ID,
) -> models.ReferenceData:
    refdata = app_state.get_app_state(r).refdata
    return await refdata.get_refdata_by_id(refdata_id)


@ROUTER_REFDATA.get(
    "/path/{s3_path:path}",
    response_model=list[models.ReferenceData],
    summary="Get reference data information by the S3 file path.",
    description="Get information about reference data available for containers by the "
        + "reference data file path. Returns at most 1000 records."
)
async def get_refdata_by_path(
    r: Request,
    s3_path: Annotated[str, FastPath(
        example="refdata-bucket/checkm2/checkm2_refdata-2.4.tgz",
        description="The S3 path to the reference data, starting with the bucket.",
        min_length=models.S3_PATH_MIN_LENGTH,
        max_length=models.S3_PATH_MAX_LENGTH,
    )],
) -> models.ReferenceData:
    refdata = app_state.get_app_state(r).refdata
    return await refdata.get_refdata_by_path(s3_path)


@ROUTER_REFDATA.get(
    "/etag/{s3_etag}",
    response_model=list[models.ReferenceData],
    summary="Get reference data information by the S3 Etag.",
    description="Get information about reference data available for containers by the "
        + "reference data Etag. Returns at most 1000 records."
)
async def get_refdata_by_etag(
    r: Request,
    s3_etag: Annotated[str, FastPath(
        example="a70a4d1732484e75434df2c08570e1b2-3",
        description="The S3 Etag of the file.",
        min_length=models.ETAG_MIN_LENGTH,
        max_length=models.ETAG_MAX_LENGTH,
    )],
) -> models.ReferenceData:
    refdata = app_state.get_app_state(r).refdata
    return await refdata.get_refdata_by_etag(s3_etag)


@ROUTER_ADMIN.post(
    "/images/{image_id:path}",
    response_model=models.Image,
    summary="Approve an image",
    description="Approve a Docker image for use with this service. "
        + "The image must be publicly accessible and have an entrypoint. "
        + "The image may not already exist in the system."
        
)
async def approve_image(
    r: Request,
    image_id: _ANN_IMAGE_ID,
    user: kb_auth.KBaseUser=Depends(_AUTH)
) -> models.Image:
    _ensure_admin(user, "Only service administrators can approve images.")
    images = app_state.get_app_state(r).images
    return await images.register(image_id, user.user)


@ROUTER_ADMIN.post(
    "/refdata/{refdata_s3_path:path}",
    response_model=models.ReferenceData,
    summary="Create reference data",
    description="Define an S3 file as containing reference data necessary for one or more "
        + "containers and start the reference data staging process."
)
async def create_refdata(
    r: Request,
    # will be validated later 
    refdata_s3_path: Annotated[str, FastPath(
        example="refdata-bucket/checkm2/checkm2_refdata-2.4.tgz",
        description="The S3 path to the reference data to register, starting with the bucket. "
            + "If the refdata consists of multiple files, they must be archived.",
        min_length=models.S3_PATH_MIN_LENGTH,
        max_length=models.S3_PATH_MAX_LENGTH,
    )],
    etag: Annotated[str | None, Query(
        example="a70a4d1732484e75434df2c08570e1b2-3",
        description="The S3 e-tag of the file. Weak e-tags are not supported. "
            + "If provided it is checked against the target file e-tag before proceeding.",
        min_length=models.ETAG_MIN_LENGTH,
        max_length=models.ETAG_MAX_LENGTH,
    )] = None,
    unpack: Annotated[bool, Query(
        description="Whether to unpack the file after download. *.tar.gz, *.tgz, and *.gz "
            + "files are supported."
    )] = False,
    user: kb_auth.KBaseUser=Depends(_AUTH)
) -> models.Image:
    _ensure_admin(user, "Only service administrators can create reference data.")
    refdata = app_state.get_app_state(r).refdata
    return await refdata.create_refdata(refdata_s3_path, user, etag=etag, unpack=unpack)


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


@ROUTER_ADMIN.get(
    "/refdata/{refdata_id}",
    response_model=models.AdminReferenceData,
    summary="Get reference data information as an admin.",
    description="Get information about reference data available for containers, including its "
        + "location and staging status, with additional details."
)
async def get_refdata_admin(
    r: Request,
    refdata_id: _ANN_REFDATA_ID,
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> models.ReferenceData:
    _ensure_admin(user, "Only service administrators can get refdata as an admin.")
    refdata = app_state.get_app_state(r).refdata
    return await refdata.get_refdata_by_id(refdata_id, as_admin=True)


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
        + "including the expiration time.\n\n"
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
    if not nersc_cli:
        raise ValueError("NERSC is currently unavailable")
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
        + "to be called by users.",
    status_code = status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def download_complete(
    r: Request,
    job_id: _ANN_JOB_ID
):
    runner, job = await _callback_handling(r, "Download", job_id)
    await runner.download_complete(job)


@ROUTER_CALLBACKS.get(
    f"/{get_refdata_download_complete_callback()}/{{refdata_id}}/{{cluster}}",
    summary="Report refdata data download complete",
    description="Report that data download for refdata is complete. This method is not expected "
        + "to be called by users.",
    status_code = status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def refdata_download_complete(
    r: Request,
    refdata_id: _ANN_REFDATA_ID,
    cluster: Annotated[models.Cluster, FastPath(
        example=models.Cluster.PERLMUTTER_JAWS,
        description="The cluster for which the refdata download is complete."
    )]
):
    logging.getLogger(__name__).info(
        f"Download reported as complete for refdata {refdata_id} on cluster {cluster.value}",
        extra={logfields.REFDATA_ID: refdata_id, logfields.CLUSTER: cluster.value},
    )
    runner = app_state.get_app_state(r).jobflow_manager.get_flow(cluster)
    await runner.refdata_complete(refdata_id)


@ROUTER_CALLBACKS.get(
    f"/{get_job_complete_callback()}/{{job_id}}",
    summary="Report job complete",
    description="Report a remote job is complete. This method is not expected "
        + "to be called by users.",
    status_code = status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def job_complete(
    r: Request,
    job_id: _ANN_JOB_ID
):
    runner, job = await _callback_handling(r, "Remote job", job_id)
    await runner.job_complete(job)


@ROUTER_CALLBACKS.get(
    f"/{get_upload_complete_callback()}/{{job_id}}",
    summary="Report data upload complete",
    description="Report that data upload for a job is complete. This method is not expected "
        + "to be called by users.",
    status_code = status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def upload_complete(
    r: Request,
    job_id: _ANN_JOB_ID
):
    runner, job = await _callback_handling(r, "Upload", job_id)
    await runner.upload_complete(job)


@ROUTER_CALLBACKS.get(
    f"/{get_error_log_upload_complete_callback()}/{{job_id}}",
    summary="Report error log file upload complete",
    description="Report that log file upload for a job in an errored state is complete. "
        + "This method is not expected to be called by users.",
    status_code = status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def error_log_upload_complete(
    r: Request,
    job_id: _ANN_JOB_ID
):
    runner, job = await _callback_handling(r, "Error log upload", job_id)
    await runner.error_log_upload_complete(job)


async def _callback_handling(
    r: Request, operation: str, job_id: str
) -> (JobFlow, models.AdminJobDetails):
    logging.getLogger(__name__).info(
        f"{operation} reported as complete for job {job_id}",
        extra={logfields.JOB_ID: job_id},
    )
    appstate = app_state.get_app_state(r)
    job = await appstate.job_state.get_job(job_id, _SERVICE_USER, as_admin=True)
    return appstate.jobflow_manager.get_flow(job.job_input.cluster), job


class ClientLifeTimeError(Exception):
    """ An error thrown when a client's lifetime is less than required. """
