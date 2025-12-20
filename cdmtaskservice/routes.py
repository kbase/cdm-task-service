"""
CDM task service endpoints.
"""

import datetime
from enum import Enum
import logging
from fastapi import (
    APIRouter,
    Depends,
    Path as FastPath,
    Request,
    Response,
    status,
    Query,
)
from fastapi.responses import FileResponse, StreamingResponse
from kbase.auth import InvalidUserError
from pathlib import Path
from pydantic import BaseModel, Field, AwareDatetime, ConfigDict
from typing import Annotated, Any

from cdmtaskservice import app_state
from cdmtaskservice import localfiles
from cdmtaskservice import logfields
from cdmtaskservice import models
from cdmtaskservice import sites
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
from cdmtaskservice.user import CTSUser, CTSRole, SERVICE_USER

NOTES = "This service is a prototype"

# may need to split these into different files if this file gets too big
ROUTER_GENERAL = APIRouter(tags=["General"])
ROUTER_SITES = APIRouter(tags=["Sites"])
ROUTER_JOBS = APIRouter(tags=["Jobs"], prefix="/jobs")
ROUTER_IMAGES = APIRouter(tags=["Images"], prefix="/images")
ROUTER_REFDATA = APIRouter(tags=["Reference Data"], prefix="/refdata")
ROUTER_ADMIN = APIRouter(tags=["Admin"], prefix="/admin")
ROUTER_CALLBACKS = APIRouter(tags=["Callbacks"])
ROUTER_EXTERNAL_EXEC = APIRouter(tags=["External Execution"])

_AUTH = KBaseHTTPBearer()


def _ensure_admin(user: CTSUser, err_msg: str):
    if not user.is_full_admin():
        raise UnauthorizedError(err_msg)


def _ensure_executor(user: CTSUser, err_msg: str):
    if not user.is_external_executor:
        raise UnauthorizedError(err_msg)


def _ensure_refdata_service(user: CTSUser, err_msg: str):
    if not user.is_refdata_service:
        raise UnauthorizedError(err_msg)


def _ensure_admin_or_executor(user: CTSUser, err_msg: str):
    if not user.is_full_admin() and not user.is_external_executor:
        raise UnauthorizedError(err_msg)


class Root(BaseModel):
    """ General information about the service """
    service_name: Annotated[str, Field(description="The name of the service.")]
    version: Annotated[str, Field(
        examples=[VERSION], description="The semantic version of the service."
    )]
    git_hash: Annotated[str, Field(
        examples=["b78f6e15e85381a7df71d6005d99e866f3f868dc"],
        description="The git commit of the service code."
    )]
    server_time: Annotated[datetime.datetime, Field(
        examples=["2022-10-07T17:58:53.188698Z"],
        description="The server's time as an ISO8601 string."
    )]
    notes: Annotated[str, Field(description="Notes about the service.")]


@ROUTER_GENERAL.get(
    "/",
    response_model=Root,
    summary="General service info",
    description="General information about the service.")
async def root(r: Request) -> Root:
    return {
        "service_name": app_state.get_app_state(r).service_name,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": utcdatetime(),
        "notes": NOTES,
    }

class PathPermission(str, Enum):
    """ The permission for an S3 path. """
    # Add read later if necessary
    WRITE = "write"


class AllowedPath(BaseModel):
    """ A path where a user can read and / or write data to the S3 filesystem. """
    path: Annotated[str, Field(
        examples=[["cts/io"]],
        description="An S3 path where the user is allowed to read and / or write files."
    )]
    perm: Annotated[PathPermission, Field(
        examples=[[PathPermission.WRITE]],
        description="The permission for the path."
    )]


class WhoAmI(BaseModel):
    """ Information about the user. """
    user: Annotated[str, Field(examples=["kbasehelp"], description="The user's username.")]
    roles: Annotated[list[CTSRole], Field(
        examples=[[CTSRole.FULL_ADMIN]], description="The users's roles for the service."
    )]
    allowed_paths: Annotated[list[AllowedPath], Field(
        examples=[AllowedPath(path="cts/io", perm=PathPermission.WRITE)],
        description="The S3 paths where the user is allowed to read and / or write files. "
            + "If empty, the user can read and write to anywhere the service can read and write, "
            + "other than where job logs are stored."
    )]


@ROUTER_GENERAL.get(
    "/whoami/",
    response_model=WhoAmI,
    summary="Who am I? What does it all mean?",
    description="Information about the current user."
)
async def whoami(r: Request, user: CTSUser=Depends(_AUTH)) -> WhoAmI:
    # Later this can be updated to a dynamic lookup if necessary
    aps = app_state.get_app_state(r).allowed_paths
    return WhoAmI(
        user=user.user,
        roles=user.roles,
        allowed_paths=[AllowedPath(path=p, perm=PathPermission.WRITE) for p in aps],
    )


class Site(sites.ComputeSite):
    """
    Information about a remote compute site and its status.
    
    For a site to be usable, it must be both active and available.
    """
    
    active: Annotated[bool, Field(
        description="Whether the compute site has been set to active by a service admin."
    )]
    available: Annotated[bool, Field(description="Whether the compute site is available.")]
    unavailable_reason: Annotated[str | None, Field(
        description="The reason the site is unavailable."
    )] = None


class Sites(BaseModel):
    """ The supported compute sites. """
    
    sites: Annotated[list[Site], Field(description="The list of compute sites.")]


@ROUTER_SITES.get(
    "/sites/",
    response_model=Sites,
    summary="Available compute sites",
    description="Information about the sites available for running jobs."
)
async def get_sites(r: Request) -> Sites:
    ret = []
    jfm = app_state.get_app_state(r).jobflow_manager
    active = await jfm.list_active_clusters()
    avail = await jfm.list_available_clusters()
    for cl, err in avail.items():
        ret.append(Site(
            active=cl in active,
            available=not err,
            unavailable_reason=err,
            **sites.CLUSTER_TO_SITE[cl].model_dump()
        ))
    return Sites(sites=ret)


class ListJobsResponse(BaseModel):
    """ The response to a successful job listing request. """
    jobs: Annotated[list[models.JobPreview], Field(description="The jobs")]


_ANN_JOB_SITE = Annotated[sites.Cluster | None, Query(
    description="Filter jobs by the site where the job ran."
)]
_ANN_JOB_STATE = Annotated[models.JobState | None, Query(
    description="Filter jobs by the state of the job."
)]
_ANN_JOB_AFTER = Annotated[AwareDatetime | None, Query(
    openapi_examples={"isodate": {"value": "2024-10-24T22:35:40Z"}},
    description="Filter jobs where the last update time is newer than the provided date, "
        + "inclusive",
)]
_ANN_JOB_BEFORE = Annotated[AwareDatetime | None, Query(
    openapi_examples={"isodate": {"value": "2024-10-24T22:35:59.999Z"}},
    description="Filter jobs where the last update time is older than the provided date, "
        + "exclusive",
)]
_ANN_JOB_LIMIT = Annotated[int | None, Query(
    openapi_examples={"max value": {"value": 1000}},
    description="The maximum number of jobs to return",
    ge=1,
    le=1000,
)]


@ROUTER_JOBS.get(
    "/",
    response_model=ListJobsResponse,
    response_model_exclude_none=True,
    summary="List jobs",
    description="List jobs for the current user."
)
async def list_jobs(
    r: Request,
    cluster: _ANN_JOB_SITE = None,
    state: _ANN_JOB_STATE = None,
    after: _ANN_JOB_AFTER = None,
    before: _ANN_JOB_BEFORE = None,
    limit: _ANN_JOB_LIMIT = 1000,
    user: CTSUser=Depends(_AUTH),
) -> ListJobsResponse:
    job_state = app_state.get_app_state(r).job_state
    return ListJobsResponse(jobs=await job_state.list_jobs(
        user=user.user,
        site=cluster,
        state=state,
        after=after,
        before=before,
        limit=limit,
    ))


class SubmitJobResponse(BaseModel):
    """ The response to a successful job submission request. """
    job_id: Annotated[str, Field(description="An opaque job ID.")]


@ROUTER_JOBS.post(
    "/",
    response_model=SubmitJobResponse,
    summary="Submit a job",
    description="Submit a job to the system.\n\n"
        + "Note that currently the kbase job flow does not support manifest files."
)
async def submit_job(
    r: Request,
    job_input: models.JobInput,
    user: CTSUser=Depends(_AUTH),
) -> SubmitJobResponse:
    job_state = app_state.get_app_state(r).job_state
    return SubmitJobResponse(job_id=await job_state.submit(job_input, user))


_ANN_JOB_ID = Annotated[str, FastPath(
    openapi_examples={"job id": {"value": "f0c24820-d792-4efa-a38b-2458ed8ec88f"}},
    description="The job ID.",
    pattern=r"^[\w-]+$",
    min_length=1,
    max_length=50,
)]


@ROUTER_JOBS.get(
    "/{job_id}/status",
    response_model=models.JobStatus,
    summary="Get a job's minimal status",
    description="Get minimal information about a job to determine the current status of the job. "
        + "Suitable for polling for job completion. Only the submitting user may view the job."
)
async def get_job_status(
    r: Request,
    job_id: _ANN_JOB_ID,
    user: CTSUser=Depends(_AUTH),
) -> models.JobStatus:
    job_state = app_state.get_app_state(r).job_state
    return await job_state.get_job_status(job_id, user)


@ROUTER_JOBS.get(
    "/{job_id}",
    response_model=models.Job,
    response_model_exclude_none=True,
    summary="Get a job",
    description="Get a job. Only the submitting user may view the job."
)
async def get_job(
    r: Request,
    job_id: _ANN_JOB_ID,
    user: CTSUser=Depends(_AUTH),
) -> models.Job:
    job_state = app_state.get_app_state(r).job_state
    return await job_state.get_job(job_id, user)


class ExitCodes(BaseModel):
    """ The response to a request for job container exit codes. """
    exit_codes: Annotated[list[int | None], Field(
        examples=[[0, 2, 0, None, 0, 255]],
        description="The exit codes for a job indexed by container number."
    )]


@ROUTER_JOBS.get(
    "/{job_id}/exit_codes",
    response_model=ExitCodes,
    summary="Get a job's exit codes",
    description="Get the container exit codes for a job. "
        + "Only the submitting user may view the exit codes."
)
async def get_job_exit_codes(
    r: Request,
    job_id: _ANN_JOB_ID,
    user: CTSUser=Depends(_AUTH),
) -> ExitCodes:
    jobstate = app_state.get_app_state(r).job_state
    return ExitCodes(exit_codes=await jobstate.get_job_exit_codes(job_id, user))


_ANN_CONTAINER_NUMBER = Annotated[int, FastPath(
    openapi_examples={"container_num": {"value": 2}},
    description="The container / subjob number.",
    ge=0,
)]


@ROUTER_JOBS.get(
    "/{job_id}/log/{container_num}/stdout",
    response_class=StreamingResponse,
    summary="Get a job's stdout logs",
    description="Get the stdout stream from a job container. "
        + "Only the submitting user may view the logs."
)
async def get_job_stdout(
    r: Request,
    job_id: _ANN_JOB_ID,
    container_num: _ANN_CONTAINER_NUMBER,
    user: CTSUser=Depends(_AUTH),
) -> StreamingResponse:
    return await get_logs(r, job_id, container_num, user)


@ROUTER_JOBS.get(
    "/{job_id}/log/{container_num}/stderr",
    response_class=StreamingResponse,
    summary="Get a job's stderr logs",
    description="Get the stderr stream from a job container. "
        + "Only the submitting user may view the logs."
)
async def get_job_stderr(
    r: Request,
    job_id: _ANN_JOB_ID,
    container_num: _ANN_CONTAINER_NUMBER,
    user: CTSUser=Depends(_AUTH),
) -> StreamingResponse:
    return await get_logs(r, job_id, container_num, user, stderr=True)


async def get_logs(
    r: Request,
    job_id: str,
    container_num: int,
    user: CTSUser,
    stderr: bool = False,
) -> StreamingResponse:
    jobstate = app_state.get_app_state(r).job_state
    filegen, filename = await jobstate.stream_job_logs(job_id, container_num, user, stderr=stderr)
    return StreamingResponse(
        filegen,
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


_ANN_IMAGE_ID = Annotated[str, FastPath(
    openapi_examples={"image with digest": {
        "value": "ghcr.io/kbase/collections:checkm2_0.1.6"
            + "@sha256:c9291c94c382b88975184203100d119cba865c1be91b1c5891749ee02193d380",
    }},
    description="The name of a docker image. Include the SHA to ensure "
        + "referencing the correct image.",
    # Don't bother validating other than some basic checks, validation will occur when
    # checking / getting the image SHA from the remote repository
    min_length=1,
    max_length=1000,
)]


class Images(BaseModel):
    """ The response to a request to get images in the system. """
    data: Annotated[list[models.Image], Field(description="The images.")]
    # if we add paging / sorting / filtering, could add more info here


@ROUTER_IMAGES.get(
    "/",
    response_model=Images,
    response_model_exclude_none=True,
    summary="Get images",
    description="Get images registered in the service. At most 1000 are returned "
        + "in no particular order.",
)
async def get_images(r: Request):
    images = await app_state.get_app_state(r).images.get_images()
    return Images(data=images)


@ROUTER_IMAGES.get(
    "/{image_id:path}",
    response_model=models.Image,
    response_model_exclude_none=True,
    summary="Get an image by name",
    description="Get an image previously registered to the system by the image name. "
        + "If a digest and tag are provided the tag is ignored.",
)
async def get_image(r: Request, image_id: _ANN_IMAGE_ID):
    return await app_state.get_app_state(r).images.get_image(image_id)


_ANN_REFDATA_ID = Annotated[str, FastPath(
    openapi_examples={"refdata id": {"value": "f0c24820-d792-4efa-a38b-2458ed8ec88f"}},
    description="The reference data ID.",
    pattern=r"^[\w-]+$",
    min_length=1,
    max_length=50,
)]


class RefData(BaseModel):
    """ The response to a request to get reference data in the system. """
    data: Annotated[list[models.ReferenceData], Field(description="The reference data.")]
    # if we add paging / sorting / filtering, could add more info here


@ROUTER_REFDATA.get(
    "/",
    response_model=RefData,
    response_model_exclude_none=True,
    summary="Get reference data information",
    description="Get information about reference data available for containers in the system "
        + "in no particular order. Returns at most 1000 records."
)
async def get_refdata(r: Request) -> RefData:
    refdata = app_state.get_app_state(r).refdata
    rd = await refdata.get_refdata()
    return RefData(data=rd)


@ROUTER_REFDATA.get(
    "/id/{refdata_id}",
    response_model=models.ReferenceData,
    response_model_exclude_none=True,
    summary="Get reference data information by ID",
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
    response_model=RefData,
    response_model_exclude_none=True,
    summary="Get reference data information by the S3 file path",
    description="Get information about reference data available for containers by the "
        + "reference data file path. Returns at most 1000 records."
)
async def get_refdata_by_path(
    r: Request,
    s3_path: Annotated[str, FastPath(
        openapi_examples={"refdata path": {
            "value": "refdata-bucket/checkm2/checkm2_refdata-2.4.tgz"
        }},
        description="The S3 path to the reference data, starting with the bucket. "
            + "Please note that spaces are valid S3 object characters, so be careful about "
            + "trailing spaces in the input.",
        min_length=models.S3_PATH_MIN_LENGTH,
        max_length=models.S3_PATH_MAX_LENGTH,
    )],
) -> RefData:
    refdata = app_state.get_app_state(r).refdata
    rd = await refdata.get_refdata_by_path(s3_path)
    return RefData(data=rd)


# TODO CHECKSUM FEATURE add get refdata by crc64nvme if needed... YAGNI

@ROUTER_ADMIN.post(
    "/images/{image_id:path}",
    response_model=models.Image,
    response_model_exclude_none=True,
    summary="Approve an image",
    description="Approve a Docker image for use with this service. "
        + "The image must be publicly accessible and have an entrypoint. "
        + "The image may not already exist in the system."
)
async def approve_image(
    r: Request,
    image_id: _ANN_IMAGE_ID,
    image_usage: models.ImageUsage = None,
    refdata_id: Annotated[str, Query(
        openapi_examples={"refdata id": {"value": "3a28c155-ea8b-4e1b-baef-242d991a8200"}},
        description="The ID of reference data to associate with the image. The reference data "
            + "must already be registered with the system, although it may still be staging "
            + "at remote compute sites.",
        pattern=r"^[\w-]+$",
        min_length=1,
        max_length=50,
    )] = None,
    default_refdata_mount_point: Annotated[str, Query(
        openapi_examples={"mount_point": {"value": "/reference_data"}},
        description="The default mount point for the image refdata in the container. Overridden "
            + "by the refdata mount point in a job submission if provided there. If a default "
            + "mount point is provided, then providing a mount point in the job submission "
            + "is optional. Must be an absolute path.",
        pattern=models.ABSOLUTE_PATH_REGEX,
        min_length=1,
        max_length=1024,
    )] = None,
    user: CTSUser=Depends(_AUTH)
) -> models.Image:
    _ensure_admin(user, "Only service administrators can approve images.")
    images = app_state.get_app_state(r).images
    return await images.register(
        image_id,
        user.user,
        image_usage=image_usage,
        refdata_id=refdata_id,
        default_refdata_mount_point=default_refdata_mount_point
    )


@ROUTER_ADMIN.delete(
    "/images/{image_id:path}",
    summary="Delete an image",
    description="Delete a Docker image, making it no longer usable with this service. "
        + "If a digest and tag are provided the tag is ignored.",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def delete_image(
    r: Request,
    image_id: _ANN_IMAGE_ID,
    user: CTSUser=Depends(_AUTH)
):
    _ensure_admin(user, "Only service administrators can delete images.")
    images = app_state.get_app_state(r).images
    await images.delete_image(image_id)


@ROUTER_ADMIN.post(
    "/refdata/{refdata_s3_path:path}",
    response_model=models.ReferenceData,
    response_model_exclude_none=True,
    summary="Create reference data",
    description="Define an S3 file as containing reference data necessary for one or more "
        + "images and start the reference data staging process."
)
async def create_refdata(
    r: Request,
    # will be validated later 
    refdata_s3_path: Annotated[str, FastPath(
        openapi_examples={"refdata path": {
            "value": "refdata-bucket/checkm2/checkm2_refdata-2.4.tgz"}
        },
        description="The S3 path to the reference data to register, starting with the bucket. "
            + "If the refdata consists of multiple files, they must be archived. "
            + "Please note that spaces are valid S3 object characters, so be careful about "
            + "trailing spaces in the input.",
        min_length=models.S3_PATH_MIN_LENGTH,
        max_length=models.S3_PATH_MAX_LENGTH,
    )],
    crc64nvme: Annotated[str, Query(
        openapi_examples={"checksum": {"value": "4ekt2WB1KO4="}},
        description="The base64 encoded CRC64/NVME checksum of the file. "
            + "If provided it is checked against the target file checksum before proceeding. "
            + "Note you may need to URL encode the checksum if your HTTP client or application "
            + "doesn't do it for you.",
        min_length=models.CRC64NVME_B64ENC_LENGTH,
        max_length=models.CRC64NVME_B64ENC_LENGTH,
    )] = None,
    unpack: Annotated[bool, Query(
        description="Whether to unpack the file after download. *.tar.gz, *.tgz, and *.gz "
            + "files are supported."
    )] = False,
    user: CTSUser=Depends(_AUTH)
) -> models.ReferenceData:
    _ensure_admin(user, "Only service administrators can create reference data.")
    refdata = app_state.get_app_state(r).refdata
    return await refdata.create_refdata(refdata_s3_path, user, crc64nvme=crc64nvme, unpack=unpack)


@ROUTER_ADMIN.get(
    "/jobs",
    response_model=ListJobsResponse,
    response_model_exclude_none=True,
    summary="List jobs as an admin",
    description="List jobs for a provided user or all users."
)
async def list_jobs_admin(
    r: Request,
    user: Annotated[str, Query(
        openapi_examples={"kbasehelp user": {"value": "kbasehelp"}},
        description="Filter jobs by the owner of the job.",
        min_length=1,
        max_length=100,
        pattern=r"^[a-z][a-z\d_]*$",
    )] = None,
    cluster: _ANN_JOB_SITE = None,
    state: _ANN_JOB_STATE = None,
    after: _ANN_JOB_AFTER = None,
    before: _ANN_JOB_BEFORE = None,
    limit: _ANN_JOB_LIMIT = 1000,
    methoduser: CTSUser=Depends(_AUTH),
) -> ListJobsResponse:
    _ensure_admin(methoduser, "Only service administrators can list other users' jobs.")
    auth = app_state.get_app_state(r).auth
    if user and not await auth.is_valid_kbase_user(user, app_state.get_request_token(r)):
        raise InvalidUserError(f"No such user: {user}")
    job_state = app_state.get_app_state(r).job_state
    return ListJobsResponse(jobs=await job_state.list_jobs(
        user=user,
        site=cluster,
        state=state,
        after=after,
        before=before,
        limit=limit,
    ))

@ROUTER_ADMIN.get(
    "/jobs/{job_id}",
    response_model=models.AdminJobDetails,
    response_model_exclude_none=True,
    summary="Get a job as an admin",
    description="Get any job, regardless of ownership, with additional details about the job run."
)
async def get_job_admin(
    r: Request,
    job_id: _ANN_JOB_ID,
    user: CTSUser=Depends(_AUTH),
) -> models.AdminJobDetails:
    _ensure_admin_or_executor(
        user,
        "Only service administrators and external job executors can get jobs as an admin."
    )
    job_state = app_state.get_app_state(r).job_state
    return await job_state.get_job(job_id, user, as_admin=True)


@ROUTER_ADMIN.get(
    "/jobs/{job_id}/exit_codes",
    response_model=ExitCodes,
    summary="Get any job's exit codes",
    description="Get the container exit codes for any job, regardless of ownership."
)
async def get_job_exit_codes_admin(
    r: Request,
    job_id: _ANN_JOB_ID,
    user: CTSUser=Depends(_AUTH),
) -> ExitCodes:
    _ensure_admin(user, "Only service administrators can view other users' job details.")
    jobstate = app_state.get_app_state(r).job_state
    return ExitCodes(exit_codes=await jobstate.get_job_exit_codes(job_id, user, as_admin=True))


class SubJobs(BaseModel):
    """ The subjobs / containers for a job. """
    
    subjobs: Annotated[list[models.SubJob], Field(description="The list of subjobs.")]


@ROUTER_ADMIN.get(
    "/jobs/{job_id}/container/",
    response_model=SubJobs,
    response_model_exclude_none=True,
    summary="Get state for all containers",
    description="Get the state for all of a job's containers. "
        + "Only applicable for sites where container state is managed by the service"
)
async def get_job_containers(
    r: Request,
    job_id: _ANN_JOB_ID,
    user: CTSUser=Depends(_AUTH),
) -> SubJobs:
    sjs = await _get_containers(r, job_id, user)
    return SubJobs(subjobs=sjs)


@ROUTER_ADMIN.get(
    "/jobs/{job_id}/container/{container_num}",
    response_model=models.SubJob,
    response_model_exclude_none=True,
    summary="Get state for a container",
    description="Get the state for a particular subjob / container. "
        + "Only applicable for sites where container state is managed by the service"
)
async def get_job_container(
    r: Request,
    job_id: _ANN_JOB_ID,
    container_num: _ANN_CONTAINER_NUMBER,
    user: CTSUser=Depends(_AUTH),
) -> models.SubJob:
    return await _get_containers(r, job_id, user, container_num=container_num)


async def _get_containers(
    r: Request,
    job_id: str,
    user: CTSUser,
    container_num: int | None = None,
) -> models.SubJob | list[models.SubJob]:
    # TODO CODE could use same strategy as exit codes to allow getting state w/o flow availble.
    #           Since admin only don't worry about it for now
    _ensure_admin(user, "Only service administrators get container state.")
    appstate = app_state.get_app_state(r)
    job = await appstate.job_state.get_job(job_id, user, as_admin=True)
    flow = await appstate.jobflow_manager.get_flow(job.job_input.cluster)
    return await flow.get_subjobs(job.id, container_num=container_num)


@ROUTER_ADMIN.get(
    "/jobs/{job_id}/runner_status",
    response_model=dict[str, Any],
    summary="Get a job's external status",
    description="Get the status of a job in an external job runner such as JAWS. "
        + "This endpoint should be used for informational purposes only and the data structure "
        + "may change at any time - changes are not treated as backwards incompatibilities. "
        + "If the job has not yet been submitted to an external runner, "
        + "an empty dictionary is returned."
)
async def get_job_runner_status(
    r: Request,
    job_id: _ANN_JOB_ID,
    container_number: Annotated[int, Query(
        openapi_examples={"container_number": {"value": 12}},
        description="The number of the container to query. If the containers are tracked "
            + "by the external runner rather than this service, this parameter is ignored.",
        ge=0
    )] = 0,
    user: CTSUser=Depends(_AUTH),
) -> dict[str, Any]:
    _ensure_admin(user, "Only service administrators can get job runner status.")
    appstate = app_state.get_app_state(r)
    job = await appstate.job_state.get_job(job_id, user, as_admin=True)
    flow = await appstate.jobflow_manager.get_flow(job.job_input.cluster)
    return await flow.get_job_external_runner_status(job, container_number=container_number)


class UpdateAdminMeta(BaseModel):
    """ A specification for how to alter a job's administrative metadata. """
    
    model_config = ConfigDict(extra='forbid')
    
    set_fields: Annotated[dict[str, str | int | float], Field(
        default_factory=dict,
        examples=[{"foo": "bar", "count": 1}],
        description="Keys and their values to set in the administrative metadata for the job."
            + "Any matching keys will be overwritten."
    )]
    unset_keys: Annotated[set[str], Field(
        default_factory=set,
        examples=[{"foobar", "othercount"}],
        description="Keys to remove from the administrative metadata for the job."
    )]


@ROUTER_ADMIN.put(
    "/jobs/{job_id}/meta",
    summary="Update a job's admin metadata",
    description="Update the job's administrative metadata. Any extant metadata keys not included "
        + "in the request are unaffected. The job's owner can view the metadata.",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def update_job_admin_meta(
    r: Request,
    update: UpdateAdminMeta,
    job_id: _ANN_JOB_ID,
    admin: CTSUser=Depends(_AUTH),
):
    _ensure_admin(admin, "Only service administrators can alter admin metadata.")
    job_state = app_state.get_app_state(r).job_state
    await job_state.update_job_admin_meta(
        job_id, admin, set_fields=update.set_fields, unset_keys=update.unset_keys
    )


@ROUTER_ADMIN.delete(
    "/jobs/{job_id}/clean",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Clean up after a job",
    description="Remove any job related files managed by this service at the remote compute site. "
        + "The job must be in a terminal state."
)
async def clean_job(
    r: Request,
    job_id: _ANN_JOB_ID,
    force: Annotated[bool, Query(
        description="**WARNING**: setting force to true may cause undefined behavior. True will " 
        + "cause job files to be removed regardless of job state."
    )] = False,
    user: CTSUser=Depends(_AUTH),
):
    _ensure_admin(user, "Only service administrators can clean jobs.")
    appstate = app_state.get_app_state(r)
    job = await appstate.job_state.get_job(job_id, user, as_admin=True)
    flow = await appstate.jobflow_manager.get_flow(job.job_input.cluster)
    await flow.clean_job(job, force=force)


@ROUTER_ADMIN.get(
    "/refdata/{refdata_id}",
    response_model=models.AdminReferenceData,
    response_model_exclude_none=True,
    summary="Get reference data information as an admin",
    description="Get information about reference data available for containers, including its "
        + "location and staging status, with additional details."
)
async def get_refdata_admin(
    r: Request,
    refdata_id: _ANN_REFDATA_ID,
    user: CTSUser=Depends(_AUTH),
) -> models.AdminReferenceData:
    _ensure_admin(user, "Only service administrators can get refdata as an admin.")
    refdata = app_state.get_app_state(r).refdata
    return await refdata.get_refdata_by_id(refdata_id, as_admin=True)


@ROUTER_ADMIN.delete(
    "/refdata/{refdata_id}/{cluster}/clean",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Clean up refdata staging files",
    description="Remove refdata staging related files managed by this service at the remote "
        + "compute site. The refdata staging must be in a terminal state.\n\n"
        + "The refdata itself and NERSC -> LRC transfer triggering files are not removed."
)
async def clean_refdata(
    r: Request,
    refdata_id: _ANN_REFDATA_ID,
    cluster: Annotated[sites.Cluster, FastPath(
        description="The cluster where refdata staging files should be cleaned up."
    )],
    force: Annotated[bool, Query(
        description="**WARNING**: setting force to true may cause undefined behavior. True will " 
        + "cause refdata files to be removed regardless of staging state."
    )] = False,
    user: CTSUser=Depends(_AUTH),
):
    _ensure_admin(user, "Only service administrators can clean jobs.")
    appstate = app_state.get_app_state(r)
    refdata = await appstate.refdata.get_refdata_by_id(refdata_id, as_admin=True)
    flow = await appstate.jobflow_manager.get_flow(cluster)
    await flow.clean_refdata(refdata, force=force)


@ROUTER_ADMIN.put(
    "/sites/{site}/active",
    summary="Activate a site",
    description="Set a site to active, allowing submission of jobs and other activities "
        + "that require accessing external site resources.",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def set_site_active(
    r: Request,
    site: Annotated[sites.Cluster, FastPath(description="The site to modify")],
    user: CTSUser=Depends(_AUTH)
):
    _ensure_admin(user, "Only service administrators may alter sites.")
    flowman = app_state.get_app_state(r).jobflow_manager
    await flowman.set_site_active(site)


@ROUTER_ADMIN.delete(
    "/sites/{site}/active",
    summary="Deactivate a site",
    description="Set a site to inactive, disallowing submission of jobs and other activities "
        + "that require accessing external site resources.",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def set_site_inactive(
    r: Request,
    site: Annotated[sites.Cluster, FastPath(description="The site to modify")],
    user: CTSUser=Depends(_AUTH)
):
    _ensure_admin(user, "Only service administrators may alter sites.")
    flowman = app_state.get_app_state(r).jobflow_manager
    await flowman.set_site_inactive(site)


class NERSCClientInfo(BaseModel):
    """ Provides information about the NERSC SFAPI client. """
    id: Annotated[str, Field(description="The opaque ID of the client.")]
    expires_at: Annotated[datetime.datetime, Field(
        examples=["2024-10-24T22:35:40Z"],
        description="The time the client expires in ISO8601 format."
    )]
    expires_in: Annotated[datetime.timedelta, Field(
        examples=["PT12H30M5S"],
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
        openapi_examples={"iso8601 duration": {"value": "P30DT12H30M5S"}},
        description="The required remaining lifetime of the client as an "
            + "ISO8601 duration string. If the lifetime is shorter than this value, an error "
            + "will be returned.",
        ge=datetime.timedelta(seconds=1)
    )] = None,
    user: CTSUser=Depends(_AUTH)
) -> NERSCClientInfo:
    _ensure_admin(user, "Only service administrators may view NERSC client information.")
    nersc_cli = app_state.get_app_state(r).sfapi_client_provider()
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


class UnsentNotifications(BaseModel):
    """ Provides information about unsent job state transition notifications. """
    jobs: Annotated[int, Field(
        description="The number of jobs with unsent job state transition notifications."
    )]
    transitions: Annotated[int, Field(
        description="The number of unsent job state transition notifications."
    )]


@ROUTER_ADMIN.post(
    "/unsent_notifications",
    response_model=UnsentNotifications,
    summary="Send unsent job state transition notifications",
    description="""
**WARNING**: Read the documentation here fully before using this endpoint.

Check for unsent Kafka job state transition notifications older than the specified
time and send them.

Note that it is possible to cause messages to be sent multiple times by calling this
endpoint.

For instance, if Kafka is down and at least one CTS server is up and processing jobs, the
job updates will be cached by the CTS's Kafka client. Calling this endpoint will cache
all those updates as well. If Kafka comes back up, all the updates, the originals
and the updates generated from this endpoint, will be sent to the server.

It it safe to call this endpoint when any running CTS servers and Kafka have been up
long enough such that there is a minimal update queue in the CTS servers; e.g.
the CTS servers should send messages before the time delay specified in the
arguments expires.
  
It is not safe to run this code when updates are not sent to Kafka within the time delay;
in that case running this code may cause duplicate messages.

Returns the number of jobs found that need notifications sent and the number of
notifications to send, which may be > 1 per job. The endpoint may return before
sending of notifications completes.
""",
)
async def handle_unsent_notifications(
    r: Request,
    notification_age: Annotated[datetime.timedelta, Query(
        openapi_examples={"iso8601 duration": {"value": "PT10M"}},
        description="The amount of time the transition must have occurred in the past "
            + "to trigger a send as an ISO8601 duration string. "
            + "Transitions newer than this will be ignored.",
        ge=datetime.timedelta(minutes=1)
    )] = "PT10M",
    user: CTSUser=Depends(_AUTH),
):
    _ensure_admin(user, "Only service administrators may send notifications.")
    kc = app_state.get_app_state(r).kafka_checker
    older_than = utcdatetime() - notification_age
    logging.getLogger(__name__).info(
        f"Checking for unsent kafka messages older than {older_than.isoformat()}"
    )
    jobs, notifs = await kc.check(older_than)
    return UnsentNotifications(jobs=jobs, transitions=notifs)


@ROUTER_CALLBACKS.get(
    f"/{get_download_complete_callback()}/{{job_id}}",
    summary="Report data download complete",
    description="Report that data download for a job is complete. This method is not expected "
        + "to be called by users.",
    status_code=status.HTTP_204_NO_CONTENT,
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
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def refdata_download_complete(
    r: Request,
    refdata_id: _ANN_REFDATA_ID,
    cluster: Annotated[sites.Cluster, FastPath(
        description="The cluster for which the refdata download is complete."
    )]
):
    logging.getLogger(__name__).info(
        f"Download reported as complete for refdata {refdata_id} on cluster {cluster.value}",
        extra={logfields.REFDATA_ID: refdata_id, logfields.CLUSTER: cluster.value},
    )
    runner = await app_state.get_app_state(r).jobflow_manager.get_flow(cluster)
    await runner.refdata_complete(refdata_id)


@ROUTER_CALLBACKS.get(
    f"/{get_job_complete_callback()}/{{job_id}}",
    summary="Report job complete",
    description="Report a remote job is complete. This method is not expected "
        + "to be called by users.",
    status_code=status.HTTP_204_NO_CONTENT,
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
    status_code=status.HTTP_204_NO_CONTENT,
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
    status_code=status.HTTP_204_NO_CONTENT,
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
    job = await appstate.job_state.get_job(job_id, SERVICE_USER, as_admin=True)
    return await appstate.jobflow_manager.get_flow(job.job_input.cluster), job


@ROUTER_EXTERNAL_EXEC.put(
    "/external_exec/jobs/{job_id}/container/{container_num}/update/{new_state}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Update a container's state.",
    description="Not for general use.\n\nUpdate a container / subjob's state.",
)
async def update_container(
    r: Request,
    job_id: _ANN_JOB_ID,
    container_num: _ANN_CONTAINER_NUMBER,
    new_state: Annotated[models.JobState, FastPath(
        openapi_examples={"job_submitted": {"value": models.JobState.JOB_SUBMITTED}},
        description="The new state for the container / subjob.",
    )],
    update: models.ContainerUpdate,
    user: CTSUser=Depends(_AUTH),
):
    _ensure_executor(user, "Only external job executors can update container state.")
    appstate = app_state.get_app_state(r)
    job = await appstate.job_state.get_job(job_id, user, as_admin=True)
    flow =  await appstate.jobflow_manager.get_flow(job.job_input.cluster)
    await flow.update_container_state(job, container_num, new_state, update)


@ROUTER_EXTERNAL_EXEC.put(
    "/external_exec/refdata/{refdata_id}/{cluster}/update/{new_state}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Update refdata state.",
    description="Not for general use.\n\nUpdate refdata state for a cluster.",
)
async def update_refdata(
    r: Request,
    refdata_id: _ANN_REFDATA_ID,
    cluster: Annotated[sites.Cluster, FastPath(
        description="The cluster for which the refdata should be updated."
    )],
    new_state: Annotated[models.ReferenceDataState, FastPath(
        openapi_examples={"download_submitted": {
            "value": models.ReferenceDataState.DOWNLOAD_SUBMITTED
        }},
        description="The new state for the refdata.",
    )],
    update: models.RefdataUpdate | None = None,
    user: CTSUser=Depends(_AUTH),
):
    _ensure_refdata_service(user, "Only refdata services can update refdata state.")
    appstate = app_state.get_app_state(r)
    flow =  await appstate.jobflow_manager.get_flow(cluster)
    await flow.update_refdata_state(refdata_id, new_state, update)


@ROUTER_EXTERNAL_EXEC.get(
    f"/{localfiles.CONDOR_EXE_PATH}",
    summary="Get the condor executable script",
    description="Not for general use.\n\nGets the executable script to run on workers "
        + "for running HTCondor jobs.",
    response_class=FileResponse,
)
async def get_condor_executable(r: Request) -> FileResponse:
    file = app_state.get_app_state(r).condor_exe_path
    return FileResponse(file, filename=Path(localfiles.CONDOR_EXE_PATH).name)


@ROUTER_EXTERNAL_EXEC.get(
    f"/{localfiles.CODE_ARCHIVE_PATH}",
    summary="Get the code archive.",
    description="Not for general use.\n\nGets the code archive file for external job execution.",
    response_class=FileResponse,
)
async def get_code_archive(r: Request) -> FileResponse:
    file = app_state.get_app_state(r).code_archive_path
    return FileResponse(file, filename=Path(localfiles.CODE_ARCHIVE_PATH).name)


class ClientLifeTimeError(Exception):
    """ An error thrown when a client's lifetime is less than required. """
