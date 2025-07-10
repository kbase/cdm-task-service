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
from pydantic import BaseModel, Field, AwareDatetime, ConfigDict
from typing import Annotated, Any

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

SERVICE_NAME = "CDM Task Service"
NOTES = "This service is a prototype"

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
        str, Field(examples=[SERVICE_NAME], description="The name of the service."
    )]
    version: Annotated[
        str, Field(examples=[VERSION], description="The semantic version of the service."
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
async def root() -> Root:
    return {
        "service_name": SERVICE_NAME,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": utcdatetime(),
        "notes": NOTES,
    }


class WhoAmI(BaseModel):
    """ The username associated with the provided user token and the user's admin state. """
    user: Annotated[str, Field(examples=["kbasehelp"], description="The user's username.")]
    is_service_admin: Annotated[bool, Field(
        examples=[False], description="Whether the user is a service administrator."
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


class ListJobsResponse(BaseModel):
    """ The response to a successful job listing request. """
    jobs: Annotated[list[models.JobPreview], Field(description="The jobs")]


_ANN_JOB_STATE = Annotated[models.JobState, Query(
    description="Filter jobs by the state of the job."
)]
_ANN_JOB_AFTER = Annotated[AwareDatetime, Query(
    openapi_examples={"isodate": {"value": "2024-10-24T22:35:40Z"}},
    description="Filter jobs where the last update time is newer than the provided date, "
        + "inclusive",
)]
_ANN_JOB_BEFORE = Annotated[AwareDatetime, Query(
    openapi_examples={"isodate": {"value": "2024-10-24T22:35:59.999Z"}},
    description="Filter jobs where the last update time is older than the provided date, "
        + "exclusive",
)]
_ANN_JOB_LIMIT = Annotated[int, Query(
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
    state: _ANN_JOB_STATE = None,
    after: _ANN_JOB_AFTER = None,
    before: _ANN_JOB_BEFORE = None,
    limit: _ANN_JOB_LIMIT = 1000,
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> ListJobsResponse:
    job_state = app_state.get_app_state(r).job_state
    return ListJobsResponse(jobs=await job_state.list_jobs(
        user=user.user,
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
    user: kb_auth.KBaseUser=Depends(_AUTH),
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
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> models.Job:
    job_state = app_state.get_app_state(r).job_state
    return await job_state.get_job(job_id, user.user)


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
    summary="Get reference data information by the S3 file path",
    description="Get information about reference data available for containers by the "
        + "reference data file path. Returns at most 1000 records."
)
async def get_refdata_by_path(
    r: Request,
    s3_path: Annotated[str, FastPath(
        openapi_examples={"refdata path": {"value": "refdata-bucket/checkm2/checkm2_refdata-2.4.tgz"}},
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
    summary="Approve an image",
    description="Approve a Docker image for use with this service. "
        + "The image must be publicly accessible and have an entrypoint. "
        + "The image may not already exist in the system."
)
async def approve_image(
    r: Request,
    image_id: _ANN_IMAGE_ID,
    refdata_id: Annotated[str, Query(
        openapi_examples={"refdata id": {"value": "3a28c155-ea8b-4e1b-baef-242d991a8200"}},
        description="The ID of reference data to associate with the image. The reference data "
            + "must already be registered with the system, although it may still be staging "
            + "at remote compute sites.",
        pattern=r"^[\w-]+$",
        min_length=1,
        max_length=50,
    )] = None,
    user: kb_auth.KBaseUser=Depends(_AUTH)
) -> models.Image:
    _ensure_admin(user, "Only service administrators can approve images.")
    images = app_state.get_app_state(r).images
    return await images.register(image_id, user.user, refdata_id=refdata_id)


@ROUTER_ADMIN.delete(
    "/images/{image_id:path}",
    summary="Delete an image",
    description="Delete a Docker image, making it no longer usable with this service. "
        + "If a digest and tag are provided the tag is ignored.",
    status_code = status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
async def delete_image(
    r: Request,
    image_id: _ANN_IMAGE_ID,
    user: kb_auth.KBaseUser=Depends(_AUTH)
):
    _ensure_admin(user, "Only service administrators can delete images.")
    images = app_state.get_app_state(r).images
    await images.delete_image(image_id)


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
        openapi_examples={"refdata path": {"value": "refdata-bucket/checkm2/checkm2_refdata-2.4.tgz"}},
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
    user: kb_auth.KBaseUser=Depends(_AUTH)
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
    state: _ANN_JOB_STATE = None,
    after: _ANN_JOB_AFTER = None,
    before: _ANN_JOB_BEFORE = None,
    limit: _ANN_JOB_LIMIT = 1000,
    methoduser: kb_auth.KBaseUser=Depends(_AUTH),
) -> ListJobsResponse:
    _ensure_admin(methoduser, "Only service administrators can list other users' jobs.")
    kbauth = app_state.get_app_state(r).auth
    if user and not await kbauth.is_valid_user(user, app_state.get_request_token(r)):
        raise kb_auth.InvalidUserError(f"No such user: {user}")
    job_state = app_state.get_app_state(r).job_state
    return ListJobsResponse(jobs=await job_state.list_jobs(
        user=user,
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
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> models.AdminJobDetails:
    _ensure_admin(user, "Only service administrators can get jobs as an admin.")
    job_state = app_state.get_app_state(r).job_state
    return await job_state.get_job(job_id, user.user, as_admin=True)


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
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> models.AdminJobDetails:
    _ensure_admin(user, "Only service administrators can get job runner status an admin.")
    appstate = app_state.get_app_state(r)
    job = await appstate.job_state.get_job(job_id, user.user, as_admin=True)
    flow = appstate.jobflow_manager.get_flow(job.job_input.cluster)
    return await flow.get_job_external_runner_status(job)


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
    status_code = status.HTTP_204_NO_CONTENT,
)
async def update_job_admin_meta(
    r: Request,
    update: UpdateAdminMeta,
    job_id: _ANN_JOB_ID,
    admin: kb_auth.KBaseUser=Depends(_AUTH),
):
    _ensure_admin(admin, "Only service administrators can alter admin metadata.")
    job_state = app_state.get_app_state(r).job_state
    await job_state.update_job_admin_meta(
        job_id, admin, set_fields=update.set_fields, unset_keys=update.unset_keys
    )


@ROUTER_ADMIN.get(
    "/refdata/{refdata_id}",
    response_model=models.AdminReferenceData,
    summary="Get reference data information as an admin",
    description="Get information about reference data available for containers, including its "
        + "location and staging status, with additional details."
)
async def get_refdata_admin(
    r: Request,
    refdata_id: _ANN_REFDATA_ID,
    user: kb_auth.KBaseUser=Depends(_AUTH),
) -> models.AdminReferenceData:
    _ensure_admin(user, "Only service administrators can get refdata as an admin.")
    refdata = app_state.get_app_state(r).refdata
    return await refdata.get_refdata_by_id(refdata_id, as_admin=True)


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
    user: kb_auth.KBaseUser=Depends(_AUTH),
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
