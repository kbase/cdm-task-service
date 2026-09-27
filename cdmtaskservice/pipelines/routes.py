"""
CDM pipeline job endpoints.
"""

from fastapi import APIRouter, Depends, Query, Request, Response, status
from fastapi import Path as FastPath
from pydantic import BaseModel, Field
from typing import Annotated, Any

from cdmtaskservice import app_state, models, sites
from cdmtaskservice.http_bearer import KBaseHTTPBearer
from cdmtaskservice.jobflows.flowmanager import JobFlow
from cdmtaskservice.pipelines import models as pipe_models
from cdmtaskservice.pipelines.definition import PipelineDefinition
from cdmtaskservice.routes_shared import (
    ensure_admin as _ensure_admin,
    ensure_admin_or_executor as _ensure_admin_or_executor,
    ANN_JOB_SITE as _ANN_JOB_SITE,
    ANN_JOB_STATE as _ANN_JOB_STATE,
    ANN_JOB_AFTER as _ANN_JOB_AFTER,
    ANN_JOB_BEFORE as _ANN_JOB_BEFORE,
    ANN_JOB_LIMIT as _ANN_JOB_LIMIT,
    ANN_JOB_ADMIN_USER as _ANN_JOB_ADMIN_USER,
    ensure_valid_kbase_user as _ensure_valid_kbase_user,
    get_job_and_flow as _get_job_and_flow_generic,
    admin_get_job_and_flow as _admin_get_job_and_flow_generic,
    JobIDType as _JobIDType,
)
from cdmtaskservice.user import CTSUser

ROUTER_PIPELINES = APIRouter(tags=["Pipelines - Experimental"], prefix="/pipelines")

ROUTER_ADMIN_PIPELINES = APIRouter(
    tags=["Admin Pipelines - Experimental"], prefix="/admin/pipelines"
)

_AUTH = KBaseHTTPBearer()


async def _get_job_and_flow(
    r: Request, job_id: str, user: CTSUser
) -> tuple[pipe_models.AdminPipelineJob, JobFlow]:
    """ Fetch a pipeline job and its associated job flow. """
    return await _get_job_and_flow_generic(r, job_id, user, job_id_type=_JobIDType.PIPELINE)


async def _admin_get_job_and_flow(
    r: Request, job_id: str, user: CTSUser, action: str
) -> tuple[pipe_models.AdminPipelineJob, JobFlow]:
    """
    Verify the user is a full admin, then fetch a pipeline job and its associated job flow.
    """
    return await _admin_get_job_and_flow_generic(
        r, job_id, user, action, job_id_type=_JobIDType.PIPELINE
    )


class PipelineDefinitionInfo(BaseModel):
    """ Information about a registered pipeline version. """
    name: Annotated[str, Field(
        examples=["readsqc"],
        description="The pipeline's name.",
    )]
    version: Annotated[pipe_models.SemverVersion, Field(
        examples=["0.1.0"],
        description="The pipeline version.",
    )]
    description: Annotated[str, Field(
        description="A human readable description of the pipeline.",
    )]
    doc_urls: Annotated[list[str], Field(
        description="URLs to documentation for this pipeline version, if any.",
    )]
    # TODO FEATURE add a toggle to return standard json schema
    input_schema: Annotated[dict[str, Any], Field(
        description="The shape of this pipeline version's input parameters, excluding file "
            + "references. The input must match the shape described by this schema.",
    )]
    files_schema: Annotated[dict[str, Any], Field(
        description="The shape of this pipeline version's file references. The file input "
            + "must match the shape described by this schema.",
    )]


def _to_definition_info(pd: PipelineDefinition) -> PipelineDefinitionInfo:
    return PipelineDefinitionInfo(
        name=pd.name,
        version=pd.version,
        description=pd.description,
        doc_urls=pd.doc_urls,
        input_schema=pd.input_schema,
        files_schema=pd.files_schema,
    )


class PipelineDefinitions(BaseModel):
    """ The response to a request to list available pipeline definitions. """
    data: Annotated[list[PipelineDefinitionInfo], Field(
        description="The requested pipeline definitions."
    )]


@ROUTER_PIPELINES.get(
    "/available",
    response_model=PipelineDefinitions,
    summary="List pipeline definitions",
    description="List the latest registered version of every available pipeline, "
        + "along with relevant data from its definition.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def list_pipeline_definitions(r: Request) -> PipelineDefinitions:
    registry = app_state.get_app_state(r).pipeline_registry
    return PipelineDefinitions(
        data=[_to_definition_info(pd) for pd in registry.list_latest()]
    )


_ANN_PIPELINE_NAME = Annotated[str, FastPath(
    examples=["readsqc"],
    description="The pipeline's name.",
    min_length=1,
    max_length=256,
)]


_ANN_PIPELINE_VERSION = Annotated[pipe_models.SemverVersion, FastPath(
    examples=["0.1.0"],
    description="The pipeline version.",
)]


@ROUTER_PIPELINES.get(
    "/available/{name}",
    response_model=PipelineDefinitionInfo,
    summary="Get the latest version of a pipeline definition",
    description="Get the latest registered version of a pipeline, along with relevant data "
        + "from its definition.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def get_pipeline_definition(r: Request, name: _ANN_PIPELINE_NAME) -> PipelineDefinitionInfo:
    registry = app_state.get_app_state(r).pipeline_registry
    return _to_definition_info(registry.get(name))


@ROUTER_PIPELINES.get(
    "/available/{name}/versions",
    response_model=PipelineDefinitions,
    summary="List all versions of a pipeline definition",
    description="List every registered version of a pipeline, along with relevant data from "
        + "each version's definition, sorted newest to oldest by semantic versioning.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def list_pipeline_definition_versions(
    r: Request, name: _ANN_PIPELINE_NAME
) -> PipelineDefinitions:
    registry = app_state.get_app_state(r).pipeline_registry
    versions = registry.list_versions(name)
    versions.reverse()
    return PipelineDefinitions(
        data=[_to_definition_info(pd) for pd in versions]
    )


@ROUTER_PIPELINES.get(
    "/available/{name}/versions/{version}",
    response_model=PipelineDefinitionInfo,
    summary="Get a specific version of a pipeline definition",
    description="Get a specific registered version of a pipeline, along with relevant data "
        + "from its definition.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def get_pipeline_definition_version(
    r: Request, name: _ANN_PIPELINE_NAME, version: _ANN_PIPELINE_VERSION
) -> PipelineDefinitionInfo:
    registry = app_state.get_app_state(r).pipeline_registry
    return _to_definition_info(registry.get(name, version))


class ListPipelineJobsResponse(BaseModel):
    """ The response to a successful pipeline job listing request. """
    jobs: Annotated[list[pipe_models.PipelineJobPreview], Field(description="The pipeline jobs")]


@ROUTER_PIPELINES.get(
    "/jobs",
    response_model=ListPipelineJobsResponse,
    response_model_exclude_none=True,
    summary="List pipeline jobs",
    description="List pipeline jobs for the current user.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def list_pipeline_jobs(
    r: Request,
    cluster: _ANN_JOB_SITE = None,
    state: _ANN_JOB_STATE = None,
    after: _ANN_JOB_AFTER = None,
    before: _ANN_JOB_BEFORE = None,
    limit: _ANN_JOB_LIMIT = 1000,
    user: CTSUser = Depends(_AUTH),
) -> ListPipelineJobsResponse:
    job_state = app_state.get_app_state(r).job_state
    return ListPipelineJobsResponse(jobs=await job_state.list_pipeline_jobs(
        user=user.user,
        site=cluster,
        state=state,
        after=after,
        before=before,
        limit=limit,
    ))


class SubmitPipelineJobResponse(BaseModel):
    """ The response to a successful pipeline job submission request. """
    job_id: Annotated[str, Field(description="An opaque job ID.")]


class PipelineJobInputCreate(pipe_models.PipelineJobInput):
    """ Input to a pipeline job. """

    # restrict to clusters registered for pipeline jobs
    cluster: Annotated[sites.PipelineCluster, Field(
        examples=[sites.PipelineCluster.PERLMUTTER_JAWS.value]
    )]


@ROUTER_PIPELINES.post(
    "/jobs",
    response_model=SubmitPipelineJobResponse,
    summary="Submit a pipeline job",
    description="Submit a job running a registered pipeline version to the system.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def submit_pipeline_job(
    r: Request,
    pipeline_job_input: PipelineJobInputCreate,
    user: CTSUser = Depends(_AUTH),
) -> SubmitPipelineJobResponse:
    job_state = app_state.get_app_state(r).job_state
    # PipelineJobInputCreate.cluster is PipelineCluster (a subset of Cluster) for API validation,
    # but downstream code expects PipelineJobInput with cluster typed as Cluster.
    # model_construct skips re-validation of the already-checked fields; only cluster is
    # overridden to convert PipelineCluster -> Cluster.
    job_input = pipe_models.PipelineJobInput.model_construct(**{
        **vars(pipeline_job_input),
        pipe_models.FLD_PIPELINE_JOB_INPUT_CLUSTER: sites.Cluster(
            pipeline_job_input.cluster.value
        ),
    })
    del pipeline_job_input
    job_id = await job_state.submit_pipeline_job(job_input, user)
    return SubmitPipelineJobResponse(job_id=job_id)


_ANN_PIPELINE_JOB_ID = Annotated[str, FastPath(
    openapi_examples={"job id": {"value": "pipeline-f0c24820-d792-4efa-a38b-2458ed8ec88f"}},
    description="The pipeline job ID.",
    pattern=r"^[\w-]+$",
    min_length=1,
    max_length=50,
)]


@ROUTER_PIPELINES.get(
    "/jobs/{job_id}/status",
    response_model=models.JobStatus,
    summary="Get a pipeline job's minimal status",
    description="Get minimal information about a pipeline job to determine the current status "
        + "of the job. Suitable for polling for job completion. Only the submitting user may "
        + "view the job.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def get_pipeline_job_status(
    r: Request,
    job_id: _ANN_PIPELINE_JOB_ID,
    user: CTSUser = Depends(_AUTH),
) -> models.JobStatus:
    job_state = app_state.get_app_state(r).job_state
    return await job_state.get_pipeline_job_status(job_id, user)


@ROUTER_PIPELINES.get(
    "/jobs/{job_id}",
    response_model=pipe_models.PipelineJob,
    response_model_exclude_none=True,
    summary="Get a pipeline job",
    description="Get a pipeline job. Only the submitting user may view the job.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def get_pipeline_job(
    r: Request,
    job_id: _ANN_PIPELINE_JOB_ID,
    user: CTSUser = Depends(_AUTH),
) -> pipe_models.PipelineJob:
    job_state = app_state.get_app_state(r).job_state
    return await job_state.get_pipeline_job(job_id, user)


@ROUTER_PIPELINES.put(
    "/jobs/{job_id}/cancel",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Cancel a pipeline job",
    description="Cancel a pipeline job.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def cancel_pipeline_job(
    r: Request,
    job_id: _ANN_PIPELINE_JOB_ID,
    user: CTSUser = Depends(_AUTH),
):
    job, flow = await _get_job_and_flow(r, job_id, user)
    await flow.cancel_job(job)


@ROUTER_PIPELINES.get(
    "/jobs/{job_id}/runner_status",
    response_model=models.ExternalRunnerStatus,
    summary="Get a pipeline job's external runner status",
    description="Get the status of the pipeline job as reported by the external job runner, "
        + "rather than the service's own state tracking.\n\n"
        + "**This endpoint is rarely needed.** The standard job status endpoint is faster, "
        + "cheaper, and sufficient for normal polling. Call this endpoint only when you suspect "
        + "the service's job state has desynced from the external runner — for example, "
        + "if a job appears stuck in the service but you want to verify what the runner reports."
        + "\n\nThis is an experimental API and is subject to change without notice."
)
async def get_pipeline_job_runner_status(
    r: Request,
    job_id: _ANN_PIPELINE_JOB_ID,
    user: CTSUser = Depends(_AUTH),
) -> models.ExternalRunnerStatus:
    job, flow = await _get_job_and_flow(r, job_id, user)
    return await flow.get_job_external_runner_status(job)


@ROUTER_ADMIN_PIPELINES.get(
    "/jobs",
    response_model=ListPipelineJobsResponse,
    response_model_exclude_none=True,
    summary="List pipeline jobs as an admin",
    description="List pipeline jobs for a provided user or all users.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def list_pipeline_jobs_admin(
    r: Request,
    user: _ANN_JOB_ADMIN_USER = None,
    cluster: _ANN_JOB_SITE = None,
    state: _ANN_JOB_STATE = None,
    after: _ANN_JOB_AFTER = None,
    before: _ANN_JOB_BEFORE = None,
    limit: _ANN_JOB_LIMIT = 1000,
    methoduser: CTSUser = Depends(_AUTH),
) -> ListPipelineJobsResponse:
    _ensure_admin(methoduser, "Only service administrators can list other users' pipeline jobs.")
    await _ensure_valid_kbase_user(r, user)
    job_state = app_state.get_app_state(r).job_state
    return ListPipelineJobsResponse(jobs=await job_state.list_pipeline_jobs(
        user=user,
        site=cluster,
        state=state,
        after=after,
        before=before,
        limit=limit,
    ))


@ROUTER_ADMIN_PIPELINES.get(
    "/jobs/{job_id}",
    response_model=pipe_models.AdminPipelineJob,
    response_model_exclude_none=True,
    summary="Get a pipeline job as an admin",
    description="Get any pipeline job, regardless of ownership, with additional details about "
        + "the job run.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def get_pipeline_job_admin(
    r: Request,
    job_id: _ANN_PIPELINE_JOB_ID,
    user: CTSUser = Depends(_AUTH),
) -> pipe_models.AdminPipelineJob:
    _ensure_admin_or_executor(
        user,  # external executors currently don't run pipeline jobs but this doesn't hurt
        "Only service administrators and external job executors can get pipeline jobs as "
            + "an admin."
    )
    job_state = app_state.get_app_state(r).job_state
    return await job_state.get_pipeline_job(job_id, user, as_admin=True)


@ROUTER_ADMIN_PIPELINES.get(
    "/jobs/{job_id}/runner_status",
    response_model=dict[str, Any],
    summary="Get a pipeline job's external status",
    description="Get the status of a pipeline job in an external job runner such as JAWS. "
        + "This endpoint should be used for informational purposes only and the data structure "
        + "may change at any time - changes are not treated as backwards incompatibilities. "
        + "If the job has not yet been submitted to an external runner, "
        + "an empty dictionary is returned.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def get_pipeline_job_runner_details_admin(
    r: Request,
    job_id: _ANN_PIPELINE_JOB_ID,
    user: CTSUser = Depends(_AUTH),
) -> dict[str, Any]:
    job, flow = await _admin_get_job_and_flow(
        r, job_id, user, "get pipeline job runner status."
    )
    return await flow.get_job_external_runner_details(job)


@ROUTER_ADMIN_PIPELINES.put(
    "/jobs/{job_id}/cancel",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Cancel any pipeline job",
    description="Cancel any pipeline job.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def cancel_pipeline_job_admin(
    r: Request,
    job_id: _ANN_PIPELINE_JOB_ID,
    user: CTSUser = Depends(_AUTH),
):
    job, flow = await _admin_get_job_and_flow(r, job_id, user, "cancel any pipeline job.")
    await flow.cancel_job(job)


@ROUTER_ADMIN_PIPELINES.delete(
    "/jobs/{job_id}/clean",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
    summary="Clean up after a pipeline job",
    description="Remove any pipeline job related files managed by this service at the remote "
        + "compute site. The job must be in a terminal state. If the job is already cleaned "
        + "this is a noop.\n\n"
        + "This is an experimental API and is subject to change without notice."
)
async def clean_pipeline_job(
    r: Request,
    job_id: _ANN_PIPELINE_JOB_ID,
    force: Annotated[bool, Query(
        description="**WARNING**: setting force to true may cause undefined behavior. True will "
        + "cause job files to be removed regardless of job state."
    )] = False,
    user: CTSUser = Depends(_AUTH),
):
    _ensure_admin(user, "Only service administrators can clean pipeline jobs.")
    appstate = app_state.get_app_state(r)
    job = await appstate.job_state.get_pipeline_job(job_id, user, as_admin=True)
    await appstate.flow_cleaner.clean_job(job, user, force=force)
