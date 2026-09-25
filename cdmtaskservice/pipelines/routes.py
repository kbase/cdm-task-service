"""
CDM pipeline job endpoints.
"""

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from typing import Annotated, Any

from cdmtaskservice import app_state, sites
from cdmtaskservice.http_bearer import KBaseHTTPBearer
from cdmtaskservice.pipelines import models as pipe_models
from cdmtaskservice.pipelines.definition import PipelineDefinition
from cdmtaskservice.user import CTSUser

ROUTER_PIPELINES = APIRouter(tags=["Pipelines - Experimental"], prefix="/pipelines")

_AUTH = KBaseHTTPBearer()


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
        description="The latest version of each registered pipeline."
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
