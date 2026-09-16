"""
CDM pipeline job endpoints.
"""

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from typing import Annotated

from cdmtaskservice import app_state, sites
from cdmtaskservice.http_bearer import KBaseHTTPBearer
from cdmtaskservice.pipelines.models import FLD_PIPELINE_JOB_INPUT_CLUSTER, PipelineJobInput
from cdmtaskservice.user import CTSUser

ROUTER_PIPELINES = APIRouter(tags=["Pipelines - Experimental"], prefix="/pipelines")

_AUTH = KBaseHTTPBearer()


class SubmitPipelineJobResponse(BaseModel):
    """ The response to a successful pipeline job submission request. """
    job_id: Annotated[str, Field(description="An opaque job ID.")]


class PipelineJobInputCreate(PipelineJobInput):
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
    job_input = PipelineJobInput.model_construct(**{
        **vars(pipeline_job_input),
        FLD_PIPELINE_JOB_INPUT_CLUSTER: sites.Cluster(pipeline_job_input.cluster.value),
    })
    del pipeline_job_input
    job_id = await job_state.submit_pipeline_job(job_input, user)
    return SubmitPipelineJobResponse(job_id=job_id)
