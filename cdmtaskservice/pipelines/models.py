"""
Data models for pipeline jobs - jobs that run a registered PipelineDefinition rather than an
arbitrary user-supplied image. These mirror the Job / JobInput / AdminJobDetails hierarchy in
cdmtaskservice.models, reusing the pieces that aren't specific to arbitrary job input.
"""

import dataclasses
from typing import Annotated, Any

import semver
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    PlainValidator,
    field_validator,
)

from cdmtaskservice import models, sites
from cdmtaskservice.s3.paths import validate_path, S3PathSyntaxError


FLD_PIPELINE_JOB_INPUT_CLUSTER = "cluster"
""" The field name of the cluster in a PipelineJobInput. """

FLD_PIPELINE_JOB_INPUT_INPUT = "input"
""" The field name of the pipeline input in a PipelineJobInput. """

FLD_PIPELINE_JOB_INPUT_FILES = "files"
""" The field name of the pipeline file references in a PipelineJobInput. """

PIPELINE_JOB_ID_PREFIX = "pipeline-"
""" The prefix used for pipeline job IDs, distinguishing them from standard job IDs. """


def is_pipeline_job_id(job_id: str) -> bool:
    """ Check whether a job ID refers to a pipeline job rather than a standard job. """
    return job_id.startswith(PIPELINE_JOB_ID_PREFIX)


SemverVersion = Annotated[
    semver.Version,
    PlainValidator(lambda v: v if isinstance(v, semver.Version) else semver.Version.parse(v)),
    PlainSerializer(str, return_type=str),
]
""" A pydantic-compatible semver.Version field - accepts a Version or a version string on input
and always serializes back to a string. """


@dataclasses.dataclass(frozen=True)
class PipelineSpec:
    """ Identifies a specific version of a registered pipeline. """

    name: str
    """ The pipeline's name. """

    version: semver.Version
    """ The pipeline's version. """


def _validate_s3_path(s3path: str) -> str:
    if not isinstance(s3path, str):  # run as a before validator so needs to check type
        raise ValueError("S3 paths must be a string")
    try:
        return validate_path(s3path)
    except S3PathSyntaxError as e:
        raise ValueError(str(e)) from e


class PipelineJobInputPreview(BaseModel):
    """
    The input to a pipeline job, consisting of fields containing small amounts of data -
    the pipeline's own input parameters alongside the pipeline identity and output location.
    Suitable for a list of jobs.
    """
    model_config = ConfigDict(extra="forbid")

    cluster: Annotated[sites.Cluster, Field(
        examples=[sites.Cluster.PERLMUTTER_JAWS.value],
        description="The cluster on which to run the pipeline.",
    )]
    input: Annotated[dict[str, Any], Field(
        description="The pipeline version's input parameters, in the shape defined by that "
            + "pipeline version's input model. Never contains file references.",
    )]
    pipeline: Annotated[str, Field(
        examples=["readsqc"],
        description="The name of the pipeline to run.",
        min_length=1,
        max_length=256,
    )]
    version: Annotated[SemverVersion, Field(
        examples=["0.1.0"],
        description="The exact version of the pipeline to run.",
    )]
    output_dir: Annotated[str, Field(
        examples=["mybucket/out"],
        description="The S3 folder, starting with the bucket, in which to place results.",
        min_length=models.S3_PATH_MIN_LENGTH,
        max_length=models.S3_PATH_MAX_LENGTH,
    )]

    @field_validator("output_dir", mode="before")
    @classmethod
    def _check_outdir(cls, v):
        return _validate_s3_path(v).rstrip("/") + "/"

    def get_pipeline_spec(self) -> PipelineSpec:
        """ Get the pipeline name and version this input targets. """
        return PipelineSpec(name=self.pipeline, version=self.version)


class PipelineJobInput(PipelineJobInputPreview):
    """
    The input to a pipeline job - the pipeline's own input alongside the pipeline identity and
    output location.
    """
    model_config = ConfigDict(extra="forbid")

    files: Annotated[dict[str, Any], Field(
        description="The pipeline version's file references, in the shape defined by that "
            + "pipeline version's input model. When returned from the service, the input "
            + "has been validated and any S3 files included always have a checksum.",
    )]


class PipelineJobPreview(models.JobStatus, models.InternalJobCommonPreviewFields):
    """
    Information about a pipeline job, consisting of fields containing small amounts of data.
    Suitable for a list of jobs.
    """
    # This is an outgoing data structure only so we don't add validators
    pipeline_input: PipelineJobInputPreview


class PipelineJob(PipelineJobPreview, models.InternalJobNonPreviewFields):
    """
    Information about a pipeline job. The pipeline equivalent of models.Job.
    """
    pipeline_input: PipelineJobInput


class AdminPipelineJob(models.InternalAdminJobFields, PipelineJob):
    """
    Information about a pipeline job with added details of interest to service administrators.
    The pipeline equivalent of models.AdminJobDetails.
    """
