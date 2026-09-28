"""
Data models for pipeline jobs - jobs that run a registered PipelineDefinition rather than an
arbitrary user-supplied image. These mirror the Job / JobInput / AdminJobDetails hierarchy in
cdmtaskservice.models, reusing the pieces that aren't specific to arbitrary job input.
"""

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


SemverVersion = Annotated[
    semver.Version,
    PlainValidator(lambda v: v if isinstance(v, semver.Version) else semver.Version.parse(v)),
    PlainSerializer(str, return_type=str),
]
""" A pydantic-compatible semver.Version field - accepts a Version or a version string on input
and always serializes back to a string. """


def _validate_s3_path(s3path: str) -> str:
    if not isinstance(s3path, str):  # run as a before validator so needs to check type
        raise ValueError("S3 paths must be a string")
    try:
        return validate_path(s3path)
    except S3PathSyntaxError as e:
        raise ValueError(str(e)) from e


class PipelineJobInput(BaseModel):
    """
    The input to a pipeline job - the pipeline's own input alongside the pipeline identity and
    output location.
    """
    model_config = ConfigDict(extra="forbid")

    cluster: Annotated[sites.PipelineCluster, Field(
        examples=[sites.PipelineCluster.PERLMUTTER_JAWS.value],
        description="The cluster on which to run the pipeline.",
    )]
    input: Annotated[dict[str, Any], Field(
        description="The pipeline version's input, in the shape defined by that pipeline "
            + "version's input model. When returned from the service, the input "
            + "has been validated and any S3 files included always have a checksum.",
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


class PipelineJobPreview(models.JobStatus, models.InternalJobCommonPreviewFields):
    """
    Information about a pipeline job, consisting of fields containing small amounts of data.
    Suitable for a list of jobs.

    Unlike job previews, pipeline_input is included as-is; the pipeline's input is an
    opaque, pipeline-specific structure and so cannot be trimmed down the way arbitrary job
    input files are for standard jobs.
    """
    # This is an outgoing data structure only so we don't add validators
    pipeline_input: PipelineJobInput


class PipelineJob(PipelineJobPreview, models.InternalJobNonPreviewFields):
    """
    Information about a pipeline job. The pipeline equivalent of models.Job.
    """


class AdminPipelineJob(models.InternalAdminJobFields, PipelineJob):
    """
    Information about a pipeline job with added details of interest to service administrators.
    The pipeline equivalent of models.AdminJobDetails.
    """
