"""
Pydantic models for the CTS.
"""

import datetime
from enum import Enum
import math
from pathlib import Path
from pydantic import (
    BaseModel,
    Field,
    ByteSize,
    ConfigDict,
    StringConstraints,
    field_validator,
    model_validator,
)
from typing import Annotated, Self, NamedTuple

from cdmtaskservice.arg_checkers import contains_control_characters
from cdmtaskservice.s3.paths import validate_path, validate_bucket_name, S3PathSyntaxError


# TODO TEST
# TODO EXAMPLES try using examples instead of the deprecated example. Last time I tried no joy
#               still doesn't seem to work as of 24/11/11
#               https://github.com/fastapi/fastapi/discussions/11137

# WARNNING: Model field names also define field names in the MOngo database.
# As such, field names cannot change without creating a mapping for field names in the mongo
# layer, or data will not be returned correclty and corruption of old datq may occur.
# The strings below are used in the mongo interface to define fields and indexes.
# They must match with the field names in the models.

FLD_IMAGE_NAME = "name"
FLD_IMAGE_DIGEST = "digest"
FLD_IMAGE_TAG = "tag"
FLD_JOB_ID = "id"
FLD_JOB_JOB_INPUT = "job_input"
FLD_JOB_INPUT_RUNTIME = "runtime"
FLD_JOB_TRANS_TIMES = "transition_times"
FLD_JOB_STATE = "state"
FLD_JOB_NERSC_DETAILS = "nersc_details"
FLD_NERSC_DETAILS_DL_TASK_ID = "download_task_id"
FLD_NERSC_DETAILS_UL_TASK_ID = "upload_task_id"
FLD_NERSC_DETAILS_LOG_UL_TASK_ID = "log_upload_task_id"
FLD_JOB_JAWS_DETAILS = "jaws_details"
FLD_JAWS_DETAILS_RUN_ID = "run_id"
FLD_JOB_OUTPUTS = "outputs"
FLD_JOB_ERROR = "error"
FLD_JOB_ADMIN_ERROR = "admin_error"
FLD_JOB_TRACEBACK = "traceback"


# https://en.wikipedia.org/wiki/Filename#Comparison_of_filename_limitations
# POSiX fully portable filenames and /
_PATH_REGEX=r"^[\w.-/]+$"


def _validate_bucket_name(bucket: str, index: int = None) -> str:
    try:
        return validate_bucket_name(bucket, index=index)
    except S3PathSyntaxError as e:
        raise ValueError(str(e)) from e


def _validate_s3_path(s3path: str, index: int = None) -> str:
    if not isinstance(s3path, str):  # run as a before validator so needs to check type
        raise ValueError("S3 paths must be a string")
    try:
        return validate_path(s3path, index=index)
    except S3PathSyntaxError as e:
        raise ValueError(str(e)) from e


def _err_on_control_chars(s: str, allowed_chars: list[str] = None):
    if s is None:
        return s
    pos = contains_control_characters(s, allowed_chars=allowed_chars)
    if pos > -1: 
        raise ValueError(f"contains a disallowed control character at position {pos}")
    return s


# Note: doc strings on the individual enums don't show up in OpenAPI docs.
class ParameterType(str, Enum):
    """
    The type of a parameter if not a string literal.
    
    input_files will cause the list of input files for the container to be inserted
    into the entrypoint command line or an environment variable.
    
    manifest_file will cause the list of input files for the container to be placed in a
    manifest file and will insert the file location into the entrypoint command line
    or an environment variable.
    
    container_number will insert the integer number of the container into the
    entrypoint command line or an environment variable. This can be used so prevent file path
    collisions if multiple containers write to the same file paths.
    """

    INPUT_FILES = "input_files"
    MANIFEST_FILE = "manifest_file"
    CONTAINTER_NUMBER = "container_number"


class InputFilesFormat(str, Enum):
    """
    The format of an input files type parameter.
    
    Comma and space separated lists format the list of files as one would expect.
    
    Repeat parameter causes the input files to be inserted as a space separated list,
    with each file preceded by the parameter flag. Only valid for CLI flag based arguments.
    If the flag ends with an equals sign, there is no space left between the flag and the
    parameter.
    
    Examples:  
    `-i`: `-i file1 -i file2 ... -i fileN`  
    `--input_file=`: `--input_file=file1 --input_file=file2 ... --input_file=fileN`  
    """

    COMMA_SEPARATED_LIST = "comma_separated_list"
    SPACE_SEPARATED_LIST = "space_separated_list"
    REPEAT_PARAMETER = "repeat_parameter"


class ManifestFileFormat(str, Enum):
    """
    The format of a manifest file.
    
    files will cause the manifest file to consist of a list of the input files, one per line.
    
    data_ids will cause the manifest file to consist of a list of the data IDs associated with
    the input files, one per line.
    """

    FILES = "files"
    DATA_IDS = "data_ids"


class Parameter(BaseModel):
    """
    Represents a special parameter passed to a container.
    
    Can represent a set of input file names formatted in various ways, a manifest file
    containing file paths or data IDs, or a container number.
    
    input_files_format is required when the type is input_files, but ignored otherwise.  
    manifest_file_format is required when the type is manifest_file, but ignored otherwise.  
    manifest_file_header is optional and only taken into account when the type is manifest_file.  
    """

    type: Annotated[ParameterType, Field(example=ParameterType.INPUT_FILES)]
    input_files_format: Annotated[InputFilesFormat | None, Field(
        example=InputFilesFormat.COMMA_SEPARATED_LIST,
    )] = None
    manifest_file_format: Annotated[ManifestFileFormat | None, Field(
        example=ManifestFileFormat.FILES,
    )] = None
    manifest_file_header: Annotated[str | None, Field(
        example="genome_id",
        description="The header for the manifest file, if any.",
        min_length=1,
        max_length=1000,
    )] = None
    
    @field_validator("manifest_file_header", mode="after")
    @classmethod
    def _validate_manifest_file_header(cls, v):
        return _err_on_control_chars(v, allowed_chars=["\t"])

    @model_validator(mode="after")
    def _check_fields(self) -> Self:
        match self.type:
            case ParameterType.INPUT_FILES:
                if not self.input_files_format:
                    raise ValueError("The input_files_format field is required for "
                                     + f"{ParameterType.INPUT_FILES.value} parameter types")
                return self
            case ParameterType.MANIFEST_FILE:
                if not self.manifest_file_format:
                    raise ValueError("The manifest_file_format field is required for "
                                     + f"{ParameterType.MANIFEST_FILE.value} parameter types")
                return self
            case ParameterType.CONTAINTER_NUMBER:
                return self
            case _:
                # Impossible to test but here for safety if new types are added
                raise ValueError(f"Unknown parameter type: {self.type}")
        return self


ArgumentString = Annotated[str, StringConstraints(
    min_length=1,
    max_length=1024,
    pattern=r"^[\w\./][\w\./-]*$"  # open this up as needed, careful to not allow dangerous chars
)]


Flag = Annotated[str, StringConstraints(
    min_length=2,
    max_length=256,
    # open this up as needed, careful to not allow dangerous chars
    pattern=r"^--?[a-zA-Z][\w\.-]*=?$"
)]


EnvironmentVariable = Annotated[str, StringConstraints(
    min_length=1,
    max_length=256,
    pattern=r"^[A-Z_][A-Z0-9_]*$"  # https://stackoverflow.com/a/2821183/643675
)]


class Parameters(BaseModel):
    """
    A set of parameters for a container in a job.
    
    Specifying files as part of arguments to a job can be done in three ways:
    
    * If the entrypoint command takes a glob or a directory as an argument, that glob or
      directory can be specified literally in an argument. All files will be available in the
      input mount point, with paths resulting from the path in S3 as well as the input_roots
      parameter from the job input.
    * If individual filenames need to be specified, use a Parameter instance in
      the positional, flag, or environmental arguments. At most one Parameter can
      be specified per parameter set.
    * A Parameter instance can also be used to specify a manifest of file names or
      file IDs to be passed to the container.
    """
    
    input_mount_point: Annotated[str, Field(
        example="/input_files",
        default="/input_files",
        description="Where to place input files in the container. "
            + "Must start from the container root and include at least one directory "
            + "when resolved.",
        min_length=1,
        max_length=1024,
        pattern=_PATH_REGEX,
    )] = "/input_files"
    output_mount_point: Annotated[str, Field(
        example="/output_files",
        default="/output_files",
        description="Where output files should be written in the container. "
            + "Must start from the container root and include at least one directory "
            + "when resolved.",
        min_length=1,
        max_length=1024,
        pattern=_PATH_REGEX,
    )] = "/output_files"
    # TODO REFDATA if the container requires refdata throw an error if this is None
    refdata_mount_point: Annotated[str | None, Field(
        example="/reference_data",
        description="Where reference data files should be pleased in the container. "
            + "Must start from the container root and include at least one directory "
            + "when resolved.",
        min_length=1,
        max_length=1024,
        pattern=_PATH_REGEX,
    )] = None
    positional_args: Annotated[list[ArgumentString | Parameter] | None, Field(
        example=[
            "process",
            {
                "type": ParameterType.INPUT_FILES.value,
                "input_files_format": InputFilesFormat.COMMA_SEPARATED_LIST.value,
            }
        ],
        description="A list of positional parameters to be inserted at the end of the container "
            + "entrypoint command. Strings are treated as literals."
    )] = None
    flag_args: Annotated[dict[Flag, ArgumentString | Parameter] | None, Field(
        example={
            "--output-dir": "/output_files",
            "--input-file=": {
                "type": ParameterType.INPUT_FILES.value,
                "input_files_format": InputFilesFormat.REPEAT_PARAMETER.value,
            },
        },
        description="A dictionary of flag parameters to be inserted into the container "
            + "entrypoint command line. String values are treated as literals."
    )] = None
    environment: Annotated[dict[EnvironmentVariable, ArgumentString | Parameter] | None, Field(
        example={
            "DIAMOND_DB_PATH": "/reference_data",
            "FILE_MANIFEST": {
                "type": ParameterType.MANIFEST_FILE.value,
                "manifest_file_format": ManifestFileFormat.FILES.value,
                "manifest_file_header": "infiles",
            },
        },
        description="A dictionary of environment variables to be inserted into the container. "
            + "String values are treated as literals."
    )] = None
    
    @field_validator(
        "input_mount_point",
        "output_mount_point",
        "refdata_mount_point",
        mode="after",
    )
    @classmethod
    def _check_path(cls, v):
        if v is None:
            return None
        vp = Path(v)
        if vp.root != "/":
            raise ValueError("path must be absolute")
        if len(vp.resolve().parts) < 2:
            raise ValueError("path must contain at least one directory under root")
        return v
    
    @model_validator(mode="after")
    def _check_parameters(self) -> Self:
        self.get_file_parameter()
        return self
    
    def get_file_parameter(self) -> Parameter | None:
        """
        If a file specification parameter is present in the arguments, get it.
        Returns None otherwise.
        """
        # tried methodtools.lru_cache here but Pydantic complained
        if hasattr(self, "_param"):  # memoize
            return self._param
        param = None
        if self.positional_args:
            for i, p in enumerate(self.positional_args):
                param = self._check_parameter(param, p, f"Positional parameter at index {i}")
        if self.flag_args:
            for p in self.flag_args.values():
                param = self._check_parameter(param, p)
        if self.environment:
            for key, p in self.environment.items():
                param = self._check_parameter(
                    # Space separated lists in environment variables don't really make sense.
                    # Come back to this if needed.
                    param, p, f"Environmental parameter at key {key}", no_space=True)
        self._param = param
        return param

    def _check_parameter(
            self, param: str | Parameter, p: str | Parameter, loc: str = None, no_space=False
    ):
        if isinstance(p, Parameter):
            if p.type is ParameterType.CONTAINTER_NUMBER:
                return param
            if loc and p.type is ParameterType.INPUT_FILES:
                if p.input_files_format is InputFilesFormat.REPEAT_PARAMETER:
                    raise ValueError(
                        f"{loc} may not have {InputFilesFormat.REPEAT_PARAMETER.value} "
                        + f" set as a {ParameterType.INPUT_FILES.value} type"
                    )
                if no_space and p.input_files_format is InputFilesFormat.SPACE_SEPARATED_LIST:
                    raise ValueError(
                        f"{loc} may not have {InputFilesFormat.SPACE_SEPARATED_LIST.value} "
                        + f" set as a {ParameterType.INPUT_FILES.value} type"
                    )
            if param is not None:
                raise ValueError(
                    # This may need to change for all vs all analyses
                    f"At most one {Parameter.__name__} instance "
                    + "with file input is allowed per parameter set")
            return p
        return param


class S3File(BaseModel):
    """ A file in an S3 instance. """
    
    file: Annotated[str, Field(
        example="mybucket/foo/bar/baz.jpg",
        description="A path to an object in an S3 instance, starting with the bucket.",
        min_length=3 + 1 + 1,  # 3 for bucket + / + 1 char
        max_length=63 + 1 + 1024,  # 63 for bucket + / + 1024 bytes
    )]
    data_id: Annotated[str | None, Field(
        example="GCA_000146795.3",
        description="An arbitrary string representing the ID of the data in the file.",
        min_length=1,
        max_length=255,
    )] = None
    etag: Annotated[str | None, Field(
        example="a70a4d1732484e75434df2c08570e1b2-3",
        description="The S3 e-tag of the file. Weak e-tags are not supported. "
            + "If provided on input it is checked against the "
            + "target file e-tag before proceeding. Always provided with output.",
        min_length=32,
        max_length=32 + 1 + 5,  # 32 for the md5ish + dash + up to 10000 parts = 38
    )] = None
    
    @field_validator("file", mode="before")
    @classmethod
    def _check_file(cls, v):
        return _validate_s3_path(v)

    @field_validator("data_id", mode="after")
    @classmethod
    def _check_data_id(cls, v):
        return _err_on_control_chars(v)

    # Don't bother validating the etag beyond length, it'll be compared to the file etag on 
    # the way in and will come from S3 on the way out
    
    model_config = ConfigDict(frozen=True)
    
# TODO FEATURE How to handle all vs all? Current model is splitting file list between containers


class Cluster(str, Enum):
    """
    The location where a job should run.
    
    perlmutter-jaws: The Perlmutter cluster at NESRC run via JAWS.
    """

    PERLMUTTER_JAWS = "perlmutter-jaws"
    # TODO LAWRENCIUM add when available


class JobInput(BaseModel):
    """ Input to a Job. """
    
    # In the design there's a field for authentication source. This seems so unlikely to ever
    # be implemented we're leaving it out here. If it is implemented kbase should be the default.
    
    # Similarly, the design allows for multiple S3 instances. We'll implement with a default
    # when actually needed.
    
    cluster: Annotated[Cluster, Field(example=Cluster.PERLMUTTER_JAWS.value)]
    image: Annotated[str, Field(
        example="ghcr.io/kbase/collections:checkm2_0.1.6"
            + "@sha256:c9291c94c382b88975184203100d119cba865c1be91b1c5891749ee02193d380",
        description="The Docker image to run for the job. Include the SHA to ensure the "
            + "exact code requested is run.",
        # Don't bother validating other than some basic checks, validation will occur when
        # checking / getting the image SHA from the remote repository
        min_length=1,
        max_length=1000
    )]
    params: Parameters
    num_containers: Annotated[int, Field(
        example=1,
        default=1,
        description="The number of containers to run in parallel. Input files will be split "
            + "between the containers. If there are more containers than input files the "
            + "container count will be reduced appropriately.",
        ge=1,
        le=1000,
    )] = 1
    cpus: Annotated[int, Field(
        example=1,
        default=1,
        description="The number of CPUs to allocate per container.",
        ge=1,
        # https://jaws-docs.readthedocs.io/en/latest/Resources/compute_resources.html#table-of-available-resources
        le=256,
    )] = 1
    memory: Annotated[ByteSize, Field(
        example="10MB",
        default="10MB",
        description="The amount of memory to allocate per container either as the number of "
            + "bytes or a specification string such as 100MB, 2GB, etc.",
        # https://jaws-docs.readthedocs.io/en/latest/Resources/compute_resources.html#table-of-available-resources
        ge=1 * 1000 * 1000,
        le=492 * 1000 * 1000 * 1000
    )] = 10 * 1000 * 1000
    runtime: Annotated[datetime.timedelta, Field(
        example="PT12H30M5S",  # TODO EXAMPLES add a seconds example if examples works
        default = "PT60S",
        description="The runtime required for each container as the number of seconds or an "
            + "ISO8601 duration string.",
        ge=1,
        # https://jaws-docs.readthedocs.io/en/latest/Resources/compute_resources.html#table-of-available-resources
        # TODO LAWRENCIUM can handle 3 days, need to check per site. same for CPU
        le=2 * 24 * 60 * 60 - (15 * 60),  # max JAWS runtime
    )] = datetime.timedelta(seconds=60)
    input_files: Annotated[list[str] | list[S3File], Field(
        example=[  # TODO EXAMPLES add a string example if examples works
            {
                "file": "mybucket/foo/bat",
                "data_id": "GCA_000146795.3",
                "etag": "a70a4d1732484e75434df2c08570e1b2-3"
            }
        ],
        description="The S3 input files for the job, either a list of file paths as strings or a "
            + "list of data structures including the file path and optionally a data ID and / or "
            + "ETag. The file paths always start with the bucket. "
            + "Either all or no files must have data IDs associated with them. "
            + "When returned from the service, the ETag is always included.",
        min_length=1,
        # Need to see how well this performs. If we need more files per job,
        # can test performance & tweak S3 client concurrency. Alternatively, add a file set
        # endpoint where file sets of size < some reasonable number can be submitted, and then
        # multiple file sets can be combined in a JobInput
        max_length=10000,
    )]
    input_roots: Annotated[list[str] | None, Field(
        example=["mybucket/foo/bar"],
        description="If specified, preserves file hierarchies for S3 files below the given "
            + "root paths, starting from the bucket. Any files that are not prefixed by a "
            + "root path are placed in the root directory of the job input dir. "
            + "If any input files have the same path in the input dir, the job fails. "
            + 'For example, given a input_roots entry of ["mybucket/foo/bar"] and the input '
            + 'files ["otherbucket/foo", "mybucket/bar", "mybucket/foo/bar/baz/bat"] the job '
            + "input directory would include the files foo, bar, and baz/bat. "
            + 'To preserve hierarchies for all files, set input_roots to [""].  '
            + "If an input file matches more than one root, the longest root that isn't "
            + "identical to the file path is used. "
            + "Whitespace on the left side of the path is is ignored. "
            + "Duplicate roots are ignored.",
    )] = None
    output_dir: Annotated[str, Field(
        example="mybucket/out",
        description="The S3 folder, starting with the bucket, in which to place results.",
        min_length=3 + 1 + 1,
        max_length=63 + 1 + 1024
    )]
    
    @field_validator("input_files", mode="before")
    @classmethod
    def _check_input_files(cls, v):
        if v is None:
            return None
        newlist = []
        for i, f in enumerate(v):
            if isinstance(f, str):
                newlist.append(_validate_s3_path(f, index=i))
            else:
                newlist.append(f)
        return newlist
    
    @field_validator("input_files", mode="after")
    @classmethod
    def _check_data_ids(cls, v):
        if not isinstance(v[0], S3File):
            return v
        data_ids = bool(v[0].data_id)
        for f in v:
            if bool(f.data_id) is not data_ids:
                raise ValueError("Either all or no files must have data IDs")
        return v
    
    @field_validator("input_roots", mode="before")
    @classmethod
    def _check_input_roots(cls, v):
        if v is None:
            return None
        newlist = set()
        for i, ir in enumerate(v):
            if not isinstance(ir, str):  # before validator so need to check type
                raise ValueError(f"S3 path must be a string at index {i}")
            if not ir.strip():
                newlist.add("")
            else:
                parts = ir.split("/")
                if len(parts) == 1:
                    newlist.add(_validate_bucket_name(parts[0], index=i))
                elif len(parts) == 2 and not parts[1].strip():  # like "foo/   "
                    newlist.add(_validate_bucket_name(parts[0], index=i))
                else:
                    newlist.add(_validate_s3_path(ir, index=i))
        return list(newlist)
    
    @field_validator("output_dir", mode="before")
    @classmethod
    def _check_outdir(cls, v):
        return _validate_s3_path(v)
    
    @model_validator(mode="after")
    def _check_manifest_data_ids(self) -> Self:
        fp = self.params.get_file_parameter()
        # This seems really sketchy. Not sure how else to do it though
        if (fp
            # manifest file format is ignored, but not necc None, if type is not manifest file
            and fp.type is ParameterType.MANIFEST_FILE
            and fp.manifest_file_format is ManifestFileFormat.DATA_IDS
            and (
                not isinstance(self.input_files[0], S3File)
                or not self.input_files[0].data_id
            )
        ):
            raise ValueError(
                "If a manifest file with data IDs is specified, "
                + "data IDs must be supplied for all files"
            )
        return self
    
    @model_validator(mode="after")
    def _check_num_containers(self) -> Self:
        if self.num_containers > len(self.input_files):
            raise ValueError(
                "num_containers must be less than or equal to the number of input files"
            )
        return self

    def inputs_are_S3File(self) -> bool:
        """
        Returns True if the inputfiles are of type S3File, False otherwise.
        """
        return isinstance(self.input_files[0], S3File)
        
    def get_files_per_container(self) -> list[list[S3File | str]]:
        """
        Returns the input files split up by container.
        """
        fpc, extra_files = divmod(len(self.input_files), self.num_containers)
        infiles = list(self.input_files)
        files = []
        for i in range(1, self.num_containers + 1):
            # this seems dumb. It works though, make it pretty later
            fcount = fpc + (1 if i <= extra_files else 0)
            files.append(infiles[:fcount])
            infiles = infiles[fcount:]
        return files


class JobState(str, Enum):
    """
    The state of a job.
    """
    CREATED = "created"
    DOWNLOAD_SUBMITTED = "download_submitted"
    JOB_SUBMITTING = "job_submitting"
    JOB_SUBMITTED = "job_submitted"
    UPLOAD_SUBMITTING = "upload_submitting"
    UPLOAD_SUBMITTED = "upload_submitted"
    COMPLETE = "complete"
    ERROR_PROCESSING_SUBMITTING = "error_processing_submitting"
    ERROR_PROCESSING_SUBMITTED = "error_processing_submitted"
    ERROR = "error"


class Image(BaseModel):
    """
    Information about a Docker image.
    """
    # This is an outgoing data structure only so we don't add validators
    name: Annotated[str, Field(
        example="ghcr.io/kbase/collections",
        description="The normalized name of the a docker image, consisting of the "
            + "host and path.",
    )]
    digest: Annotated[str, Field(
        example="sha256:c9291c94c382b88975184203100d119cba865c1be91b1c5891749ee02193d380",
        description="The image digest.",
    )]
    entrypoint: Annotated[list[str], Field(
        example=["checkm2", "predict"],
        description="The entrypoint command extracted from the image."
    )]
    tag: Annotated[str | None, Field(
        example="checkm2_0.1.6",
        description="The image tag at registration time. "
            + "The tag may no longer point to the same image."
    )] = None
    registered_by: Annotated[str, Field(
        example="aparkin",
        description="The username of the user that registered this image."
    )]
    registered_on: Annotated[datetime.datetime, Field(
        example="2024-10-24T22:35:40Z",
        description="The time of registration."
    )]
    # TODO REFERENCEDATA add reference data ID
    
    @property
    def name_with_digest(self):
        """
        Returns the normalized name with the digest, e.g. name@digest.
        
        This form always refers to the same image and is compatible
        with Docker, Shifter, and Apptainer.
        """ 
        return f"{self.name}@{self.digest}"


class S3FileOutput(BaseModel):
    """ An output file in an S3 instance. """

    # no validators since this is an outgoing data structure only
    file: Annotated[str, Field(
        example="mybucket/foo/bar/baz.jpg",
        description="A path to an object in an S3 instance, starting with the bucket.",
    )]
    etag: Annotated[str, Field(
        example="a70a4d1732484e75434df2c08570e1b2-3",
        description="The S3 e-tag of the file. Weak e-tags are not supported. "
    )]


class Job(BaseModel):
    """
    Information about a job.
    """
    # This is an outgoing data structure only so we don't add validators
    id: Annotated[str, Field(
        description="An opaque, unique string that serves as the job's ID.",
    )]
    job_input: JobInput
    user: Annotated[str, Field(example="myuserid", description="The user that ran the job.")]
    image: Image
    state: Annotated[JobState, Field(example=JobState.COMPLETE.value)]
    # hmm, should this be a dict vs a list of tuples?
    transition_times: Annotated[list[tuple[JobState, datetime.datetime]], Field(
        example=[
            (JobState.CREATED.value, "2024-10-24T22:35:40Z"),
            (JobState.UPLOAD_SUBMITTED.value, "2024-10-24T22:35:41Z"),
            (JobState.JOB_SUBMITTING, "2024-10-24T22:47:67Z"),
        ],
        description="A list of tuples of (job_state, time_job_state_entered)."
    )]
    outputs: list[S3FileOutput] | None = None
    error: Annotated[str | None, Field(
        example="The front fell off",
        description="A description of the error that occurred."
    )] = None


class NERSCDetails(BaseModel):
    """
    Details about a job run at NERSC.
    """
    download_task_id: Annotated[list[str], Field(
        description="IDs for tasks run via the NERSC SFAPI to download files from an S3 "
            + "instance to NERSC. Note that task details only persist for ~10 minutes past "
            + "completion in the SFAPI. Multiple tasks indicate job retries after failures."
    )]
    upload_task_id: Annotated[list[str], Field(
        description="IDs for tasks run via the NERSC SFAPI to upload files to an S3 "
            + "instance from NERSC. Note that task details only persist for ~10 minutes past "
            + "completion in the SFAPI. Multiple tasks indicate job retries after failures."
            + "Empty if an upload task has not yet been submitted to NERSC."
    )] = []
    log_upload_task_id: Annotated[list[str], Field(
        description="IDs for tasks run via the NERSC SFAPI to upload log files to an S3 "
            + "instance from NERSC. Note that task details only persist for ~10 minutes past "
            + "completion in the SFAPI. Multiple tasks indicate job retries after failures."
            + "Empty if a log upload task has not yet been submitted to NERSC."
    )] = []


class JAWSDetails(BaseModel):
    """
    Details about a JAWS job run.
    """
    run_id: Annotated[list[str], Field(
        description="Run IDs for a JGI JAWS job. Multiple run IDs indicate job retries after "
            + "failures.")]

class AdminJobDetails(Job):
    """
    Information about a job with added details of interest to service administrators.
    """
    nersc_details: NERSCDetails | None = None
    jaws_details: JAWSDetails | None = None
    admin_error: Annotated[str | None, Field(
        example="The back fell off",
        description="A description of the error that occurred oriented towards service "
            + "admins, potentially including more details."
    )] = None
    traceback: Annotated[str | None, Field(description="The error's traceback.")] = None
