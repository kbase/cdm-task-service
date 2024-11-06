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
from cdmtaskservice.s3.paths import validate_path, validate_bucket_name, S3PathError


# TODO TEST
# TODO EXAMPLES try using examples instead of the deprecated example. Last time I tried no joy

# https://en.wikipedia.org/wiki/Filename#Comparison_of_filename_limitations
# POSiX fully portable filenames and /
_PATH_REGEX=r"^[\w.-/]+$"


def _validate_bucket_name(bucket: str, index: int = None) -> str:
    try:
        return validate_bucket_name(bucket, index=index)
    except S3PathError as e:
        raise ValueError(str(e)) from e


def _validate_s3_path(s3path: str, index: int = None) -> str:
    if not isinstance(s3path, str):  # run as a before validator so needs to check type
        raise ValueError("S3 paths must be a string")
    try:
        return validate_path(s3path, index=index)
    except S3PathError as e:
        raise ValueError(str(e)) from e


def _err_on_control_chars(s: str, allowed_chars: list[str] = None):
    if s is None:
        return s
    pos = contains_control_characters(s, allowed_chars=allowed_chars)
    if pos > -1: 
        raise ValueError(f"contains a disallowed control character at position {pos}")
    return s


class ParameterType(str, Enum):
    """ The type of a parameter if not a string literal. """

    INPUT_FILES = "input_files"
    """
    This parameter type will cause the list of input files for the container to be inserted
    into the entrypoint command line.
    """
    
    MANIFEST_FILE = "manifest_file"
    """
    This parameter type will cause the list of input files for the container to be placed in a
    manifest file and will insert the file location into the entrypoint command line. 
    """


class InputFilesFormat(str, Enum):
    """ The format of an input files type parameter. """

    COMMA_SEPARATED_LIST = "comma_separated_list"
    """ The input files will be inserted as a comma separated list. """
    
    SPACE_SEPARATED_LIST = "space_separated_list"
    """ The input files will be inserted as a space separated list. """
    
    REPEAT_PARAMETER = "repeat_parameter"
    """
    The input files will be inserted as a space separated list, with each file preceded
    by the parameter flag. Only valid for CLI flag based arguments. If the flag ends with an
    equals sign, there is no space left between the flag and the parameter.
    
    Examples:
    -i: -i file1 -i file2 ... -i fileN
    --input_file=: --input_file=file1 --input_file=file2 ... --input_file=fileN
    """


class ManifestFileFormat(str, Enum):
    """ The format of a manifest file. """

    FILES = "files"
    """ The manifest file will consist of a list of the input files, one per line. """
    
    DATA_IDS = "data_ids"
    """
    The manifest file will consist of a list of the data IDs associated with the input files,
    one per line.
    """


class Parameter(BaseModel):
    """ Represents the value of a parameter passed to a container. """

    type: Annotated[ParameterType, Field(
        example=ParameterType.INPUT_FILES, description="The type of the parameter",
    )]
    input_files_format: Annotated[InputFilesFormat | None, Field(
        example=InputFilesFormat.COMMA_SEPARATED_LIST,
        description="The format of the input files when inserted into the container entrypoint "
            + f"command line. Required for {ParameterType.INPUT_FILES.value} parameters. "
            + f"Ignored for {ParameterType.MANIFEST_FILE.value} parameters.",
    )] = None
    manifest_file_format: Annotated[ManifestFileFormat | None, Field(
        example=ManifestFileFormat.FILES,
        description="The format for the manifest file. Required for "
            + f"{ParameterType.MANIFEST_FILE.value} parameters. "
            + f"Ignored for {ParameterType.MANIFEST_FILE.value} parameters.",
    )] = None
    manifest_file_header: Annotated[str | None, Field(
        example="genome_id",
        description="The header for the manifest file, if any. Only valid for, "
            + f"{ParameterType.MANIFEST_FILE.value} parameters. "
            + f"Ignored for {ParameterType.MANIFEST_FILE.value} parameters.",
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
    # TODO OPENAPI check f strings work with the open api docs
    f"""
    A set of parameters for a container in a job.
    
    Specifying files as part of arguments to a job can be done in two ways:
    
    * If the entrypoint command takes a glob or a directory as an argument, that glob or
      directory can be specified literally in an argument. All files will be available in the
      input mount point, with paths resulting from the path in S3 as well as the input_roots
      parameter from the job input.
    * If individual filenames need to be specified, use a {Parameter.__name__} instance in
      the positional, flag, or environmental arguments. At most one {Parameter.__name__} can
      be specified per parameter set.
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
            + "entrypoint command. Strings are treated as literals and can each be no more than "
            + "1000 characters."
        # TODO SECURITY be sure to quote the strings https://docs.python.org/dev/library/shlex.html#shlex.quote
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
            + "entrypoint command line. Strings are treated as literals. Keys and strings "
            + "can each be no more than 1000 characters."
        # TODO SECURITY be sure to quote the keys and strings https://docs.python.org/dev/library/shlex.html#shlex.quote
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
            + "Strings are treated as literals. Keys and strings "
            + "can each be no more than 1000 characters."
        # TODO SECURITY be sure to quote the keys and strings https://docs.python.org/dev/library/shlex.html#shlex.quote
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
        self.get_parameter()
        return self
    
    def get_parameter(self) -> Parameter | None:
        """
        If a file specification parameter is present in the arguments, get it.
        Returns None otherwise.
        """
        param = None
        if self.positional_args:
            for i, p in enumerate(self.positional_args):
                param = self._check_parameter(param, p, f"Positional parameter at index {i}")
        if self.flag_args:
            for p in self.flag_args.values():
                param = self._check_parameter(param, p)
        if self.environment:
            for key, p in self.environment.items():
                param = self._check_parameter(param, p, f"Environmental parameter at key {key}")
        return param

    def _check_parameter(self, param: str | Parameter, p: str | Parameter, loc: str = None):
        if isinstance(p, Parameter):
            if (loc and
                p.type is ParameterType.INPUT_FILES and
                p.input_files_format is InputFilesFormat.REPEAT_PARAMETER
            ):
                raise ValueError(
                    f"{loc} may not have {InputFilesFormat.REPEAT_PARAMETER.value} "
                    + f" set as a {ParameterType.INPUT_FILES.value} type"
                )
            if param is not None:
                raise ValueError(
                    # This may need to change for all vs all analyses
                    f"At most one {Parameter.__name__} instance is allowed per parameter set")
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
    """ The location where a job should run. """

    PERLMUTTER_JAWS = "perlmutter-jaws"
    """ The Perlmutter cluster at NESRC run via JAWS. """

    # TODO LAWRENCIUM add when available


FilesPerContainer = NamedTuple("FilesPerContainer", [
    ("containers", int),
    ("files_per_container", int),
    ("last_container", int),
    ("files", list[list[S3File | str]])
])
"""
Information about splitting files to be processed among containers.

containers - the number of containers expected to be run; this is the mimimum of the specified
    container count and the number of files.
files_per_container - the number of files to be run per container
last_container - the remainder of files to be run in the last container. Always less than
    files_per_container.
files - a list of lists of files, split up per container.
"""


class JobInput(BaseModel):
    """ Input to a Job. """
    
    # In the design there's a field for authentication source. This seems so unlikely to ever
    # be implemented we're leaving it out here. If it is implemented kbase should be the default.
    
    # Similarly, the design allows for multiple S3 instances. We'll implement with a default
    # when actually needed.
    
    cluster: Annotated[Cluster, Field(
        example=Cluster.PERLMUTTER_JAWS.value,
        description="The compute location where a job should run."
    )]
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
    params: Annotated[Parameters, Field(description="The job parameters.")]
    num_containers: Annotated[int, Field(
        example=1,
        default=1,
        description="The number of containers to run in parallel. Input files will be split "
            + "between the containers. If there are more containers than input files the "
            + "container count will be reduced appropriately",
        ge=1,
        # TODO LIMITS whats a reasonable max number of containers per job? 1k seems ok for now 
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
        description="The amount of memory to allocate per container.",
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
        le=3 * 24 * 60 * 60,  # max JAWS runtime
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
            + "Either all or no files must have data IDs associated with them."
            + "When returned from the service, the Etag is always included.",
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
            + "Whitespace on the left side of the path is is ignored."
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

    def inputs_are_S3File(self) -> bool:
        f"""
        Returns True if the inputfiles are of type {S3File.__name__}, False otherwise.
        """
        return isinstance(self.input_files[0], S3File)
        
    def get_files_per_container(self) -> FilesPerContainer:
        """
        Returns the number of files to be run per container and the files, split up by container.
        """
        containers = min(self.num_containers, len(self.input_files))
        fpc = math.ceil(len(self.input_files) / containers)
        files = [self.input_files[i:i + fpc] for i in range(0, fpc * containers, fpc)]
        return FilesPerContainer(containers, fpc, len(self.input_files) % fpc, files)


class JobState(str, Enum):
    """
    The state of a job.
    """
    # TODO OPENAPI add documentation when the server is running & it's clear how enum docs work
    CREATED = "created"
    UPLOAD_SUBMITTED = "upload_submitted"
    JOB_SUBMITTING = "job_submitting"
    JOB_SUBMITTED = "job_submitted"
    DOWNLOAD_SUBMITTING = "download_submitting"
    DOWNLOAD_SUBMITTED = "download_submitted"
    COMPLETE = "complete"
    ERROR = "error"


class Image(BaseModel):
    """
    Information about a Docker image.
    """
    # This is an outgoing data structure only so we don't add validators
    normed_name: Annotated[str, Field(
        example="ghcr.io/kbase/collections"
            +"@sha256:c9291c94c382b88975184203100d119cba865c1be91b1c5891749ee02193d380",
        description="The normalized name of the a docker image, consisting of the "
            + "host, path, and digest.",
    )]
    entrypoint: Annotated[list[str], Field(
        example=["checkm2", "predict"],
        description="The entrypoint command extracted from the image."
    )]
    tag: Annotated[str | None, Field(
        example="checkm2_0.1.6 ",
        description="The image tag at registration time. "
            + "The tag may no longer point to the same image."
    )] = None
    # TODO REFERENCEDATA add reference data ID


class Job(BaseModel):
    """
    Information about a job.
    """
    # This is an outgoing data structure only so we don't add validators
    id: Annotated[str, Field(
        description="An opaque, unique string that serves as the job's ID.",
    )]
    job_input: Annotated[JobInput, Field(description="The job input.")]
    user: Annotated[str, Field(example="myuserid", description="The user that ran the job.")]
    image: Annotated[Image, Field(description="The image to be run in the job.")]
    state: Annotated[JobState, Field(
        example=JobState.COMPLETE.value,
        description="The state of the job."
    )]
    # hmm, should this be a dict vs a list of tuples?
    transition_times: Annotated[list[tuple[JobState, datetime.datetime]], Field(
        example=[
            (JobState.CREATED.value, "2024-10-24T22:35:40Z"),
            (JobState.UPLOAD_SUBMITTED.value, "2024-10-24T22:35:41Z"),
            (JobState.JOB_SUBMITTING, "2024-10-24T22:47:67Z"),
        ],
        description="A list of tuples of (job_state, time_job_state_entered)."
    )]
    # TODO JOBRUNNING add job info like the jaws ID(?) and log locations. See design doc.
    #      should we expose the JAWS ID to users? Maybe just admins. Subclass this if so
    # TODO ERRORHANDLING add error field and class
