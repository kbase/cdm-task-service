"""
Pydantic models for the CTS.
"""

import datetime
from enum import Enum
from pydantic import (
    BaseModel,
    ByteSize,
    ConfigDict,
    conlist,
    Field,
    field_validator,
    HttpUrl,
    model_validator,
    StringConstraints,
)
from typing import Annotated, Self

from cdmtaskservice.arg_checkers import (
    contains_control_characters as _contains_control_characters,
    not_falsy as _not_falsy,
)
from cdmtaskservice.exceptions import InvalidReferenceDataStateError
from cdmtaskservice.s3.paths import validate_path, validate_bucket_name, S3PathSyntaxError
from cdmtaskservice import sites


# TODO TEST

# WARNNING: Model field names also define field names in the MOngo database.
# As such, field names cannot change without creating a mapping for field names in the mongo
# layer, or data will not be returned correclty and corruption of old datq may occur.
# The strings below are used in the mongo interface to define fields and indexes.
# They must match with the field names in the models.

FLD_IMAGE_NAME = "name"
FLD_IMAGE_DIGEST = "digest"
FLD_IMAGE_TAG = "tag"
FLD_JOB_USER = "user"
FLD_JOB_JOB_INPUT = "job_input"
FLD_JOB_INPUT_INPUT_FILES = "input_files"
FLD_JOB_INPUT_RUNTIME = "runtime"
FLD_JOB_STATE_TRANSITION_ID = "trans_id"
FLD_JOB_STATE_TRANSITION_NOTIFICATION_SENT = "notif_sent"
FLD_JOB_ADMIN_META = "admin_meta"
FLD_JOB_NERSC_DETAILS = "nersc_details"
FLD_NERSC_DETAILS_DL_TASK_ID = "download_task_id"
FLD_NERSC_DETAILS_UL_TASK_ID = "upload_task_id"
FLD_NERSC_DETAILS_LOG_UL_TASK_ID = "log_upload_task_id"
FLD_JOB_JAWS_DETAILS = "jaws_details"
FLD_JAWS_DETAILS_RUN_ID = "run_id"
FLD_JOB_CPU_HOURS = "cpu_hours"
FLD_JOB_OUTPUTS = "outputs"
FLD_JOB_OUTPUT_FILE_COUNT = "output_file_count"
FLD_JOB_LOGPATH = "logpath"
FLD_REFDATA_FILE = "file"
FLD_REFDATA_STATUSES = "statuses"
FLD_REFDATA_CLUSTER = "cluster"
FLD_REFDATA_NERSC_DL_TASK_ID = "nersc_download_task_id"
# Fields that are shared between multiple models for consistency
# Currently refdata & jobs
FLD_COMMON_ID = "id"
FLD_COMMON_STATE = "state"
FLD_COMMON_TRANS_TIMES = "transition_times"
FLD_COMMON_ERROR = "error"
FLD_COMMON_ADMIN_ERROR = "admin_error"
FLD_COMMON_TRACEBACK = "traceback"
FLD_COMMON_STATE_TRANSITION_STATE = "state"
FLD_COMMON_STATE_TRANSITION_TIME = "time"

S3_PATH_MIN_LENGTH = 3 + 1 + 1  # 3 for bucket + / + 1 char
S3_PATH_MAX_LENGTH = 63 + 1 + 1024  # 63 for bucket + / + 1024 bytes
CRC64NVME_B64ENC_LENGTH=12


# https://en.wikipedia.org/wiki/Filename#Comparison_of_filename_limitations
# POSIX fully portable filenames
ABSOLUTE_PATH_REGEX = r"^/(?:[\w.-]+/)*[\w.-]+/?$"


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
    pos = _contains_control_characters(s, allowed_chars=allowed_chars)
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


ArgumentString = Annotated[str, StringConstraints(
    min_length=1,
    max_length=1024,
    # open this up as needed, careful to not allow dangerous chars. Do not allow spaces
    pattern=r"^[\w\./][\w\./-]*$"
)]


Flag = Annotated[str, StringConstraints(
    min_length=2,
    max_length=256,
    # open this up as needed, careful to not allow dangerous chars. Do not allow spaces
    pattern=r"^--?[a-zA-Z0-9][\w\.-]*=?$"
)]


EnvironmentVariable = Annotated[str, StringConstraints(
    min_length=1,
    max_length=256,
    pattern=r"^[A-Z_][A-Z0-9_]*$"  # https://stackoverflow.com/a/2821183/643675
)]


class Parameter(BaseModel):
    """
    Represents a special parameter passed to a container.
    
    Can represent a set of input file names formatted in various ways, a manifest file
    containing file paths or data IDs, or a container number.
    
    input_files_format is required when the type is input_files, but ignored otherwise.  
    manifest_file_format is required when the type is manifest_file, but ignored otherwise.  
    manifest_file_header is optional and only taken into account when the type is manifest_file.  
    """
    # The Parameter and ParameterWithFlag pydoc is almost identical, be sure to edit in sync
    model_config = ConfigDict(extra='forbid')
    
    type: Annotated[ParameterType, Field(examples=[ParameterType.INPUT_FILES])]
    input_files_format: Annotated[InputFilesFormat | None, Field(
        examples=[InputFilesFormat.COMMA_SEPARATED_LIST],
    )] = None
    manifest_file_format: Annotated[ManifestFileFormat | None, Field(
        examples=[ManifestFileFormat.FILES],
    )] = None
    manifest_file_header: Annotated[str | None, Field(
        examples=["genome_id"],
        description="The header for the manifest file, if any.",
        min_length=1,
        max_length=1000,
    )] = None
    container_num_prefix: Annotated[ArgumentString | None, Field(
        examples=["/output/job-"],
        description="A string that will be prepended to the container number."
    )] = None
    container_num_suffix: Annotated[ArgumentString | None, Field(
        examples=["_output"],
        description="A string that will be appended to the container number."
    )] = None
    
    def get_flag(self):
        """
        Get any command line flag associated with this parameter.
        """
        return None
    
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


class ParameterWithFlag(Parameter):
    """
    Represents a special parameter passed to a container, optionally with a
    preceding flag argument.
    
    Can represent a set of input file names formatted in various ways, a manifest file
    containing file paths or data IDs, or a container number.
    
    input_files_format is required when the type is input_files, but ignored otherwise.  
    manifest_file_format is required when the type is manifest_file, but ignored otherwise.  
    manifest_file_header is optional and only taken into account when the type is manifest_file.  
    """
    # The Parameter and ParameterWithFlag pydoc is almost identical, be sure to edit in sync
    model_config = ConfigDict(extra='forbid')
    flag: Annotated[Flag | None, Field(
        examples=["--file"],
        description="A command line argument flag to precede the parameter. "
            + "If the flag ends with '=', no space is left between the flag and its value.",
    )] = None
    
    
    def get_flag(self):
        """
        Get any command line flag associated with this parameter.
        """
        return self.flag
    
    @model_validator(mode="after")
    def _check_fields(self) -> Self:
        if (self.type == ParameterType.INPUT_FILES
            and self.input_files_format == InputFilesFormat.REPEAT_PARAMETER
            and not self.flag
        ):
            raise ValueError("If the repeat parameter input files format is specified,"
                             + " a flag must be provided."
            )
        return self


class Parameters(BaseModel):
    """
    A set of parameters for a container in a job.
    
    Specifying files as part of arguments to a job can be done in three ways:
    
    * If the entrypoint command takes a directory as an argument, that
      directory can be specified literally in an argument. All files will be available in the
      input mount point, with paths resulting from the path in S3 as well as the input_roots
      parameter from the job input.
    * If individual filenames need to be specified, use a Parameter instance in
      the positional or environmental arguments. At most one Parameter can
      be specified per parameter set.
    * A Parameter instance can also be used to specify a manifest of file names or
      file IDs to be passed to the container.
    """
    model_config = ConfigDict(extra='forbid')
    
    input_mount_point: Annotated[str, Field(
        examples=["/input_files"],
        default="/input_files",
        description="Where to place input files in the container. "
            + "Must start from the container root and include at least one directory "
            + "when resolved.",
        min_length=1,
        max_length=1024,
        pattern=ABSOLUTE_PATH_REGEX,
    )] = "/input_files"
    output_mount_point: Annotated[str, Field(
        examples=["/output_files"],
        default="/output_files",
        description="Where output files should be written in the container. "
            + "Must start from the container root and include at least one directory "
            + "when resolved.",
        min_length=1,
        max_length=1024,
        pattern=ABSOLUTE_PATH_REGEX,
    )] = "/output_files"
    refdata_mount_point: Annotated[str | None, Field(
        examples=["/reference_data"],
        description="Where reference data files should be pleased in the container. "
            + "Must start from the container root and include at least one directory "
            + "when resolved. Required if the job image requires reference data and a default "
            + "reference data mount point isn't specified for the image. If a default is "
            + "provided, this setting overrides it.",
        min_length=1,
        max_length=1024,
        pattern=ABSOLUTE_PATH_REGEX,
    )] = None
    args: Annotated[list[ArgumentString | Flag | ParameterWithFlag] | None, Field(
        examples=[[
            "process",
            "--output-dir", "/output_files",
            {
                "type": ParameterType.INPUT_FILES.value,
                "input_files_format": InputFilesFormat.REPEAT_PARAMETER.value,
                "flag": "--input-file=",
            },
            "--jobid",
            {
                "type": ParameterType.CONTAINTER_NUMBER,
            },
        ]],
        description="A list of command line arguments to be inserted after the container "
            + "entrypoint command. Strings are treated as literals."
    )] = None
    environment: Annotated[dict[EnvironmentVariable, ArgumentString | Parameter] | None, Field(
        examples=[{
            "DIAMOND_DB_PATH": "/reference_data",
            "FILE_MANIFEST": {
                "type": ParameterType.MANIFEST_FILE.value,
                "manifest_file_format": ManifestFileFormat.FILES.value,
                "manifest_file_header": "infiles",
            },
        }],
        description="A dictionary of environment variables to be inserted into the container. "
            + "String values are treated as literals."
    )] = None
    
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
        if self.args:
            for p in self.args:
                param = self._check_parameter(param, p)
        if self.environment:
            for key, p in self.environment.items():
                param = self._check_parameter(
                    param, p, f"Environmental parameter at key {key}")
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
                # Space separated lists in environment variables don't really make sense.
                # Come back to this if needed.
                if p.input_files_format is InputFilesFormat.SPACE_SEPARATED_LIST:
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
    model_config = ConfigDict(extra='forbid', frozen=True)
    
    file: Annotated[str, Field(
        examples=["mybucket/foo/bar/baz.jpg"],
        description="A path to an object in an S3 instance, starting with the bucket. "
            + "Please note that spaces are valid S3 object characters, so be careful about "
            + "trailing spaces in the input.",
        min_length=S3_PATH_MIN_LENGTH,
        max_length=S3_PATH_MAX_LENGTH,
    )]
    crc64nvme: Annotated[str | None, Field(
        examples=["4ekt2WB1KO4="],
        description="The base64 encoded CRC64/NVME checksum of the file. "
            + "If provided on input it is checked against the "
            + "target file checksum before proceeding. Always provided with output.",
        min_length=CRC64NVME_B64ENC_LENGTH,
        max_length=CRC64NVME_B64ENC_LENGTH,
    )] = None
    
    @field_validator("file", mode="before")
    @classmethod
    def _check_file(cls, v):
        return _validate_s3_path(v)


class S3FileWithDataID(S3File):
    """ A file in an S3 instance, optionally with a data ID. """
    model_config = ConfigDict(extra='forbid', frozen=True)
    
    data_id: Annotated[str | None, Field(
        examples=["GCA_000146795.3"],
        description="An arbitrary string representing the ID of the data in the file.",
        min_length=1,
        max_length=255,
    )] = None

    @field_validator("data_id", mode="after")
    @classmethod
    def _check_data_id(cls, v):
        return _err_on_control_chars(v)
    
# TODO FEATURE How to handle all vs all? Current model is splitting file list between containers


class JobInputPreview(BaseModel):
    """
    Input to a Job, consisting of fields containing small amounts of data.
    Suitable for a list of jobs.
    """
    model_config = ConfigDict(extra='forbid')
    
    # In the design there's a field for authentication source. This seems so unlikely to ever
    # be implemented we're leaving it out here. If it is implemented kbase should be the default.
    
    # Similarly, the design allows for multiple S3 instances. We'll implement with a default
    # when actually needed.
    
    cluster: Annotated[sites.Cluster, Field(examples=[sites.Cluster.PERLMUTTER_JAWS.value])]
    image: Annotated[str, Field(
        examples=["ghcr.io/kbase/collections:checkm2_0.1.6"
            + "@sha256:c9291c94c382b88975184203100d119cba865c1be91b1c5891749ee02193d380"],
        description="The Docker image to run for the job. Include the SHA to ensure the "
            + "exact code requested is run.",
        # Don't bother validating other than some basic checks, validation will occur when
        # checking / getting the image SHA from the remote repository
        min_length=1,
        max_length=1000
    )]
    params: Parameters
    num_containers: Annotated[int, Field(
        examples=[1],
        default=1,
        description="The number of containers to run in parallel. Input files will be split "
            + "between the containers. If there are more containers than input files the "
            + "container count will be reduced appropriately.",
        ge=1,
        le=1000,
    )] = 1
    cpus: Annotated[int, Field(
        examples=[1],
        default=1,
        description="The number of CPUs to allocate per container.",
        ge=1,
        le=sites.MAX_CPUS,
    )] = 1
    memory: Annotated[ByteSize, Field(
        examples=["10MB"],
        default="10MB",
        description="The amount of memory to allocate per container either as the number of "
            + "bytes or a specification string such as 100MB, 2GB, etc.",
        ge=1 * 1000 * 1000,
        le=sites.MAX_MEM_GB * 1000 * 1000 * 1000
    )] = 10 * 1000 * 1000
    runtime: Annotated[datetime.timedelta, Field(
        examples=["PT12H30M5S", 12 * 3600 + 30 * 60 + 5],
        default = "PT60S",
        description="The runtime required for each container as the number of seconds or an "
            + "ISO8601 duration string.",
        ge=datetime.timedelta(seconds=1),
        le=datetime.timedelta(minutes=sites.MAX_RUNTIME_MIN),
    )] = datetime.timedelta(seconds=60)
    input_roots: Annotated[list[str] | None, Field(
        examples=[["mybucket/foo/bar"]],
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
        examples=["mybucket/out"],
        description="The S3 folder, starting with the bucket, in which to place results.",
        min_length=3 + 1 + 1,
        max_length=63 + 1 + 1024
    )]
    
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
        return _validate_s3_path(v).rstrip("/") + "/"
    
    def get_total_compute_time_sec(self) -> float:
        """
        Return the total compute time as seconds for the job, calculated as
        cpus * containers * runtime.
        """ 
        return self.cpus * self.num_containers * self.runtime.total_seconds()


class JobInput(JobInputPreview):
    """
    Input to a Job.
    """
    model_config = ConfigDict(extra='forbid')

    input_files: Annotated[list[str] | list[S3FileWithDataID], Field(
        examples=[
            [{
                "file": "mybucket/foo/bat",
                "data_id": "GCA_000146795.3",
                "crc64name": "4ekt2WB1KO4=",
            }],
            ["mybucket/foo/bat"]
        ],
        description="The S3 input files for the job, either a list of file paths as strings or a "
            + "list of data structures including the file path and optionally a data ID and / or "
            + "CRC64/NVME checksum. The file paths always start with the bucket. "
            + "All S3 input files are expected to have a CRC64/NVME checksum available, even "
            + "if it is not provided in the input data structure. "
            + "Either all or no files must have data IDs associated with them. "
            + "When returned from the service, the checksum is always included.",
        min_length=1,
        # Need to see how well this performs. If we need more files per job,
        # can test performance & tweak S3 client concurrency. Alternatively, add a file set
        # endpoint where file sets of size < some reasonable number can be submitted, and then
        # multiple file sets can be combined in a JobInput
        max_length=10000,
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
        if not isinstance(v[0], S3FileWithDataID):
            return v
        data_ids = bool(v[0].data_id)
        for f in v:
            if bool(f.data_id) is not data_ids:
                raise ValueError("Either all or no files must have data IDs")
        return v
    
    @model_validator(mode="after")
    def _check_manifest_data_ids(self) -> Self:
        fp = self.params.get_file_parameter()
        # This seems really sketchy. Not sure how else to do it though
        if (fp
            # manifest file format is ignored, but not necc None, if type is not manifest file
            and fp.type is ParameterType.MANIFEST_FILE
            and fp.manifest_file_format is ManifestFileFormat.DATA_IDS
            and (
                not isinstance(self.input_files[0], S3FileWithDataID)
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
        Returns True if the inputfiles are of type S3FileWithDataID, False otherwise.
        """
        return isinstance(self.input_files[0], S3FileWithDataID)
        
    def get_files_per_container(self) -> list[list[S3FileWithDataID | str]]:
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


class RegistrationInfo(BaseModel):
    """
    Information about when and by whom something was registered.
    """
    # Output only model, no validation
    registered_by: Annotated[str, Field(
        examples=["aparkin"],
        description="The username of the user that performed the registration."
    )]
    registered_on: Annotated[datetime.datetime, Field(
        examples=["2024-10-24T22:35:40Z"],
        description="The time of registration."
    )]


class JobImage(RegistrationInfo):
    """
    Information about a Docker image as stored in a Job data structure.
    """
    # This is an outgoing data structure only so we don't add validators
    name: Annotated[str, Field(
        examples=["ghcr.io/kbase/collections"],
        description="The normalized name of the a docker image, consisting of the "
            + "host and path.",
    )]
    digest: Annotated[str, Field(
        examples=["sha256:c9291c94c382b88975184203100d119cba865c1be91b1c5891749ee02193d380"],
        description="The image digest.",
    )]
    entrypoint: Annotated[list[str], Field(
        examples=[["checkm2", "predict"]],
        description="The entrypoint command extracted from the image."
    )]
    tag: Annotated[str | None, Field(
        examples=["checkm2_0.1.6"],
        description="The image tag at registration time. "
            + "The tag may no longer point to the same image."
    )] = None
    refdata_id: Annotated[str | None, Field(
        examples=["3a28c155-ea8b-4e1b-baef-242d991a8200"],
        description="The ID of reference data associated with the image. The image requires "
            + "this reference data to run, and it will be mounted into the image container at "
            + "the refdata mount point."
    )] = None
    default_refdata_mount_point: Annotated[str | None, Field(
        examples=["/reference_data"],
        description="The default mount point for the image refdata in the container. Overridden "
            + "by the refdata mount point in a job submission if provided there. If a default "
            + "mount point is provided, then providing a mount point in the job submission "
            + "is optional. Must be an absolute path.",
    )] = None
    
    @property
    def name_with_digest(self):
        """
        Returns the normalized name with the digest, e.g. name@digest.
        
        This form always refers to the same image and is compatible
        with Docker, Shifter, and Apptainer.
        """ 
        return f"{self.name}@{self.digest}"


class ImageUsage(BaseModel):
    """
    Usage information for a docker image.
    """
    urls: Annotated[conlist(HttpUrl, max_length=20) | None, Field(
        examples=[["https://github.com/chklovski/CheckM2"]],
        description="Documentation urls for the image."
    )] = None
    usage_notes: Annotated[str | None, Field(
        examples=["Note that the entrypoint includes the 'predict' subcommand, so it is "
            + "unnecessary to include it in the arguments supplied to the job"
        ],
        description="An arbitrary string containing usage notes for users. Markdown is "
            + "suggested but not required.",
        max_length=10000,
    )] = None


class Image(ImageUsage, JobImage):
    """
    Information about a docker image.
    """


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


JOB_TERMINAL_STATES = {JobState.COMPLETE, JobState.ERROR}  # canceled at some point


class JobStateTransition(BaseModel):
    """
    Denotes the new state and the entry time when a state change occurs.
    """
    # outgoing only, no validators
    state: JobState
    time: Annotated[datetime.datetime, Field(
        examples=["2024-10-24T22:35:40Z"],
        description="The time at which the new state was entered."
    )]


class JobStatus(BaseModel):
    """
    Minimal information about a job to determine the current state of the job, e.g. whether it's
    complete, etc.
    """
    # This is an outgoing data structure only so we don't add validators
    id: Annotated[str, Field(
        description="An opaque, unique string that serves as the job's ID.",
    )]
    user: Annotated[str, Field(examples=["myuserid"], description="The user that ran the job.")]
    admin_meta: Annotated[dict[str, str | int | float], Field(
        default_factory=dict,
        examples=[{"name": "Getrude", "children": 31619}],
        description="Metadata for the job set by service administrators. This metadata is mutable "
            + "and arbitrary from the point of view of the service."
    )]
    state: Annotated[JobState, Field(examples=[JobState.COMPLETE.value])]
    transition_times: Annotated[list[JobStateTransition], Field(
        examples=[[
            {"state": JobState.CREATED.value, "time": "2024-10-24T22:35:40Z"},
            {"state": JobState.UPLOAD_SUBMITTED.value, "time": "2024-10-24T22:35:41Z"},
            {"state": JobState.JOB_SUBMITTING, "time": "2024-10-24T22:47:67Z"},
        ]],
        description="A list of job state transitions."
    )]


class JobPreview(JobStatus):
    """
    Information about a job, consisting of fields containing small amounts of data.
    Suitable for a list of jobs.
    """
    # This is an outgoing data structure only so we don't add validators
    job_input: JobInputPreview
    image: JobImage
    input_file_count: Annotated[int, Field(
        examples=[42],
        description="The number of input files."
    )]
    # This is different from the job_input field, which takes a iso8601 time delta,
    # but I think the inconsistency is warranted. You want to make it easy for people to input
    # and understand a wide range of times but have a consistent output in a fairly standard
    # unit.
    cpu_hours: Annotated[float | None, Field(
        examples=[52.4],
        description="The total CPU hours used by the job, if available.")
    ] = None
    output_file_count: Annotated[int | None, Field(
        examples=[24],
        description="The number of output files, if available."
    )] = None
    error: Annotated[str | None, Field(
        examples=["The front fell off"],
        description="A description of the error that occurred."
    )] = None
    logpath: Annotated[str | None, Field(
        examples=["cts-logs/container_logs/e14a21ba-032d-42f2-b235-d82606675b17"],
        description="A location in S3 where the logfiles for the job containers can be viewed."
    )] = None
    
    def get_refdata_mount_point(self) -> str | None:
        """
        Get the in-container mount point for reference data or None if there is no reference
        data for this job.
        """
        return self.job_input.params.refdata_mount_point or self.image.default_refdata_mount_point


class Job(JobPreview):
    """
    Information about a job.
    """
    # This is an outgoing data structure only so we don't add validators
    job_input: JobInput
    # May need to assemble jobs manually if path validation is too expensive.
    outputs: list[S3File] | None = None


class NERSCDetails(BaseModel):
    """
    Details about a job run at NERSC.
    """
    # Output only model, no validation
    download_task_id: Annotated[list[str], Field(
        description="IDs for tasks run via the NERSC SFAPI to download files from an S3 "
            + "instance to NERSC. Note that task details only persist for ~10 minutes past "
            + "completion in the SFAPI. Multiple tasks indicate job retries after failures."
    )]
    upload_task_id: Annotated[list[str], Field(
        default_factory=list,
        description="IDs for tasks run via the NERSC SFAPI to upload files to an S3 "
            + "instance from NERSC. Note that task details only persist for ~10 minutes past "
            + "completion in the SFAPI. Multiple tasks indicate job retries after failures."
            + "Empty if an upload task has not yet been submitted to NERSC."
    )]
    log_upload_task_id: Annotated[list[str], Field(
        default_factory=list,
        description="IDs for tasks run via the NERSC SFAPI to upload log files to an S3 "
            + "instance from NERSC. Note that task details only persist for ~10 minutes past "
            + "completion in the SFAPI. Multiple tasks indicate job retries after failures."
            + "Empty if a log upload task has not yet been submitted to NERSC."
    )]


class JAWSDetails(BaseModel):
    """
    Details about a JAWS job run.
    """
    # Output only model, no validation
    run_id: Annotated[list[str], Field(
        description="Run IDs for a JGI JAWS job. Multiple run IDs indicate job retries after "
            + "failures.")]


class AdminJobStateTransition(JobStateTransition):
    
    trans_id: Annotated[str, Field(
        description="An opaque, unique ID identifying this state transition"
    )]
    # Only allows for one notification system... YAGNI
    notif_sent: Annotated[bool, Field(
        description="Whether an update has been sent to the notification system for "
        + "this state transition."
    )]

class AdminJobDetails(Job):
    """
    Information about a job with added details of interest to service administrators.
    """
    # Output only model, no validation
    transition_times: Annotated[list[AdminJobStateTransition], Field(
        examples=[[
            {
                "state": JobState.CREATED.value,
                "time": "2024-10-24T22:35:40Z",
                "trans_id": "foo",
                "notif_sent": True,
            },
            {
                "state": JobState.UPLOAD_SUBMITTED.value,
                "time": "2024-10-24T22:35:41Z",
                "trans_id": "bar",
                "notif_sent": True,
            },
            {
                "state": JobState.JOB_SUBMITTING,
                "time": "2024-10-24T22:47:67Z",
                "trans_id": "baz",
                "notif_sent": False, 
            },
        ]],
        description="A list of job state transitions."
    )]
    nersc_details: NERSCDetails | None = None
    jaws_details: JAWSDetails | None = None
    admin_error: Annotated[str | None, Field(
        examples=["The back fell off"],
        description="A description of the error that occurred oriented towards service "
            + "admins, potentially including more details."
    )] = None
    traceback: Annotated[str | None, Field(description="The error's traceback.")] = None


class ReferenceDataState(str, Enum):
    """
    The state of a reference data staging process.
    """
    CREATED = "created"
    DOWNLOAD_SUBMITTED = "download_submitted"
    COMPLETE = "complete"
    ERROR = "error"


REFDATA_TERMINAL_STATES = {ReferenceDataState.COMPLETE, ReferenceDataState.ERROR}


class RefDataStateTransition(BaseModel):
    """
    Denotes the new state and the entry time when a state change occurs.
    """
    # outgoing only, no validators
    state: ReferenceDataState
    time: Annotated[datetime.datetime, Field(
        examples=["2024-10-24T22:35:40Z"],
        description="The time at which the new state was entered."
    )]


class ReferenceDataStatus(BaseModel):
    """
    Status of a reference data staging process at a particular remote compute location.
    """
    # This is an outgoing structure only so we don't add validators
    cluster: Annotated[sites.Cluster, Field(examples=[sites.Cluster.PERLMUTTER_JAWS.value])]
    state: Annotated[ReferenceDataState, Field(
        examples=[ReferenceDataState.COMPLETE.value],
        description="The state of the reference data staging process."
    )]
    transition_times: Annotated[list[RefDataStateTransition], Field(
            examples=[[
                {"state": ReferenceDataState.CREATED.value, "time": "2024-10-24T22:35:40Z"},
                {
                    "state": ReferenceDataState.DOWNLOAD_SUBMITTED.value,
                    "time": "2024-10-24T22:35:41Z"
                },
            ]],
            description="A list refdata state transitions."
        )
    ]
    error: Annotated[str | None, Field(
        examples=["The front fell off"],
        description="A description of the error that occurred."
    )] = None


class AdminReferenceDataStatus(ReferenceDataStatus):
    """
    Information about reference data status with added details of interest to service
    administrators.
    """
    # This is an outgoing structure only so we don't add validators
    nersc_download_task_id: Annotated[list[str] | None, Field(
        description="IDs for tasks run via the NERSC SFAPI to download files from an S3 "
            + "instance to NERSC. Note that task details only persist for ~10 minutes past "
            + "completion in the SFAPI. Multiple tasks indicate job retries after failures. "
            + "Only present if the refdata is being downloaded to NERSC."
    )] = None
    admin_error: Annotated[str | None, Field(
        examples=["The back fell off"],
        description="A description of the error that occurred oriented towards service "
            + "admins, potentially including more details."
    )] = None
    traceback: Annotated[str | None, Field(description="The error's traceback.")] = None


class _ReferenceDataRoot(S3File, RegistrationInfo):
    # This is an outgoing structure only so we don't add validators
    id: Annotated[str, Field(
        description="An opaque, unique string that serves as the reference data's ID.",
    )]
    # TODO REFDATA will need a force toggle if file is overwritten after reg
    unpack: Annotated[bool, Field(
        description="Whether to unpack the file after download. *.tar.gz, *.tgz, and *.gz "
            + "files are supported."
    )] = False
    
    def get_status_for_cluster(self, cluster: sites.Cluster) -> ReferenceDataStatus:
        """
        Get the status of a cluster for this reference data.
        """ 
        _not_falsy(cluster, "cluster")
        for s in self.statuses:
            if s.cluster == cluster:
                return s
        raise InvalidReferenceDataStateError(
            f"No status for cluster {cluster.value} for reference data {self.id}"
        )


class ReferenceData(_ReferenceDataRoot):
    """
    Information about reference data that is, or will be, available for containers.
    """
    # This is an outgoing structure only so we don't add validators
    # TODO FUTURESITES will need to be able to stage to sites that don't exist currently
    #                  ... if that ever happens
    statuses: Annotated[list[ReferenceDataStatus], Field(
        description="The states of reference data staging processes at remote compute locations.",
    )]


class AdminReferenceData(_ReferenceDataRoot):
    """
    Information about reference data that is, or will be, available for containers.
    """
    # This is an outgoing structure only so we don't add validators
    # TODO FUTURESITES will need to be able to stage to sites that don't exist currently
    #                  ... if that ever happens
    statuses: Annotated[list[AdminReferenceDataStatus], Field(
        description="The states of reference data staging processes at remote compute locations.",
    )]
