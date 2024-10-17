"""
Pydantic models for the CTS.
"""

from enum import Enum
from pathlib import Path
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Annotated, Self

from cdmtaskservice.arg_checkers import contains_control_characters


# TODO TEST
# TODO EXAMPLES try using examples instead of the deprecated example. Last time I tried no joy

# https://en.wikipedia.org/wiki/Filename#Comparison_of_filename_limitations
# POSiX fully portable filenames and /
_PATH_REGEX=r"^[\w.-/]+$"


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
    
    @field_validator("manifest_file_header", mode="before")
    @classmethod
    def _validate_manifest_file_header(cls, v):
        if v is None:
            return None
        pos = contains_control_characters(v, allowed_chars=["\t"])
        if pos > -1: 
            raise ValueError(f"contains a non tab control character at position {pos}")
        return v

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


class Parameters(BaseModel):
    """ A set of parameters for a job. """
    
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
    positional_args: Annotated[list[str | Parameter] | None, Field(
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
    flag_args: Annotated[dict[str, str | Parameter] | None, Field(
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
    environment: Annotated[dict[str, str | Parameter] | None, Field(
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
        mode="before",
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
    
    @field_validator("positional_args", mode="before")
    @classmethod
    def _check_pos_args(cls, v):
        if v is None:
            return None
        for i, val in enumerate(v):
            cls._check_val(val, f"string at index {i}")
        return v

    @field_validator("flag_args", "environment", mode="before")
    @classmethod
    def _check_key_args(cls, v):
        if v is None:
            return None
        for key, val in v.items():
            cls._check_val(key, "key")
            cls._check_val(val, f"value for key {key}")
        return v

    @classmethod
    def _check_val(cls, v, name):
        if isinstance(v, str) and len(v) > 1000:
            raise ValueError(f"{name} is longer than 1000 characters")
