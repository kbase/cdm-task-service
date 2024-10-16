"""
Pydantic models for the CTS.
"""

from enum import Enum
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Annotated, Self

from cdmtaskservice.arg_checkers import contains_control_characters


# TODO TEST


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
    The input files will be inserted as a space separated list, which each file preceded
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
