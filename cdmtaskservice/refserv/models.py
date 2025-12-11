""" Models for use exclusively with the refdata server. """


from enum import Enum
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
)
from typing import Annotated


class RefdataLocalState(str, Enum):
    """
    The local state of a reference data staging process.
    
    This is hackily based on the state of the local file system.
    
    none - there's no directory for the reference data present.  
    staging - there is a directory present but no metadata file.  
    done - there is a metadata file present.  
    """
    NONE = "none"
    STAGING = "staging"
    DONE = "done"


class RefdataFile(BaseModel):
    """ Information about a file that is included in reference data. """
    # This is an outgoing structure only so we don't add validators
    
    # NOTE: these keys match the json file keys in refdata_manager.py
    file: Annotated[str, Field(
        examples=["refdata_file_1.txt"],
        description="A file that is part of a set of reference data."
    )]
    md5: Annotated[str, Field(
        examples=["80ddd6a8998e5ebbc79ebbaeaee667f7"],
        description="The file's MD5 checksum"
    )]
    size: Annotated[int, Field(
        examples=[6789154],
        description="The file's size in bytes."
    )]


class RefdataLocalStatus(BaseModel):
    """ The local status of the reference data state. """
        # This is an outgoing structure only so we don't add validators
    
    state: Annotated[RefdataLocalState, Field(
        examples=[RefdataLocalState.STAGING.value],
    )]
    files: Annotated[list[RefdataFile] | None, Field(
        description="Information about the files in the reference data if available."
    )] = None
