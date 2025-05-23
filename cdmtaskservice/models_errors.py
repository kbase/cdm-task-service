"""
Pydantic models for service error structures.
"""

import datetime
from pydantic import BaseModel, Field
from typing import Annotated, Optional

from cdmtaskservice.errors import ErrorType


class ServerErrorDetail(BaseModel):
    httpcode: int = Field(examples=[500], description="The HTTP error code")
    httpstatus: str = Field(examples=["INTERNAL SERVER ERROR"], description="The HTTP status string")
    time: Annotated[datetime.datetime, Field(
        examples=["2022-10-07T17:58:53.188698Z"],
        description="The server's time as an ISO8601 string.",
    )]
    request_id: Annotated[str | None, Field(
        examples=["172367b0-0099-4903-9a19-d12ff101b2da"],
        description="The ID of the request, if available."
    )] = None
    message: Optional[str] = Field(
        examples=["Well dang, that ain't good"],
        description="A free text string providing more information about the error"
    )

class RequestValidationDetail(BaseModel):
    # Structure from https://github.com/tiangolo/fastapi/blob/f67b19f0f73ebdca01775b8c7e531e51b9cecfae/fastapi/openapi/utils.py#L34-L59
    # Note I have witnessed other fields in the response as well, which apparently aren't
    # included in the spec
    loc: list[str | int] = Field(
        examples=[["body", "data_products", 2, "version"]],
        description="The location where the validation error occurred"
    )
    msg: str = Field(
        examples=["ensure this value has at most 20 characters"],
        description="A free text message explaining the validation problem"
    )
    type: str = Field(
        examples=["value_error.any_str.max_length"],
        description="The type of the validation error"
    )


class ClientErrorDetail(ServerErrorDetail):
    httpcode: int = Field(examples=[400], description="The HTTP error code")
    httpstatus: str = Field(examples=["BAD REQUEST"], description="The HTTP status string")
    appcode: Optional[int] = Field(
        examples=[30010],
        description="An application code providing more specific information about an error, "
            + "if available"
    )
    apperror: Optional[str] = Field(
        examples=["Request validation failed"],
        description="The error string for the application error code. If the error code is "
            + "available, the string is always available"
    )
    request_validation_detail: Optional[list[RequestValidationDetail]] = Field(
        description=
            "Information about why a request failed to pass the FastAPI validation system. "
            + f'Included when the app error is "{ErrorType.REQUEST_VALIDATION_FAILED.error_type}".'  # @UndefinedVariable
    )


class ServerError(BaseModel):
    """ An server error uncaused by the client. """
    error: ServerErrorDetail


class ClientError(BaseModel):
    """ An error caused by a bad client request. """
    error: ClientErrorDetail
