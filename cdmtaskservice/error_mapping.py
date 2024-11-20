"""
Map errors from exception type to custom error type and HTTP status. 
"""

from fastapi import status
from typing import NamedTuple

from cdmtaskservice.errors import ErrorType
from cdmtaskservice.http_bearer import MissingTokenError, InvalidAuthHeaderError
from cdmtaskservice.job_state import ETagMismatchError
from cdmtaskservice.kb_auth import InvalidTokenError, MissingRoleError
from cdmtaskservice.routes import UnauthorizedError, ClientLifeTimeError
from cdmtaskservice.s3.client import (
    S3PathInaccessibleError,
    S3PathNotFoundError,
)
from cdmtaskservice.s3.paths import S3PathSyntaxError


class ErrorMapping(NamedTuple):
    """ The application error type and HTTP status code for an exception. """
    err_type: ErrorType | None
    """ The type of application error. None if a 5XX error or Not Found."""
    http_code: int
    """ The HTTP code of the error. """


_ERR_MAP = {
    InvalidTokenError: ErrorMapping(ErrorType.INVALID_TOKEN, status.HTTP_401_UNAUTHORIZED),
    MissingRoleError: ErrorMapping(ErrorType.UNAUTHORIZED, status.HTTP_403_FORBIDDEN),
    MissingTokenError: ErrorMapping(ErrorType.NO_TOKEN, status.HTTP_401_UNAUTHORIZED),
    InvalidAuthHeaderError: ErrorMapping(
        ErrorType.INVALID_AUTH_HEADER, status.HTTP_401_UNAUTHORIZED
    ),
    UnauthorizedError: ErrorMapping(ErrorType.UNAUTHORIZED, status.HTTP_403_FORBIDDEN),
    ClientLifeTimeError: ErrorMapping(ErrorType.CLIENT_LIFETIME, status.HTTP_400_BAD_REQUEST),
    S3PathInaccessibleError: ErrorMapping(
        ErrorType.S3_PATH_INACCESSIBLE, status.HTTP_403_FORBIDDEN
    ),
    S3PathNotFoundError: ErrorMapping(ErrorType.S3_PATH_NOT_FOUND, status.HTTP_404_NOT_FOUND),
    S3PathSyntaxError: ErrorMapping(ErrorType.S3_PATH_SYNTAX, status.HTTP_400_BAD_REQUEST),
    ETagMismatchError: ErrorMapping(ErrorType.S3_ETAG_MISMATCH, status.HTTP_400_BAD_REQUEST),
}

def map_error(err: Exception) -> tuple[ErrorType, int]:
    """
    Map an error to an optional error type and a HTTP code.
    """
    # May need to add code to go up the error hierarchy if multiple errors have the same type
    ret = _ERR_MAP.get(type(err))
    if not ret:
        ret = ErrorMapping(None, status.HTTP_500_INTERNAL_SERVER_ERROR)
    return ret
