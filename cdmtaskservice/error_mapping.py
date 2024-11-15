"""
Map errors from exception type to custom error type and HTTP status. 
"""

from fastapi import status
from typing import NamedTuple

from cdmtaskservice.errors import ErrorType
from cdmtaskservice.http_bearer import MissingTokenError, InvalidAuthHeaderError
from cdmtaskservice.kb_auth import InvalidTokenError, MissingRoleError
from cdmtaskservice.routes import UnauthorizedError


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
