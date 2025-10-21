"""
Map errors from exception type to custom error type and HTTP status. 
"""

from fastapi import status
from kbase.auth import InvalidTokenError, InvalidUserError
from typing import NamedTuple

from cdmtaskservice.errors import ErrorType
from cdmtaskservice.exceptions import (
    ChecksumMismatchError,
    IllegalParameterError,
    InvalidAuthHeaderError,
    InvalidJobStateError,
    InvalidReferenceDataStateError,
    InvalidUserError,
    UnauthorizedError,
    UnavailableResourceError,
    UnsupportedOperationError,
)
from cdmtaskservice.http_bearer import MissingTokenError
from cdmtaskservice.images import NoEntrypointError
from cdmtaskservice.image_remote_lookup import ImageNameParseError, ImageInfoFetchError
from cdmtaskservice.jobflows.flowmanager import InactiveJobFlowError, UnavailableJobFlowError
from cdmtaskservice.mongo import (
    ImageTagExistsError,
    ImageDigestExistsError,
    NoSuchImageError,
    NoSuchJobError,
    NoSuchReferenceDataError,
    NoSuchSubJobError,
    ReferenceDataExistsError,
)
from cdmtaskservice.routes import ClientLifeTimeError
from cdmtaskservice.s3.client import (
    S3BucketInaccessibleError,
    S3BucketNotFoundError,
    S3PathInaccessibleError,
    S3PathNotFoundError,
)
from cdmtaskservice.s3.paths import S3PathSyntaxError

_H400 = status.HTTP_400_BAD_REQUEST
_H401 = status.HTTP_401_UNAUTHORIZED
_H403 = status.HTTP_403_FORBIDDEN
_H404 = status.HTTP_404_NOT_FOUND


class ErrorMapping(NamedTuple):
    """ The application error type and HTTP status code for an exception. """
    err_type: ErrorType | None
    """ The type of application error. None if a 5XX error or Not Found based on the url."""
    http_code: int
    """ The HTTP code of the error. """


_ERR_MAP = {
    MissingTokenError: ErrorMapping(ErrorType.NO_TOKEN, _H401),
    InvalidAuthHeaderError: ErrorMapping(ErrorType.INVALID_AUTH_HEADER, _H401),
    InvalidTokenError: ErrorMapping(ErrorType.INVALID_TOKEN, _H401),
    InvalidUserError: ErrorMapping(ErrorType.INVALID_USERNAME, _H400),
    InvalidUserError: ErrorMapping(ErrorType.INVALID_USERNAME, _H400),
    UnauthorizedError: ErrorMapping(ErrorType.UNAUTHORIZED, _H403),
    ClientLifeTimeError: ErrorMapping(ErrorType.CLIENT_LIFETIME, _H400),
    S3BucketInaccessibleError: ErrorMapping(ErrorType.S3_BUCKET_INACCESSIBLE, _H403),
    S3BucketNotFoundError: ErrorMapping(ErrorType.S3_BUCKET_NOT_FOUND, _H404),
    S3PathInaccessibleError: ErrorMapping(ErrorType.S3_PATH_INACCESSIBLE, _H403),
    S3PathNotFoundError: ErrorMapping(ErrorType.S3_PATH_NOT_FOUND, _H404),
    S3PathSyntaxError: ErrorMapping(ErrorType.S3_PATH_SYNTAX, _H400),
    ChecksumMismatchError: ErrorMapping(ErrorType.CHECKSUM_MISMATCH, _H400),
    NoEntrypointError: ErrorMapping(ErrorType.MISSING_ENTRYPOINT, _H400),
    ImageInfoFetchError: ErrorMapping(ErrorType.IMAGE_FETCH, _H400),
    ImageNameParseError: ErrorMapping(ErrorType.IMAGE_NAME_PARSE, _H400),
    ImageTagExistsError: ErrorMapping(ErrorType.IMAGE_TAG_EXISTS, _H400),
    ImageDigestExistsError: ErrorMapping(ErrorType.IMAGE_DIGEST_EXISTS, _H400),
    NoSuchImageError: ErrorMapping(ErrorType.NO_SUCH_IMAGE, _H404),
    NoSuchJobError: ErrorMapping(ErrorType.NO_SUCH_JOB, _H404),
    NoSuchSubJobError: ErrorMapping(ErrorType.NO_SUCH_SUBJOB, _H404),
    NoSuchReferenceDataError: ErrorMapping(ErrorType.NO_SUCH_REFDATA, _H404),
    ReferenceDataExistsError: ErrorMapping(ErrorType.REFDATA_EXISTS, _H400),
    InvalidJobStateError: ErrorMapping(ErrorType.INVALID_JOB_STATE, _H400),
    InvalidReferenceDataStateError: ErrorMapping(ErrorType.INVALID_REFDATA_STATE, _H400),
    UnavailableResourceError: ErrorMapping(ErrorType.RESOURCE_UNAVAILABLE, _H400),
    InactiveJobFlowError: ErrorMapping(ErrorType.JOB_FLOW_INACTIVE, _H400),
    UnavailableJobFlowError: ErrorMapping(ErrorType.JOB_FLOW_UNAVAILABLE, _H400),
    IllegalParameterError: ErrorMapping(ErrorType.ILLEGAL_PARAMETER, _H400),
    UnsupportedOperationError: ErrorMapping(ErrorType.UNSUPPORTED_OP, _H400),
}


def map_error(err: Exception) -> ErrorMapping:
    """
    Map an error to an optional error type and a HTTP code.
    """
    # May need to add code to go up the error hierarchy if multiple errors have the same type
    ret = _ERR_MAP.get(type(err))
    if not ret:
        ret = ErrorMapping(None, status.HTTP_500_INTERNAL_SERVER_ERROR)
    return ret
