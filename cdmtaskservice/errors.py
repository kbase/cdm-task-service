"""
Error codes for exceptions thrown by the CDM task service system.
"""

from enum import Enum


class ErrorType(Enum):
    """
    The type of an error, consisting of an error code and a brief string describing the type.
    :ivar error_code: an integer error code.
    :ivar error_type: a brief string describing the error type.
    """

    AUTHENTICATION_FAILED =      (10000, "Authentication failed")  # noqa: E222 @IgnorePep8
    """ A general authentication error. """

    NO_TOKEN =                   (10010, "No authentication token")  # noqa: E222 @IgnorePep8
    """ No token was provided when required. """

    INVALID_TOKEN =              (10020, "Invalid token")  # noqa: E222 @IgnorePep8
    """ The token provided is not valid. """

    INVALID_AUTH_HEADER =        (10030, "Invalid authentication header")  # noqa: E222 @IgnorePep8
    """ The authentication header is not valid. """

    UNAUTHORIZED =               (20000, "Unauthorized")  # noqa: E222 @IgnorePep8
    """ The user is not authorized to perform the requested action. """

    MISSING_PARAMETER =          (30000, "Missing input parameter")  # noqa: E222 @IgnorePep8
    """ A required input parameter was not provided. """

    ILLEGAL_PARAMETER =          (30001, "Illegal input parameter")  # noqa: E222 @IgnorePep8
    """ An input parameter had an illegal value. """

    CLIENT_LIFETIME =            (30050, "Client lifetime too short")  # noqa: E222 @IgnorePep8
    """ The client life time is shorter than requested. """

    NOT_FOUND =                  (40000, "Not Found")  # noqa: E222 @IgnorePep8
    """ The requested resource was not found. """

    REQUEST_VALIDATION_FAILED =  (30010, "Request validation failed")  # noqa: E222 @IgnorePep8
    """ A request to a service failed validation of the request. """

    UNSUPPORTED_OP =             (100000, "Unsupported operation")  # noqa: E222 @IgnorePep8
    """ The requested operation is not supported. """

    def __init__(self, error_code, error_type):
        self.error_code = error_code
        self.error_type = error_type
