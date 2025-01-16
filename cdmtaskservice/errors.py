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
    
    S3_PATH_INACCESSIBLE =       (20010, "S3 path inaccessible")  # noqa: E222 @IgnorePep8
    """ The user is not authorized to access the S3 path. """
    
    S3_BUCKET_INACCESSIBLE =     (20020, "S3 bucket inaccessible")  # noqa: E222 @IgnorePep8
    """ The user is not authorized to access the S3 bucket. """

    MISSING_PARAMETER =          (30000, "Missing input parameter")  # noqa: E222 @IgnorePep8
    """ A required input parameter was not provided. """

    ILLEGAL_PARAMETER =          (30001, "Illegal input parameter")  # noqa: E222 @IgnorePep8
    """ An input parameter had an illegal value. """
    
    REQUEST_VALIDATION_FAILED =  (30010, "Request validation failed")  # noqa: E222 @IgnorePep8
    """ A request to a service failed validation of the request. """

    S3_PATH_SYNTAX =             (30020, "Illegal S3 path")  # noqa: E222 @IgnorePep8
    """ The S3 path was invalid. """
    
    S3_ETAG_MISMATCH =           (30030, "S3 ETag mismatch")  # noqa: E222 @IgnorePep8
    """ The expected S3 ETag did not match the actual ETag. """
    
    IMAGE_NAME_PARSE =           (30040, "Image name parse error")  # noqa: E222 @IgnorePep8
    """ The provided image name could not be parsed. """

    IMAGE_FETCH =                (30050, "Image fetch error")  # noqa: E222 @IgnorePep8
    """ Information about the image could not be fetched. """
    
    MISSING_ENTRYPOINT =         (30060, "Image has no entrypoint")  # noqa: E222 @IgnorePep8
    """ The provided image does not have an entrypoint. """

    CLIENT_LIFETIME =            (30200, "Client lifetime too short")  # noqa: E222 @IgnorePep8
    """ The client life time is shorter than requested. """
    
    INVALID_JOB_STATE =          (30300, "Invalid job state")  # noqa: E222 @IgnorePep8
    """ The job is not in the correct state for the requested operation. """

    NOT_FOUND =                  (40000, "Not Found")  # noqa: E222 @IgnorePep8
    """ The requested resource was not found. """
    
    S3_PATH_NOT_FOUND =          (40010, "S3 path not found")  # noqa: E222 @IgnorePep8
    """ The S3 path was not found. """
    
    S3_BUCKET_NOT_FOUND =        (40020, "S3 bucket not found")  # noqa: E222 @IgnorePep8
    """ The S3 bucket was not found. """
    
    NO_SUCH_IMAGE =              (40030, "No such image")  # noqa: E222 @IgnorePep8
    """ The Docker image was not found in the system. """

    NO_SUCH_JOB =                (40040, "No such job")  # noqa: E222 @IgnorePep8
    """ The job was not found in the system. """

    IMAGE_TAG_EXISTS =           (50000, "Image tag exists")  # noqa: E222 @IgnorePep
    """ The tag for the image already exists in the system. """

    IMAGE_DIGEST_EXISTS =        (50010, "Image digest exists")  # noqa: E222 @IgnorePep
    """ The digest for the image already exists in the system. """

    JOB_FLOW_INACTIVE =          (60000, "Job flow inactive")  # noqa: E222 @IgnorePep
    """ The requested job flow is not currently active. """

    UNSUPPORTED_OP =             (100000, "Unsupported operation")  # noqa: E222 @IgnorePep8
    """ The requested operation is not supported. """

    def __init__(self, error_code, error_type):
        self.error_code = error_code
        self.error_type = error_type
