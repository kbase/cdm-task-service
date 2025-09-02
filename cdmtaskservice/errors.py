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

    AUTHENTICATION_FAILED =      (10000, "Authentication failed")
    """ A general authentication error. """

    NO_TOKEN =                   (10010, "No authentication token")
    """ No token was provided when required. """

    INVALID_TOKEN =              (10020, "Invalid token")
    """ The token provided is not valid. """

    INVALID_AUTH_HEADER =        (10030, "Invalid authentication header")
    """ The authentication header is not valid. """
    
    INVALID_USERNAME =           (10040, "Invalid user name")
    """ The provided user name was invalid. """

    UNAUTHORIZED =               (20000, "Unauthorized")
    """ The user is not authorized to perform the requested action. """
    
    S3_PATH_INACCESSIBLE =       (20010, "S3 path inaccessible")
    """ The user is not authorized to access the S3 path. """
    
    S3_BUCKET_INACCESSIBLE =     (20020, "S3 bucket inaccessible")
    """ The user is not authorized to access the S3 bucket. """

    MISSING_PARAMETER =          (30000, "Missing input parameter")
    """ A required input parameter was not provided. """

    ILLEGAL_PARAMETER =          (30001, "Illegal input parameter")
    """ An input parameter had an illegal value. """
    
    REQUEST_VALIDATION_FAILED =  (30010, "Request validation failed")
    """ A request to a service failed validation of the request. """

    S3_PATH_SYNTAX =             (30020, "Illegal S3 path")
    """ The S3 path was invalid. """
    
    CHECKSUM_MISMATCH =          (30030, "Checksum mismatch")
    """ The expected checksum did not match the actual checksum. """
    
    IMAGE_NAME_PARSE =           (30040, "Image name parse error")
    """ The provided image name could not be parsed. """

    IMAGE_FETCH =                (30050, "Image fetch error")
    """ Information about the image could not be fetched. """
    
    MISSING_ENTRYPOINT =         (30060, "Image has no entrypoint")
    """ The provided image does not have an entrypoint. """

    CLIENT_LIFETIME =            (30200, "Client lifetime too short")
    """ The client life time is shorter than requested. """
    
    INVALID_JOB_STATE =          (30300, "Invalid job state")
    """ The job is not in the correct state for the requested operation. """
    
    INVALID_REFDATA_STATE =      (30400, "Invalid reference data state")
    """ The reference data is not in the correct state for the requested operation. """

    NOT_FOUND =                  (40000, "Not Found")
    """ The requested resource was not found. """
    
    S3_PATH_NOT_FOUND =          (40010, "S3 path not found")
    """ The S3 path was not found. """
    
    S3_BUCKET_NOT_FOUND =        (40020, "S3 bucket not found")
    """ The S3 bucket was not found. """
    
    NO_SUCH_IMAGE =              (40030, "No such image")
    """ The Docker image was not found in the system. """

    NO_SUCH_JOB =                (40040, "No such job")
    """ The job was not found in the system. """
    
    NO_SUCH_SUBJOB =             (40050, "No such sub job")
    """ The sub job was not found in the system. """
    
    NO_SUCH_REFDATA =            (40060, "No such reference data")
    """ The reference data was not found in the system. """

    IMAGE_TAG_EXISTS =           (50000, "Image tag exists")
    """ The tag for the image already exists in the system. """

    IMAGE_DIGEST_EXISTS =        (50010, "Image digest exists")
    """ The digest for the image already exists in the system. """
    
    REFDATA_EXISTS =             (50100, "Refdata already exists")
    """ The reference data already exists in the system. """

    RESOURCE_UNAVAILABLE =       (60000, "Resource unavailable")
    """ The requested resource is not available. """

    JOB_FLOW_INACTIVE =          (60010, "Job flow inactive")
    """ The requested job flow is not currently active. """
    
    JOB_FLOW_UNAVAILABLE =       (60020, "Job flow unavailable")
    """ The requested job flow is unable to be used. """

    UNSUPPORTED_OP =             (100000, "Unsupported operation")
    """ The requested operation is not supported. """

    def __init__(self, error_code, error_type):
        self.error_code = error_code
        self.error_type = error_type
