""" S3 client exceptions. """


class S3ClientError(Exception):
    """ The base class for S3 client errors. """ 


class S3ClientConnectError(S3ClientError):
    """ The base class for S3 client errors. """ 


class S3PathError(S3ClientError):
    """ Error thrown when an S3 path is incorrectly specified. """


class S3UnexpectedError(S3ClientError):
    """ Error thrown when an S3 path is incorrectly specified. """
