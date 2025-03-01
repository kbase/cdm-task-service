"""
General exceptions used by multiple modules.
"""


class ChecksumMismatchError(Exception):
    """ Thrown when an specified checksum does not match the expected checksum. """


class IllegalParameterError(Exception):
    """ An exception thrown when an input parameter is illegal. """


class InvalidJobStateError(Exception):
    """ An exception thrown when a job is in an invalid state to perform an operation. """


class InvalidReferenceDataStateError(Exception):
    """ An exception thrown when reference data is in an invalid state to perform an operation. """


class UnauthorizedError(Exception):
    """ An exception thrown when a user attempts a forbidden action. """


class InvalidAuthHeaderError(Exception):
    """ An error thrown when an authorization header is invalid. """
