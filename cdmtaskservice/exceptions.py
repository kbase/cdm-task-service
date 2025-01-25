"""
General exceptions used by multiple modules.
"""


class ETagMismatchError(Exception):
    """ Thrown when an specified ETag does not match the expected ETag. """


class IllegalParameterError(Exception):
    """ An exception thrown when an input parameter is illegal. """


class InvalidJobStateError(Exception):
    """ An exception thrown when a job is in an invalid state to perform an operation. """


class UnauthorizedError(Exception):
    """ An exception thrown when a user attempts a forbidden action. """
