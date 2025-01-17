"""
General exceptions used by multiple modules.
"""


class UnauthorizedError(Exception):
    """ An exception thrown when a user attempts a forbidden action. """


class IllegalParameterError(Exception):
    """ An exception thrown when an input parameter is illegal. """


class InvalidJobStateError(Exception):
    """ An exception thrown when a job is in an invalid state to perform an operation. """
