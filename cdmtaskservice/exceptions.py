"""
General exceptions used by multiple modules.
"""
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.models import JobState


class ChecksumMismatchError(Exception):
    """ Thrown when an specified checksum does not match the expected checksum. """


class IllegalParameterError(Exception):
    """ An exception thrown when an input parameter is illegal. """


class InvalidJobStateError(Exception):
    """ An exception thrown when a job is in an invalid state to perform an operation. """


class MismatchedJobStateError(InvalidJobStateError):
    """
    An exception thrown when a job's current state doesn't match the state required to perform
    an operation on it.
    """

    def __init__(self, message: str, actual_state: JobState):
        """
        message - the error message.
        actual_state - the actual state of the job at the time of the error. Callers can
            inspect this to decide how to handle the error, e.g. whether to retry.
        """
        super().__init__(message)
        self.actual_state = _not_falsy(actual_state, "actual_state")


class InvalidReferenceDataStateError(Exception):
    """ An exception thrown when reference data is in an invalid state to perform an operation. """


class InvalidAIModelStateError(Exception):
    """ An exception thrown when an AI model is in an invalid state to perform an operation. """


class UnauthorizedError(Exception):
    """ An exception thrown when a user attempts a forbidden action. """


class InvalidAuthHeaderError(Exception):
    """ An error thrown when an authorization header is invalid. """


class UnavailableResourceError(Exception):
    """ An error thrown when a resouce is unavailable. """


class UnsupportedOperationError(Exception):
    """ An error thrown when an unsupported operation is requested. """


class JobRecoveryError(Exception):
    """ An error occurred during job recovery. """


class NoSuchJobError(Exception):
    """ The job does not exist in the system. """
