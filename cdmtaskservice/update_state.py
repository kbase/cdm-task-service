"""
Contains enums and classes to represent various ways in which a job or refdata staging process
might be updated.
"""

from enum import StrEnum, auto
from typing import Self, Any, TypeVar, Generic

from cdmtaskservice import models
from cdmtaskservice.arg_checkers import (
    not_falsy as _not_falsy,
    require_string as _require_string,
    check_num as _check_num,
)


class UpdateField(StrEnum):
    """
    Fields which may be present in an update.
    """
    
    NERSC_DOWNLOAD_TASK_ID = auto()
    """ The NERSC Superfacility ID for an download task. """
    
    JAWS_RUN_ID = auto()
    """ The ID of a JAWS run. """
    
    HTCONDOR_CLUSTER_ID = auto()
    """ The cluster ID of an HTCondor run. """
    
    CPU_HOURS = auto()
    """ The number of hours a job ran. """
    
    NERSC_UPLOAD_TASK_ID = auto()
    """ The NERSC Superfacility ID for an upload task. """
    
    NERSC_LOG_UPLOAD_TASK_ID = auto()
    """ The NERSC Superfacility ID for a log upload task. """
    
    OUTPUT_FILE_PATHS = auto()
    """ The output file paths for a job. """
    
    OUTPUT_FILE_COUNT = auto()
    """ The number of output file paths. """
    
    USER_ERROR = auto()
    """ Error information about a process targeted at a user. """
    
    ADMIN_ERROR = auto()
    """ Error information about a process targeted at a system admin. """
    
    TRACEBACK = auto()
    """ The traceback of an error. """
    
    LOG_PATH = auto()
    """ The path to logs created by a process. """


T = TypeVar("T", models.JobState, models.ReferenceDataState)


class Update(Generic[T]):
    """
    An update for a process.
    """
    
    def __init__(self):
        self._new_state = None
        self._current_state = None
        self._fields = {}
    
    def _set_new_state(self, new_state: str) -> Self:
        self._new_state = new_state
        return self
    
    def _set_current_state(self, current_state: str) -> Self:
        self._current_state = current_state
        return self

    def _set_fields(self, fields: dict[UpdateField, Any]) -> Self:
        self._fields = fields
        return self

    @property
    def new_state(self) -> T:
        """ The state to apply to the process. """
        if not self._new_state or not self._new_state.strip():
            raise ValueError("A programming error occurred, new state must be present")
        return self._new_state

    @property
    def current_state(self) -> T | None:
        """ Get the expected current state of the process, if any. """
        return self._current_state

    @property
    def update_fields(self) -> dict[UpdateField, Any]:
        """
        Get the fields for the update, if any. The values of the fields are basic
        types and containers, suitable for dumping to JSON.
        """
        return dict(self._fields)  # don't bother with full immutability


class JobUpdate(Update[models.JobState]):
    """ An update for a job. """


class RefdataUpdate(Update[models.ReferenceDataState]):
    """ An update for a refdata staging process. """


def submitted_download() -> JobUpdate:
    """
    Update a job's state from created to download submitted.
    """
    return JobUpdate(
        )._set_current_state(models.JobState.CREATED
        )._set_new_state(models.JobState.DOWNLOAD_SUBMITTED
    )


def submitted_nersc_download(task_id: str) -> JobUpdate:
    """
    Update a job's state from created to download submitted and add a NERSC
    superfacility API download task ID.
    """
    return JobUpdate(
        )._set_current_state(models.JobState.CREATED
        )._set_new_state(models.JobState.DOWNLOAD_SUBMITTED
        )._set_fields(
            {UpdateField.NERSC_DOWNLOAD_TASK_ID: _require_string(task_id, "task_id")}
    )


def submitted_htcondor_download(cluster_id: int) -> JobUpdate:
    """
    Update a job's state from created to download submitted and add an HTCondor cluster ID.
    """
    return JobUpdate(
        )._set_current_state(models.JobState.CREATED
        )._set_new_state(models.JobState.DOWNLOAD_SUBMITTED
        )._set_fields(
            {UpdateField.HTCONDOR_CLUSTER_ID: _check_num(cluster_id, "cluster_id")}
    )


def submitting_job() -> JobUpdate:
    """ Update a job's state from download submitted to submitting job. """
    return JobUpdate(
        )._set_current_state(models.JobState.DOWNLOAD_SUBMITTED
        )._set_new_state(models.JobState.JOB_SUBMITTING
    )


def submitted_jaws_job(jaws_run_id: str) -> JobUpdate:
    """ Update a job's state from submitting job to submitted job and add the JAWS run ID. """
    return JobUpdate(
        )._set_current_state(models.JobState.JOB_SUBMITTING
        )._set_new_state(models.JobState.JOB_SUBMITTED
        )._set_fields({UpdateField.JAWS_RUN_ID: _require_string(jaws_run_id, "jaws_run_id")}
    )


def submitting_upload(cpu_hours: float = None) -> JobUpdate:
    """
    Update a job's state from job submitted to upload submitting and add cpu hours, if available.
    """
    return JobUpdate(
        )._set_current_state(models.JobState.JOB_SUBMITTED
        )._set_new_state(models.JobState.UPLOAD_SUBMITTING
        )._set_fields(
            {
                UpdateField.CPU_HOURS: _check_num(cpu_hours, "cpu_hours", minimum=0)
            } if cpu_hours is not None else {}
    )


def submitted_nersc_upload(task_id: str) -> JobUpdate:
    """
    Update a job's state from upload submitting to upload submitted and add a NERSC
    superfacility API upload task ID.
    """
    return JobUpdate(
        )._set_current_state(models.JobState.UPLOAD_SUBMITTING
        )._set_new_state(models.JobState.UPLOAD_SUBMITTED
        )._set_fields({UpdateField.NERSC_UPLOAD_TASK_ID: _require_string(task_id, "task_id")}
    )


def complete(output_file_paths: list[models.S3File]) -> JobUpdate:
    """
    Update a job's state from upload submitted to complete and add output files.
    """
    out = [o.model_dump() for o in _not_falsy(output_file_paths, "output_file_paths")]
    return JobUpdate(
        )._set_current_state(models.JobState.UPLOAD_SUBMITTED
        )._set_new_state(models.JobState.COMPLETE
        )._set_fields({
            UpdateField.OUTPUT_FILE_PATHS: out,
            UpdateField.OUTPUT_FILE_COUNT: len(out),
        }
    )

####################
### Error states
####################


def submitting_error_processing(cpu_hours: float = None) -> JobUpdate:
    """
    Update a job's state from job submitted to error processing submitting and add cpu hours,
    if available.
    """
    return JobUpdate(
        )._set_current_state(models.JobState.JOB_SUBMITTED
        )._set_new_state(models.JobState.ERROR_PROCESSING_SUBMITTING
        )._set_fields(
            {
                UpdateField.CPU_HOURS: _check_num(cpu_hours, "cpu_hours", minimum=0)
            } if cpu_hours is not None else {}
    )


def submitted_nersc_error_processing(task_id: str) -> JobUpdate:
    """
    Update a job's state from error processing submitting to error processing submitted and add
    a NERSC superfacility API upload task ID.
    """
    return JobUpdate(
        )._set_current_state(models.JobState.ERROR_PROCESSING_SUBMITTING
        )._set_new_state(models.JobState.ERROR_PROCESSING_SUBMITTED
        )._set_fields(
            {UpdateField.NERSC_LOG_UPLOAD_TASK_ID: _require_string(task_id, "task_id")}
    )


def error(
    admin_error: str,
    user_error: str = None,
    traceback: str = None,
    log_files_path: str = None,
    cpu_hours: float = None,
) -> JobUpdate:
    """
    Update a job's state to error.
    
    admin_error - an error message targeted towards a service admin.
    user_error - an error message targeted towards a service user. Leave as null to indicate there
        is no error message available appropriate for a user.
    traceback - the error traceback.
    log_files_path - the path to any logs for the job.
    cpu_hours - the job cpu hours, if available.
    """ 
    flds = {
        UpdateField.ADMIN_ERROR: _require_string(admin_error, "admin_error"),
        UpdateField.TRACEBACK: traceback,
        UpdateField.LOG_PATH: log_files_path,
    }
    if user_error is not None:
        flds[UpdateField.USER_ERROR] = user_error
    if cpu_hours is not None:  # Don't potentially clobber cpu hours if there's nothing to write
        flds[UpdateField.CPU_HOURS] = _check_num(cpu_hours, "cpu_hours", minimum=0)
    return JobUpdate()._set_new_state(models.JobState.ERROR)._set_fields(flds)


####################
### Refdata states
####################


def submitted_refdata_download() -> RefdataUpdate:
    """
    Update a refdata staging process's state from created to download submitted.
    """
    return RefdataUpdate(
        )._set_current_state(models.ReferenceDataState.CREATED
        )._set_new_state(models.ReferenceDataState.DOWNLOAD_SUBMITTED
    )


def submitted_nersc_refdata_download(task_id: str) -> RefdataUpdate:
    """
    Update a refdata staging process's state from created to download submitted and add a NERSC
    superfacility API download task ID.
    """
    return RefdataUpdate(
        )._set_current_state(models.ReferenceDataState.CREATED
        )._set_new_state(models.ReferenceDataState.DOWNLOAD_SUBMITTED
        )._set_fields(
            {UpdateField.NERSC_DOWNLOAD_TASK_ID: _require_string(task_id, "task_id")}
    )


def refdata_complete() -> RefdataUpdate:
    """
    Update a refdata staging process's state from download submitted to complete.
    """
    return RefdataUpdate(
        )._set_current_state(models.ReferenceDataState.DOWNLOAD_SUBMITTED
        )._set_new_state(models.ReferenceDataState.COMPLETE
    )


def refdata_error(
    user_error: str,
    admin_error: str,
    traceback: str = None,
) -> RefdataUpdate:
    """
    Update a refdata staging process's state to error.
    
    user_error - an error message targeted towards a service user.
    admin_error - an error message targeted towards a service admin.
    traceback - the error traceback.
    """
    return RefdataUpdate(
        )._set_new_state(models.JobState.ERROR
        )._set_fields({
            UpdateField.USER_ERROR:_require_string(user_error, "user_error"), 
            UpdateField.ADMIN_ERROR:_require_string(admin_error, "admin_error"), 
            UpdateField.TRACEBACK:traceback, 
        }
    )
