import pytest
from unittest.mock import create_autospec, patch, PropertyMock

from cdmtaskservice.condor.client import CondorClient
from cdmtaskservice.config_s3 import S3Config
from cdmtaskservice.jobflows.kbase import KBaseRunner
from cdmtaskservice.jobflows.state_updates import SubjobFlowStateUpdates, ParentJobUpdate
from cdmtaskservice.exceptions import UnsupportedOperationError
from cdmtaskservice import models
from cdmtaskservice import update_state
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.refserv.client import RefdataServiceClient
from cdmtaskservice.s3.client import S3Client, S3ObjectMeta
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.timestamp import utcdatetime


# TODO TEST add more tests


_T = utcdatetime()

# A realistic HTCondor job ClassAd used to exercise cpu_hours / cpu_factor / max_memory paths.
# RemoteUserCpu=3600s, CommittedTime=1800s, cpus=1 → cpu_hours=1.0, cpu_factor=2.0.
# MemoryUsage=512 MiB → max_memory=512*1024*1024 bytes.
_CONDOR_AD = {
    "RemoteUserCpu": 3600.0,
    "RemoteSysCpu": 0.0,
    "CommittedTime": 1800,
    "MemoryUsage": 512,
}
_CPU_HOURS = 1.0
_CPU_FACTOR = 2.0
_MAX_MEM = 512 * 1024 * 1024


class _FakeCoroutineWrangler:
    async def run_coroutine(self, coro):
        await coro


def _job(num_containers=2):
    return models.AdminJobDetails.model_construct(
        id="jid",
        job_input=models.JobInput.model_construct(num_containers=num_containers, cpus=1),
        htcondor_details=models.HTCondorDetails(cluster_id=[123]),
    )


_JOB = _job()


def _update(admin_error=None, exit_code=None, outputs=None, traceback=None):
    return models.ContainerUpdate(
        time=_T, admin_error=admin_error, exit_code=exit_code, outputs=outputs, traceback=traceback
    )


def _make_runner():
    condor = create_autospec(CondorClient, spec_set=True, instance=True)
    mongo = create_autospec(MongoDAO, spec_set=True, instance=True)
    s3config = create_autospec(S3Config, spec_set=True, instance=True)
    # error_log_path is a Pydantic field absent from dir(S3Config), so spec_set blocks
    # direct attribute assignment; PropertyMock on the type bypasses the instance __setattr__
    type(s3config).error_log_path = PropertyMock(return_value="logs/")
    s3 = create_autospec(S3Client, spec_set=True, instance=True)
    s3config.get_internal_client.return_value = s3
    updates = create_autospec(SubjobFlowStateUpdates, spec_set=True, instance=True)
    refserv = create_autospec(RefdataServiceClient, spec_set=True, instance=True)
    runner = KBaseRunner(condor, mongo, s3config, updates, _FakeCoroutineWrangler(), refserv)
    return runner, mongo, condor, updates, s3


async def test_update_container_state_bad_args():
    runner, _, _, _, _ = _make_runner()
    s = models.JobState.JOB_SUBMITTING
    upd = _update()

    with pytest.raises(ValueError, match="^job is required$"):
        await runner.update_container_state(None, 0, s, upd)
    with pytest.raises(ValueError, match="^container_num is required$"):
        await runner.update_container_state(_JOB, None, s, upd)
    with pytest.raises(ValueError, match="^container_num must be >= 0$"):
        await runner.update_container_state(_JOB, -1, s, upd)
    with pytest.raises(ValueError, match="^new_state is required$"):
        await runner.update_container_state(_JOB, 0, None, upd)
    with pytest.raises(ValueError, match="^update is required$"):
        await runner.update_container_state(_JOB, 0, s, None)


async def test_update_container_state_no_parent_update():
    """Only 1 of 2 subjobs has reached a state - no parent job update."""
    runner, mongo, _, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = None

    await runner.update_container_state(_JOB, 0, models.JobState.JOB_SUBMITTING, _update())

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0, update_state.submitting_job(), _T
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.JOB_SUBMITTING)
    updates.update_job_state.assert_not_called()


async def test_update_container_state_nonterminal_parent_update():
    """All subjobs at JOB_SUBMITTING - parent job should transition."""
    runner, mongo, _, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(
        models.JobState.JOB_SUBMITTING, _T
    )

    await runner.update_container_state(_JOB, 0, models.JobState.JOB_SUBMITTING, _update())

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0, update_state.submitting_job(), _T
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.JOB_SUBMITTING)
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.submitting_job(), update_time=_T
    )


async def test_update_container_state_job_submitted():
    """All subjobs at JOB_SUBMITTED - parent job should transition with submitted_job update."""
    runner, mongo, _, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.JOB_SUBMITTED, _T)

    await runner.update_container_state(_JOB, 0, models.JobState.JOB_SUBMITTED, _update())

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0, update_state.submitted_job(), _T
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.JOB_SUBMITTED)
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.submitted_job(), update_time=_T
    )


async def test_update_container_state_upload_submitting():
    """All subjobs at UPLOAD_SUBMITTING - subjob carries exit code, parent job does not."""
    runner, mongo, _, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(
        models.JobState.UPLOAD_SUBMITTING, _T
    )

    await runner.update_container_state(
        _JOB, 0, models.JobState.UPLOAD_SUBMITTING, _update(exit_code=0)
    )

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0, update_state.submitting_upload_with_exit_code(0), _T
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.UPLOAD_SUBMITTING)
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.submitting_upload(), update_time=_T
    )


async def test_update_container_state_upload_submitted():
    """All subjobs at UPLOAD_SUBMITTED - parent job should transition with submitted_upload."""
    runner, mongo, _, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(
        models.JobState.UPLOAD_SUBMITTED, _T
    )

    await runner.update_container_state(_JOB, 0, models.JobState.UPLOAD_SUBMITTED, _update())

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0, update_state.submitted_upload(), _T
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.UPLOAD_SUBMITTED)
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.submitted_upload(), update_time=_T
    )


async def test_update_container_state_error_processing_submitting():
    """All subjobs at ERROR_PROCESSING_SUBMITTING - subjob carries exit code, parent does not."""
    runner, mongo, _, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(
        models.JobState.ERROR_PROCESSING_SUBMITTING, _T
    )

    await runner.update_container_state(
        _JOB, 0, models.JobState.ERROR_PROCESSING_SUBMITTING, _update(exit_code=1)
    )

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0, update_state.submitting_error_processing_with_exit_code(1), _T
    )
    updates.get_parent_job_update.assert_called_once_with(
        _JOB, models.JobState.ERROR_PROCESSING_SUBMITTING
    )
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.submitting_error_processing(), update_time=_T
    )


async def test_update_container_state_error_processing_submitted():
    """
    All subjobs at ERROR_PROCESSING_SUBMITTED - parent transitions with submitted_error_processing.
    """
    runner, mongo, _, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(
        models.JobState.ERROR_PROCESSING_SUBMITTED, _T
    )

    await runner.update_container_state(
        _JOB, 0, models.JobState.ERROR_PROCESSING_SUBMITTED, _update()
    )

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0, update_state.submitted_error_processing(), _T
    )
    updates.get_parent_job_update.assert_called_once_with(
        _JOB, models.JobState.ERROR_PROCESSING_SUBMITTED
    )
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.submitted_error_processing(), update_time=_T
    )


async def test_update_container_state_terminal_error():
    """One subjob is terminal with non-zero exit code - parent job enters error with log path."""
    runner, mongo, condor, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.ERROR, _T)
    mongo.get_exit_codes_for_subjobs.return_value = [1, 0]
    condor.get_job_status.return_value = ([], [_CONDOR_AD])

    # asyncio.sleep is patched to make the _get_condor_stats polling loop instant
    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.update_container_state(
            _JOB, 1, models.JobState.ERROR,
            _update(admin_error="container failed", traceback="Traceback: container failed"),
        )

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 1,
        update_state.error("container failed", traceback="Traceback: container failed"), _T,
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.ERROR)
    condor.get_job_status.assert_called_once_with(123)
    mongo.get_exit_codes_for_subjobs.assert_called_once_with("jid")
    updates.update_job_state.assert_called_once_with(
        "jid",
        update_state.error(
            "Check subjobs / containers for admin errors",
            user_error=(
                "At least one container exited with a non-zero "
                "error code. Please examine the logs for details."
            ),
            log_files_path="logs/jid",
            cpu_hours=_CPU_HOURS,
            cpu_factor=_CPU_FACTOR,
            max_memory=_MAX_MEM,
        ),
    )


async def test_update_container_state_terminal_complete():
    """All subjobs complete - parent job should enter complete state."""
    runner, mongo, condor, updates, s3 = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.COMPLETE, _T)
    outputs = [models.S3File(file="bucket/f.txt", crc64nvme="aaaabbbbcccc")]
    sj = models.SubJob.model_construct(outputs=outputs)
    mongo.get_subjobs.return_value = [sj]
    s3obj = S3ObjectMeta("bucket/f.txt", "etag", 0, "aaaabbbbcccc")
    s3.get_object_meta.return_value = [s3obj]
    condor.get_job_status.return_value = ([], [_CONDOR_AD])

    # asyncio.sleep is patched to make the _get_condor_stats polling loop instant
    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.update_container_state(
            _JOB, 0, models.JobState.COMPLETE, _update(outputs=outputs)
        )

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0, update_state.complete(outputs), _T
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.COMPLETE)
    condor.get_job_status.assert_called_once_with(123)
    mongo.get_subjobs.assert_called_once_with("jid")
    s3.get_object_meta.assert_called_once_with(S3Paths(["bucket/f.txt"]))
    updates.update_job_state.assert_called_once_with(
        "jid",
        update_state.complete(
            [models.S3File(file="bucket/f.txt", crc64nvme="aaaabbbbcccc")],
            cpu_hours=_CPU_HOURS,
            cpu_factor=_CPU_FACTOR,
            max_memory=_MAX_MEM,
        ),
    )


async def test_update_container_state_parent_update_fails():
    """A failure in get_parent_job_update results in handle_exception being called."""
    runner, mongo, _, updates, _ = _make_runner()
    updates.get_parent_job_update.side_effect = Exception("db error")

    await runner.update_container_state(_JOB, 0, models.JobState.JOB_SUBMITTING, _update())

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0, update_state.submitting_job(), _T
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.JOB_SUBMITTING)
    exc = updates.handle_exception.call_args.args[0]
    assert isinstance(exc, Exception)
    assert str(exc) == "db error"
    updates.handle_exception.assert_called_once_with(exc, "jid", "updating job state")


async def test_update_container_state_unsupported_state():
    """A state not in _SUBJOB_STATE_TO_UPDATE_FUNC raises before touching the DB."""
    runner, mongo, _, updates, _ = _make_runner()

    with pytest.raises(
        UnsupportedOperationError,
        match="^Cannot update a container to state created$"
    ):
        await runner.update_container_state(_JOB, 0, models.JobState.CREATED, _update())

    mongo.update_subjob_state.assert_not_called()
    updates.get_parent_job_update.assert_not_called()


async def test_error_job_no_nonzero_exit_codes():
    """All exit codes 0 or None - generic error message with no log path."""
    runner, mongo, condor, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.ERROR, _T)
    mongo.get_exit_codes_for_subjobs.return_value = [0, None]
    condor.get_job_status.return_value = ([], [_CONDOR_AD])

    # asyncio.sleep is patched to make the _get_condor_stats polling loop instant
    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.update_container_state(
            _JOB, 0, models.JobState.ERROR,
            _update(admin_error="container failed", traceback="Traceback: container failed"),
        )

    mongo.update_subjob_state.assert_called_once_with(
        "jid",
        0,
        update_state.error("container failed", traceback="Traceback: container failed"),
        _T,
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.ERROR)
    condor.get_job_status.assert_called_once_with(123)
    mongo.get_exit_codes_for_subjobs.assert_called_once_with("jid")
    updates.update_job_state.assert_called_once_with(
        "jid",
        update_state.error(
            "Check subjobs / containers for admin errors",
            user_error="An unexpected error occurred.",
            cpu_hours=_CPU_HOURS,
            cpu_factor=_CPU_FACTOR,
            max_memory=_MAX_MEM,
        ),
    )


async def test_error_job_held_running_containers():
    """_get_condor_stats exits on first iteration when all running containers are held."""
    runner, mongo, condor, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.ERROR, _T)
    mongo.get_exit_codes_for_subjobs.return_value = [1]
    # running=[held job] triggers an immediate exit via condor_jobs_all_held
    condor.get_job_status.return_value = ([{"JobStatus": 5}], [_CONDOR_AD])

    # asyncio.sleep is patched to make the _get_condor_stats polling loop instant;
    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.update_container_state(
            _JOB, 0, models.JobState.ERROR,
            _update(admin_error="container failed", traceback="Traceback: container failed"),
        )

    condor.get_job_status.assert_called_once_with(123)
    mongo.get_exit_codes_for_subjobs.assert_called_once_with("jid")
    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0,
        update_state.error("container failed", traceback="Traceback: container failed"), _T,
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.ERROR)
    updates.update_job_state.assert_called_once_with(
        "jid",
        update_state.error(
            "Check subjobs / containers for admin errors",
            user_error=(
                "At least one container exited with a non-zero "
                "error code. Please examine the logs for details."
            ),
            log_files_path="logs/jid",
            cpu_hours=_CPU_HOURS,
            cpu_factor=_CPU_FACTOR,
            max_memory=_MAX_MEM,
        ),
    )


async def test_condor_stats_timeout():
    """_get_condor_stats raises IOError after 12 attempts; handle_exception is called."""
    runner, mongo, condor, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.ERROR, _T)
    # JobStatus=2 (running, not held) keeps the loop going until the 12-attempt limit
    condor.get_job_status.return_value = ([{"JobStatus": 2}], [])

    # asyncio.sleep is patched to make the polling loop instant
    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.update_container_state(
            _JOB, 0, models.JobState.ERROR,
            _update(admin_error="container failed", traceback="Traceback: container failed"),
        )

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0,
        update_state.error("container failed", traceback="Traceback: container failed"), _T,
    )
    assert condor.get_job_status.call_count == 12
    condor.get_job_status.assert_called_with(123)
    mongo.get_exit_codes_for_subjobs.assert_not_called()
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.ERROR)
    exc = updates.handle_exception.call_args.args[0]
    assert isinstance(exc, IOError)
    assert str(exc) == "Condor jobs didn't complete for 60s after all executors sent termination"
    updates.handle_exception.assert_called_once_with(exc, "jid", "updating job state")


async def test_complete_job_no_outputs():
    """_complete_job with subjobs producing no output files sets the job to error."""
    runner, mongo, condor, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.COMPLETE, _T)
    sj = models.SubJob.model_construct(outputs=[])
    mongo.get_subjobs.return_value = [sj]
    condor.get_job_status.return_value = ([], [_CONDOR_AD])

    # asyncio.sleep is patched to make the _get_condor_stats polling loop instant
    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.update_container_state(_JOB, 0, models.JobState.COMPLETE, _update(outputs=[]))

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0, update_state.complete([]), _T
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.COMPLETE)
    condor.get_job_status.assert_called_once_with(123)
    mongo.get_subjobs.assert_called_once_with("jid")
    # stats are computed but not forwarded on this error path
    updates.update_job_state.assert_called_once_with(
        "jid",
        update_state.error(
            "The job produced no output files",
            user_error="The job produced no output files",
        ),
    )


async def test_complete_job_checksum_mismatch():
    """A CRC mismatch in _complete_job propagates as an exception to handle_exception."""
    runner, mongo, condor, updates, s3 = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.COMPLETE, _T)
    outputs = [models.S3File(file="bucket/f.txt", crc64nvme="aaaaaaaaaaaa")]
    sj = models.SubJob.model_construct(outputs=outputs)
    mongo.get_subjobs.return_value = [sj]
    s3obj = S3ObjectMeta("bucket/f.txt", "etag", 0, "bbbbbbbbbbbb")  # deliberate mismatch
    s3.get_object_meta.return_value = [s3obj]
    condor.get_job_status.return_value = ([], [_CONDOR_AD])

    # asyncio.sleep is patched to make the _get_condor_stats polling loop instant
    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.update_container_state(
            _JOB, 0, models.JobState.COMPLETE, _update(outputs=outputs)
        )

    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.COMPLETE)
    condor.get_job_status.assert_called_once_with(123)
    mongo.get_subjobs.assert_called_once_with("jid")
    s3.get_object_meta.assert_called_once_with(S3Paths(["bucket/f.txt"]))
    exc = updates.handle_exception.call_args.args[0]
    assert isinstance(exc, ValueError)
    assert str(exc) == (
        "Expected CRC64/NVME checksum aaaaaaaaaaaa but got bbbbbbbbbbbb "
        "for uploaded file bucket/f.txt"
    )
    updates.handle_exception.assert_called_once_with(exc, "jid", "updating job state")
