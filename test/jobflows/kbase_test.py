import datetime
import pytest
from unittest.mock import call, create_autospec, patch, PropertyMock

from cdmtaskservice.condor.client import CondorClient, ProcState
from cdmtaskservice.config_s3 import S3Config
from cdmtaskservice.exceptions import InvalidJobStateError, JobRecoveryError, UnsupportedOperationError
from cdmtaskservice.jobflows.kbase import KBaseRunner
from cdmtaskservice.jobflows.state_updates import SubjobFlowStateUpdates, ParentJobUpdate
from cdmtaskservice import models
from cdmtaskservice import update_state
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.refserv.client import RefdataServiceClient
from cdmtaskservice.s3.client import S3Client, S3ObjectMeta
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.timestamp import utcdatetime


# TODO TEST add more tests


_T = utcdatetime()
_TRANS_ID = "test-trans-id"

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


_UNSET = object()


def _recovery_job(state=models.JobState.ERROR, cluster_ids=_UNSET):
    if cluster_ids is _UNSET:
        cluster_ids = [123]
    return models.AdminJobDetails.model_construct(
        id="jid",
        state=state,
        job_input=models.JobInput.model_construct(num_containers=2, cpus=1),
        htcondor_details=None if cluster_ids is None
            else models.HTCondorDetails(cluster_id=cluster_ids),
    )


_JOB = _job()


def _update(admin_error=None, exit_code=None, outputs=None, traceback=None):
    return models.ContainerUpdate(
        time=_T, admin_error=admin_error, exit_code=exit_code, outputs=outputs, traceback=traceback
    )


def _make_runner(_timestamp_fn=None):
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
    runner = KBaseRunner(
        condor, mongo, s3config, updates, _FakeCoroutineWrangler(), refserv,
        _timestamp_fn=_timestamp_fn if _timestamp_fn is not None else lambda: _T,
        _trans_id_fn=lambda: _TRANS_ID,
    )
    return runner, mongo, condor, updates, s3


######
# update_container_state tests
######


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
    condor.get_cluster_classads.return_value = ([], [_CONDOR_AD])

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
    condor.get_cluster_classads.assert_called_once_with(123)
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
    condor.get_cluster_classads.return_value = ([], [_CONDOR_AD])

    # asyncio.sleep is patched to make the _get_condor_stats polling loop instant
    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.update_container_state(
            _JOB, 0, models.JobState.COMPLETE, _update(outputs=outputs)
        )

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0, update_state.complete(outputs), _T
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.COMPLETE)
    condor.get_cluster_classads.assert_called_once_with(123)
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
        update_time=None,
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


async def test_update_container_state_error_job_no_nonzero_exit_codes():
    """All exit codes 0 or None - generic error message with no log path."""
    runner, mongo, condor, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.ERROR, _T)
    mongo.get_exit_codes_for_subjobs.return_value = [0, None]
    condor.get_cluster_classads.return_value = ([], [_CONDOR_AD])

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
    condor.get_cluster_classads.assert_called_once_with(123)
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


async def test_update_container_state_error_job_held_running_containers():
    """_get_condor_stats exits on first iteration when all running containers are held."""
    runner, mongo, condor, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.ERROR, _T)
    mongo.get_exit_codes_for_subjobs.return_value = [1]
    # running=[held job] triggers an immediate exit via condor_jobs_all_held
    condor.get_cluster_classads.return_value = ([{"JobStatus": 5}], [_CONDOR_AD])

    # asyncio.sleep is patched to make the _get_condor_stats polling loop instant;
    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.update_container_state(
            _JOB, 0, models.JobState.ERROR,
            _update(admin_error="container failed", traceback="Traceback: container failed"),
        )

    condor.get_cluster_classads.assert_called_once_with(123)
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


async def test_update_container_state_condor_stats_timeout():
    """_get_condor_stats raises IOError after 12 attempts; handle_exception is called."""
    runner, mongo, condor, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.ERROR, _T)
    # JobStatus=2 (running, not held) keeps the loop going until the 12-attempt limit
    condor.get_cluster_classads.return_value = ([{"JobStatus": 2}], [])

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
    assert condor.get_cluster_classads.call_count == 12
    condor.get_cluster_classads.assert_called_with(123)
    mongo.get_exit_codes_for_subjobs.assert_not_called()
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.ERROR)
    exc = updates.handle_exception.call_args.args[0]
    assert isinstance(exc, IOError)
    assert str(exc) == "Condor jobs didn't complete for 60s after all executors sent termination"
    updates.handle_exception.assert_called_once_with(exc, "jid", "updating job state")


async def test_update_container_state_complete_job_no_outputs():
    """_complete_job with subjobs producing no output files sets the job to error."""
    runner, mongo, condor, updates, _ = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.COMPLETE, _T)
    sj = models.SubJob.model_construct(outputs=[])
    mongo.get_subjobs.return_value = [sj]
    condor.get_cluster_classads.return_value = ([], [_CONDOR_AD])

    # asyncio.sleep is patched to make the _get_condor_stats polling loop instant
    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.update_container_state(_JOB, 0, models.JobState.COMPLETE, _update(outputs=[]))

    mongo.update_subjob_state.assert_called_once_with(
        "jid", 0, update_state.complete([]), _T
    )
    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.COMPLETE)
    condor.get_cluster_classads.assert_called_once_with(123)
    mongo.get_subjobs.assert_called_once_with("jid")
    # stats are computed but not forwarded on this error path
    updates.update_job_state.assert_called_once_with(
        "jid",
        update_state.error(
            "The job produced no output files",
            user_error="The job produced no output files",
        ),
    )


async def test_update_container_state_complete_job_checksum_mismatch():
    """A CRC mismatch in _complete_job transitions the job to ERROR directly."""
    runner, mongo, condor, updates, s3 = _make_runner()
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.COMPLETE, _T)
    outputs = [models.S3File(file="bucket/f.txt", crc64nvme="aaaaaaaaaaaa")]
    sj = models.SubJob.model_construct(outputs=outputs)
    mongo.get_subjobs.return_value = [sj]
    s3obj = S3ObjectMeta("bucket/f.txt", "etag", 0, "bbbbbbbbbbbb")  # deliberate mismatch
    s3.get_object_meta.return_value = [s3obj]
    condor.get_cluster_classads.return_value = ([], [_CONDOR_AD])

    # asyncio.sleep is patched to make the _get_condor_stats polling loop instant
    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.update_container_state(
            _JOB, 0, models.JobState.COMPLETE, _update(outputs=outputs)
        )

    updates.get_parent_job_update.assert_called_once_with(_JOB, models.JobState.COMPLETE)
    condor.get_cluster_classads.assert_called_once_with(123)
    mongo.get_subjobs.assert_called_once_with("jid")
    s3.get_object_meta.assert_called_once_with(S3Paths(["bucket/f.txt"]))
    updates.handle_exception.assert_not_called()
    updates.update_job_state.assert_called_once_with(
        "jid",
        update_state.error(
            "Expected CRC64/NVME checksum aaaaaaaaaaaa but got bbbbbbbbbbbb "
            "for uploaded file bucket/f.txt",
            user_error="An unexpected error occurred",
        ),
    )


######
# recover_job tests
######


async def test_recover_job_bad_args():
    runner, _, _, _, _ = _make_runner()

    with pytest.raises(ValueError, match="^job is required$"):
        await runner.recover_job(None)


@pytest.mark.parametrize("cluster_ids", [None, []])
@pytest.mark.parametrize("force", [False, True])
async def test_recover_job_no_cluster_id(force, cluster_ids):
    runner, _, condor, updates, _ = _make_runner()
    job = _recovery_job(cluster_ids=cluster_ids)

    with pytest.raises(InvalidJobStateError, match="Job has no HTCondor cluster ID"):
        await runner.recover_job(job, force=force)

    condor.get_cluster_proc_states.assert_not_called()
    updates.update_job_state.assert_not_called()


@pytest.mark.parametrize("force", [False, True])
async def test_recover_job_other_proc_state(force):
    runner, _, condor, updates, _ = _make_runner()
    job = _recovery_job()
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.OTHER]

    with pytest.raises(
        InvalidJobStateError,
        match="HTCondor cluster contains processes in an unexpected state",
    ):
        await runner.recover_job(job, force=force)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.update_job_state.assert_not_called()


@pytest.mark.parametrize("force", [False, True])
async def test_recover_job_condor_raises(force):
    runner, _, condor, updates, _ = _make_runner()
    job = _recovery_job()
    condor.get_cluster_proc_states.side_effect = IOError("condor unavailable")

    with pytest.raises(IOError, match="condor unavailable"):
        await runner.recover_job(job, force=force)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.update_job_state.assert_not_called()


async def test_recover_job_standard_complete_state():
    runner, _, condor, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.COMPLETE)

    with pytest.raises(InvalidJobStateError, match="^Job has already completed successfully\\.$"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_not_called()
    updates.update_job_state.assert_not_called()


async def test_recover_job_standard_recovering_state():
    runner, _, condor, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.RECOVERING)

    with pytest.raises(
        InvalidJobStateError,
        match="^Job is already being recovered\\. If recovery is stuck, use force recovery\\.$",
    ):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_not_called()
    updates.update_job_state.assert_not_called()


async def test_recover_job_standard_canceled_state():
    runner, _, condor, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.CANCELED)

    with pytest.raises(InvalidJobStateError, match="^Job has already been canceled\\.$"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_not_called()
    updates.update_job_state.assert_not_called()


async def test_recover_job_standard_canceling_with_cluster_id():
    """CANCELING: refreshed job has a cluster ID → full cancel flow runs."""
    runner, mongo, condor, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.CANCELING)
    refreshed_job = _recovery_job(state=models.JobState.CANCELING)
    mongo.get_job.return_value = refreshed_job
    condor.get_cluster_classads.return_value = ([], [_CONDOR_AD])

    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.recover_job(job)

    mongo.get_job.assert_called_once_with("jid", as_admin=True)
    condor.cancel_job.assert_called_once_with(123)
    condor.get_cluster_classads.assert_called_once_with(123)
    updates.update_job_state.assert_called_once_with(
        "jid",
        update_state.canceled(cpu_hours=_CPU_HOURS, cpu_factor=_CPU_FACTOR, max_memory=_MAX_MEM),
    )


@pytest.mark.parametrize("cluster_ids", [
    pytest.param(None, id="no_htcondor_details"),
    pytest.param([], id="empty_cluster_id"),
])
async def test_recover_job_standard_canceling_no_cluster_id(cluster_ids):
    """
    CANCELING: refreshed job has no cluster ID (either no htcondor_details or empty cluster_id
    list) → condor skipped, CANCELED written with no stats.
    """
    runner, mongo, condor, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.CANCELING)
    refreshed_job = _recovery_job(state=models.JobState.CANCELING, cluster_ids=cluster_ids)
    mongo.get_job.return_value = refreshed_job

    await runner.recover_job(job)

    mongo.get_job.assert_called_once_with("jid", as_admin=True)
    condor.cancel_job.assert_not_called()
    condor.get_cluster_classads.assert_not_called()
    updates.update_job_state.assert_called_once_with(
        "jid",
        update_state.canceled(cpu_hours=None, cpu_factor=None, max_memory=None),
    )


async def test_recover_job_standard_canceling_error_propagates():
    """CANCELING: a transient error in _cancel_job propagates; job stays in CANCELING."""
    runner, mongo, _, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.CANCELING)
    refreshed_job = _recovery_job(state=models.JobState.CANCELING, cluster_ids=None)
    mongo.get_job.return_value = refreshed_job
    updates.update_job_state.side_effect = IOError("mongo unavailable")

    with pytest.raises(IOError, match="mongo unavailable"):
        await runner.recover_job(job)

    mongo.get_job.assert_called_once_with("jid", as_admin=True)
    updates.update_job_state.assert_called_once_with(
        "jid",
        update_state.canceled(cpu_hours=None, cpu_factor=None, max_memory=None),
    )


async def test_recover_job_error_state_advance_to_complete():
    """
    Job in ERROR, no held/running containers → RECOVERING lock acquired, job reset via
    mongo.recover_job, then advanced from DOWNLOAD_SUBMITTED all the way to COMPLETE.

    use_subjob_times=False is used: the original subjob timestamps predate the RECOVERING
    transition, so reusing them would make transition_times go backwards. _timestamp_fn is
    called for each state advance instead of get_parent_job_update.
    """
    lock_time = _T
    reset_time = _T + datetime.timedelta(seconds=1)
    # One fresh timestamp per state from JOB_SUBMITTING to COMPLETE (5 states).
    advance_times = [_T + datetime.timedelta(seconds=2 + i) for i in range(5)]
    ts = iter([lock_time, reset_time] + advance_times)
    runner, mongo, condor, updates, s3 = _make_runner(_timestamp_fn=lambda: next(ts))
    job = _recovery_job(state=models.JobState.ERROR)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]

    outputs = [models.S3File(file="bucket/f.txt", crc64nvme="aaaabbbbcccc")]
    sj = models.SubJob.model_construct(outputs=outputs)
    mongo.get_subjobs.return_value = [sj]
    s3.get_object_meta.return_value = [S3ObjectMeta("bucket/f.txt", "etag", 0, "aaaabbbbcccc")]
    condor.get_cluster_classads.return_value = ([], [_CONDOR_AD])

    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    condor.get_cluster_classads.assert_called_once_with(123)
    mongo.recover_job.assert_called_once_with("jid", reset_time, _TRANS_ID)
    mongo.get_subjobs.assert_called_once_with("jid")
    s3.get_object_meta.assert_called_once_with(S3Paths(["bucket/f.txt"]))
    updates.get_parent_job_update.assert_not_called()

    expected_update_calls = [
        call(
            "jid", update_state.recovering(),
            update_time=lock_time, recovery_cooldown=datetime.timedelta(0),
        ),
        call("jid", update_state.submitting_job(), update_time=advance_times[0]),
        call("jid", update_state.submitted_job(), update_time=advance_times[1]),
        call("jid", update_state.submitting_upload(), update_time=advance_times[2]),
        call("jid", update_state.submitted_upload(), update_time=advance_times[3]),
        call(
            "jid",
            update_state.complete(
                [models.S3File(file="bucket/f.txt", crc64nvme="aaaabbbbcccc")],
                cpu_hours=_CPU_HOURS, cpu_factor=_CPU_FACTOR, max_memory=_MAX_MEM,
            ),
            update_time=advance_times[4],
        ),
    ]
    updates.update_job_state.assert_has_calls(expected_update_calls)
    assert updates.update_job_state.call_count == len(expected_update_calls)


async def test_recover_job_error_state_advance_fails():
    """
    RECOVERING lock acquired and mongo.recover_job called, but _advance_job_to_complete
    fails; exception propagates; job is left in RECOVERING for admin to force-recover.
    """
    lock_time = _T
    reset_time = _T + datetime.timedelta(seconds=1)
    advance_time = _T + datetime.timedelta(seconds=2)
    ts = iter([lock_time, reset_time, advance_time])
    runner, mongo, condor, updates, s3 = _make_runner(_timestamp_fn=lambda: next(ts))
    job = _recovery_job(state=models.JobState.ERROR)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]
    # First call (RECOVERING lock) succeeds; second (first state advance) raises.
    updates.update_job_state.side_effect = [None, IOError("mongo blew up")]

    with pytest.raises(IOError, match="mongo blew up"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    condor.get_cluster_classads.assert_not_called()
    mongo.recover_job.assert_called_once_with("jid", reset_time, _TRANS_ID)
    mongo.get_subjobs.assert_not_called()
    s3.get_object_meta.assert_not_called()
    updates.get_parent_job_update.assert_not_called()
    updates.update_job_state.assert_has_calls([
        call(
            "jid", update_state.recovering(),
            update_time=lock_time, recovery_cooldown=datetime.timedelta(0),
        ),
        call("jid", update_state.submitting_job(), update_time=advance_time),
    ])
    assert updates.update_job_state.call_count == 2


async def test_recover_job_error_state_lock_fails():
    """
    Lock acquisition (ERROR → RECOVERING) fails (e.g. concurrent request won); exception
    propagates; mongo.recover_job is not called.
    """
    runner, mongo, condor, updates, s3 = _make_runner()
    job = _recovery_job(state=models.JobState.ERROR)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]
    updates.update_job_state.side_effect = InvalidJobStateError("concurrent recovery won")

    with pytest.raises(InvalidJobStateError, match="concurrent recovery won"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    condor.get_cluster_classads.assert_not_called()
    mongo.recover_job.assert_not_called()
    mongo.get_subjobs.assert_not_called()
    s3.get_object_meta.assert_not_called()
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.recovering(),
        update_time=_T, recovery_cooldown=datetime.timedelta(0),
    )


@pytest.mark.parametrize("start_state, state_updates", [
    (models.JobState.DOWNLOAD_SUBMITTED, [
        (models.JobState.JOB_SUBMITTING, update_state.submitting_job()),
        (models.JobState.JOB_SUBMITTED, update_state.submitted_job()),
        (models.JobState.UPLOAD_SUBMITTING, update_state.submitting_upload()),
        (models.JobState.UPLOAD_SUBMITTED, update_state.submitted_upload()),
    ]),
    (models.JobState.JOB_SUBMITTING, [
        (models.JobState.JOB_SUBMITTED, update_state.submitted_job()),
        (models.JobState.UPLOAD_SUBMITTING, update_state.submitting_upload()),
        (models.JobState.UPLOAD_SUBMITTED, update_state.submitted_upload()),
    ]),
    (models.JobState.JOB_SUBMITTED, [
        (models.JobState.UPLOAD_SUBMITTING, update_state.submitting_upload()),
        (models.JobState.UPLOAD_SUBMITTED, update_state.submitted_upload()),
    ]),
    (models.JobState.UPLOAD_SUBMITTING, [
        (models.JobState.UPLOAD_SUBMITTED, update_state.submitted_upload()),
    ]),
    (models.JobState.UPLOAD_SUBMITTED, []),
])
async def test_recover_job_standard_advance_to_complete(start_state, state_updates):
    runner, mongo, condor, updates, s3 = _make_runner()
    job = _recovery_job(state=start_state)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]
    complete_time = _T + datetime.timedelta(seconds=len(state_updates) + 1)
    parent_updates = [
        ParentJobUpdate(state, _T + datetime.timedelta(seconds=i + 1))
        for i, (state, _) in enumerate(state_updates)
    ] + [ParentJobUpdate(models.JobState.COMPLETE, complete_time)]
    updates.get_parent_job_update.side_effect = parent_updates

    outputs = [models.S3File(file="bucket/f.txt", crc64nvme="aaaabbbbcccc")]
    sj = models.SubJob.model_construct(outputs=outputs)
    mongo.get_subjobs.return_value = [sj]
    s3obj = S3ObjectMeta("bucket/f.txt", "etag", 0, "aaaabbbbcccc")
    s3.get_object_meta.return_value = [s3obj]
    condor.get_cluster_classads.return_value = ([], [_CONDOR_AD])

    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    condor.get_cluster_classads.assert_called_once_with(123)
    mongo.get_subjobs.assert_called_once_with("jid")
    s3.get_object_meta.assert_called_once_with(S3Paths(["bucket/f.txt"]))

    expected_times = [_T + datetime.timedelta(seconds=i + 1) for i in range(len(state_updates))]
    expected_update_calls = [
        call("jid", upd, update_time=t) for (_, upd), t in zip(state_updates, expected_times)
    ] + [call(
        "jid",
        update_state.complete(
            [models.S3File(file="bucket/f.txt", crc64nvme="aaaabbbbcccc")],
            cpu_hours=_CPU_HOURS,
            cpu_factor=_CPU_FACTOR,
            max_memory=_MAX_MEM,
        ),
        update_time=complete_time,
    )]
    updates.update_job_state.assert_has_calls(expected_update_calls)
    assert updates.update_job_state.call_count == len(expected_update_calls)

    expected_parent_calls = [
        call(job, state) for state, _ in state_updates
    ] + [call(job, models.JobState.COMPLETE)]
    updates.get_parent_job_update.assert_has_calls(expected_parent_calls)
    assert updates.get_parent_job_update.call_count == len(expected_parent_calls)


async def test_recover_job_complete_job_no_outputs():
    runner, mongo, condor, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.UPLOAD_SUBMITTED)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.COMPLETE, _T)
    sj = models.SubJob.model_construct(outputs=[])
    mongo.get_subjobs.return_value = [sj]
    condor.get_cluster_classads.return_value = ([], [_CONDOR_AD])

    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    condor.get_cluster_classads.assert_called_once_with(123)
    mongo.get_subjobs.assert_called_once_with("jid")
    updates.get_parent_job_update.assert_called_once_with(job, models.JobState.COMPLETE)
    updates.update_job_state.assert_called_once_with(
        "jid",
        update_state.error(
            "The job produced no output files",
            user_error="The job produced no output files",
        ),
    )


async def test_recover_job_complete_job_checksum_mismatch():
    runner, mongo, condor, updates, s3 = _make_runner()
    job = _recovery_job(state=models.JobState.UPLOAD_SUBMITTED)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.COMPLETE, _T)
    outputs = [models.S3File(file="bucket/f.txt", crc64nvme="aaaaaaaaaaaa")]
    sj = models.SubJob.model_construct(outputs=outputs)
    mongo.get_subjobs.return_value = [sj]
    s3obj = S3ObjectMeta("bucket/f.txt", "etag", 0, "bbbbbbbbbbbb")
    s3.get_object_meta.return_value = [s3obj]
    condor.get_cluster_classads.return_value = ([], [_CONDOR_AD])

    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    condor.get_cluster_classads.assert_called_once_with(123)
    mongo.get_subjobs.assert_called_once_with("jid")
    s3.get_object_meta.assert_called_once_with(S3Paths(["bucket/f.txt"]))
    updates.get_parent_job_update.assert_called_once_with(job, models.JobState.COMPLETE)
    updates.update_job_state.assert_called_once_with(
        "jid",
        update_state.error(
            "Expected CRC64/NVME checksum aaaaaaaaaaaa but got bbbbbbbbbbbb "
            "for uploaded file bucket/f.txt",
            user_error="An unexpected error occurred",
        ),
    )


async def test_recover_job_complete_job_condor_stats_timeout():
    """Unlike update_container_state, the IOError propagates directly to the caller."""
    runner, _, condor, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.UPLOAD_SUBMITTED)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]
    updates.get_parent_job_update.return_value = ParentJobUpdate(models.JobState.COMPLETE, _T)
    condor.get_cluster_classads.return_value = ([{"JobStatus": 2}], [])

    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        with pytest.raises(IOError, match="Condor jobs didn't complete"):
            await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.get_parent_job_update.assert_called_once_with(job, models.JobState.COMPLETE)
    assert condor.get_cluster_classads.call_count == 12
    condor.get_cluster_classads.assert_called_with(123)
    updates.update_job_state.assert_not_called()


async def test_recover_job_standard_advance_invalid_state():
    runner, _, condor, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.CREATED)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]

    with pytest.raises(RuntimeError, match="Unexpected job state"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.update_job_state.assert_not_called()


async def test_recover_job_standard_advance_update_raises():
    runner, _, condor, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.DOWNLOAD_SUBMITTED)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]
    updates.get_parent_job_update.return_value = ParentJobUpdate(
        models.JobState.JOB_SUBMITTING, _T
    )
    updates.update_job_state.side_effect = InvalidJobStateError("job was canceled")

    with pytest.raises(InvalidJobStateError, match="job was canceled"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.get_parent_job_update.assert_called_once_with(job, models.JobState.JOB_SUBMITTING)
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.submitting_job(), update_time=_T
    )


async def test_recover_job_standard_advance_parent_update_none():
    """
    get_parent_job_update returning None means not all subjobs reached that state;
    raise RuntimeError.
    """
    runner, _, condor, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.DOWNLOAD_SUBMITTED)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]
    updates.get_parent_job_update.return_value = None

    with pytest.raises(RuntimeError, match="Not all subjobs have reached state"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.get_parent_job_update.assert_called_once_with(job, models.JobState.JOB_SUBMITTING)
    updates.update_job_state.assert_not_called()


async def test_recover_job_standard_running_only():
    runner, _, condor, updates, _ = _make_runner()
    job = _recovery_job()
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.RUNNING]

    with pytest.raises(
        InvalidJobStateError,
        match="^No held containers to recover$",
    ):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.update_job_state.assert_not_called()


async def test_recover_job_standard_held_containers():
    """
    Held containers present → RECOVERING lock acquired; recover_subjobs resets held
    subjobs; release_job releases them; recover_job archives history and resets main job.
    """
    lock_time = _T
    reset_time = _T + datetime.timedelta(seconds=1)
    ts = iter([lock_time, reset_time])
    runner, mongo, condor, updates, _ = _make_runner(_timestamp_fn=lambda: next(ts))
    job = _recovery_job(state=models.JobState.JOB_SUBMITTING)
    # Container 0 complete, containers 1 and 2 held.
    condor.get_cluster_proc_states.return_value = [
        ProcState.COMPLETE, ProcState.HELD, ProcState.HELD
    ]

    await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.recovering(),
        update_time=lock_time, recovery_cooldown=datetime.timedelta(0),
    )
    mongo.recover_subjobs.assert_called_once_with("jid", [1, 2], lock_time, reset_time)
    condor.release_job.assert_called_once_with(123)
    mongo.recover_job.assert_called_once_with("jid", reset_time, _TRANS_ID)


async def test_recover_job_standard_held_release_fails():
    """
    Release_job raises → JobRecoveryError is raised; recover_job (main job reset) is not
    called; job is left in RECOVERING for the admin to force-recover.
    """
    lock_time = _T
    reset_time = _T + datetime.timedelta(seconds=1)
    ts = iter([lock_time, reset_time])
    runner, mongo, condor, updates, _ = _make_runner(_timestamp_fn=lambda: next(ts))
    job = _recovery_job(state=models.JobState.JOB_SUBMITTING)
    condor.get_cluster_proc_states.return_value = [ProcState.HELD, ProcState.HELD]
    condor.release_job.side_effect = IOError("condor unavailable")

    with pytest.raises(JobRecoveryError, match="Failed to release held HTCondor processes"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.recovering(),
        update_time=lock_time, recovery_cooldown=datetime.timedelta(0),
    )
    mongo.recover_subjobs.assert_called_once_with("jid", [0, 1], lock_time, reset_time)
    condor.release_job.assert_called_once_with(123)
    mongo.recover_job.assert_not_called()


async def test_recover_job_standard_held_lock_fails():
    """
    Lock acquisition fails (concurrent request won the RECOVERING transition) →
    InvalidJobStateError propagates; no subjob resets or HTC calls are made.
    """
    runner, mongo, condor, updates, _ = _make_runner()
    job = _recovery_job()
    condor.get_cluster_proc_states.return_value = [ProcState.HELD, ProcState.HELD]
    updates.update_job_state.side_effect = InvalidJobStateError("concurrent recovery won")

    with pytest.raises(InvalidJobStateError, match="concurrent recovery won"):
        await runner.recover_job(job)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.recovering(),
        update_time=_T, recovery_cooldown=datetime.timedelta(0),
    )
    mongo.recover_subjobs.assert_not_called()
    condor.release_job.assert_not_called()
    mongo.recover_job.assert_not_called()


async def test_recover_job_force_running_containers():
    runner, _, condor, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.RECOVERING)
    condor.get_cluster_proc_states.return_value = [ProcState.HELD, ProcState.RUNNING]

    with pytest.raises(
        InvalidJobStateError,
        match="^Cannot force recover while containers are running\\.",
    ):
        await runner.recover_job(job, force=True)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.update_job_state.assert_not_called()


async def test_recover_job_force_all_complete():
    """
    Force recovery, all containers complete (no held) → lock acquired with force_recovering()
    and _RECOVERY_COOLDOWN; mongo.recover_job archives history and resets to
    DOWNLOAD_SUBMITTED; then advanced from DOWNLOAD_SUBMITTED all the way to COMPLETE.

    use_subjob_times=False: subjob timestamps predate the current RECOVERING transition
    (they completed during the previous failed recovery window), so reusing them would make
    transition_times go backwards.
    """
    lock_time = _T
    reset_time = _T + datetime.timedelta(seconds=1)
    advance_times = [_T + datetime.timedelta(seconds=2 + i) for i in range(5)]
    ts = iter([lock_time, reset_time] + advance_times)
    runner, mongo, condor, updates, s3 = _make_runner(_timestamp_fn=lambda: next(ts))
    job = _recovery_job(state=models.JobState.RECOVERING)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]

    outputs = [models.S3File(file="bucket/f.txt", crc64nvme="aaaabbbbcccc")]
    sj = models.SubJob.model_construct(outputs=outputs)
    mongo.get_subjobs.return_value = [sj]
    s3.get_object_meta.return_value = [S3ObjectMeta("bucket/f.txt", "etag", 0, "aaaabbbbcccc")]
    condor.get_cluster_classads.return_value = ([], [_CONDOR_AD])

    with patch("cdmtaskservice.jobflows.kbase.asyncio.sleep"):
        await runner.recover_job(job, force=True)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    condor.get_cluster_classads.assert_called_once_with(123)
    mongo.recover_job.assert_called_once_with("jid", reset_time, _TRANS_ID)
    mongo.get_subjobs.assert_called_once_with("jid")
    s3.get_object_meta.assert_called_once_with(S3Paths(["bucket/f.txt"]))
    updates.get_parent_job_update.assert_not_called()

    expected_update_calls = [
        call(
            "jid", update_state.force_recovering(),
            update_time=lock_time, recovery_cooldown=datetime.timedelta(minutes=10),
        ),
        call("jid", update_state.submitting_job(), update_time=advance_times[0]),
        call("jid", update_state.submitted_job(), update_time=advance_times[1]),
        call("jid", update_state.submitting_upload(), update_time=advance_times[2]),
        call("jid", update_state.submitted_upload(), update_time=advance_times[3]),
        call(
            "jid",
            update_state.complete(
                [models.S3File(file="bucket/f.txt", crc64nvme="aaaabbbbcccc")],
                cpu_hours=_CPU_HOURS, cpu_factor=_CPU_FACTOR, max_memory=_MAX_MEM,
            ),
            update_time=advance_times[4],
        ),
    ]
    updates.update_job_state.assert_has_calls(expected_update_calls)
    assert updates.update_job_state.call_count == len(expected_update_calls)


async def test_recover_job_force_all_complete_advance_fails():
    """
    Force all-complete: lock acquired and mongo.recover_job called, but
    _advance_job_to_complete fails; exception propagates; job left in RECOVERING.
    """
    lock_time = _T
    reset_time = _T + datetime.timedelta(seconds=1)
    advance_time = _T + datetime.timedelta(seconds=2)
    ts = iter([lock_time, reset_time, advance_time])
    runner, mongo, condor, updates, s3 = _make_runner(_timestamp_fn=lambda: next(ts))
    job = _recovery_job(state=models.JobState.RECOVERING)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]
    # First call (force RECOVERING lock) succeeds; second (first state advance) raises.
    updates.update_job_state.side_effect = [None, IOError("mongo blew up")]

    with pytest.raises(IOError, match="mongo blew up"):
        await runner.recover_job(job, force=True)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    condor.get_cluster_classads.assert_not_called()
    mongo.recover_job.assert_called_once_with("jid", reset_time, _TRANS_ID)
    mongo.get_subjobs.assert_not_called()
    s3.get_object_meta.assert_not_called()
    updates.get_parent_job_update.assert_not_called()
    updates.update_job_state.assert_has_calls([
        call(
            "jid", update_state.force_recovering(),
            update_time=lock_time, recovery_cooldown=datetime.timedelta(minutes=10),
        ),
        call("jid", update_state.submitting_job(), update_time=advance_time),
    ])
    assert updates.update_job_state.call_count == 2


async def test_recover_job_force_all_complete_lock_fails():
    """
    Force all-complete: lock acquisition fails (job not in RECOVERING or cooldown not yet
    expired); exception propagates; mongo.recover_job is not called.
    """
    runner, mongo, condor, updates, s3 = _make_runner()
    job = _recovery_job(state=models.JobState.RECOVERING)
    condor.get_cluster_proc_states.return_value = [ProcState.COMPLETE, ProcState.COMPLETE]
    updates.update_job_state.side_effect = InvalidJobStateError("not in RECOVERING state")

    with pytest.raises(InvalidJobStateError, match="not in RECOVERING state"):
        await runner.recover_job(job, force=True)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    condor.get_cluster_classads.assert_not_called()
    mongo.recover_job.assert_not_called()
    mongo.get_subjobs.assert_not_called()
    s3.get_object_meta.assert_not_called()
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.force_recovering(),
        update_time=_T, recovery_cooldown=datetime.timedelta(minutes=10),
    )


async def test_recover_job_force_held_containers():
    """
    Force recovery with held containers → lock acquired with force_recovering() and
    _RECOVERY_COOLDOWN; recover_subjobs resets held subjobs; release_job releases them;
    recover_job archives history and resets main job.
    """
    lock_time = _T
    reset_time = _T + datetime.timedelta(seconds=1)
    ts = iter([lock_time, reset_time])
    runner, mongo, condor, updates, _ = _make_runner(_timestamp_fn=lambda: next(ts))
    job = _recovery_job(state=models.JobState.RECOVERING)
    # Container 0 complete, containers 1 and 2 held.
    condor.get_cluster_proc_states.return_value = [
        ProcState.COMPLETE, ProcState.HELD, ProcState.HELD
    ]

    await runner.recover_job(job, force=True)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.force_recovering(),
        update_time=lock_time, recovery_cooldown=datetime.timedelta(minutes=10),
    )
    mongo.recover_subjobs.assert_called_once_with("jid", [1, 2], lock_time, reset_time)
    condor.release_job.assert_called_once_with(123)
    mongo.recover_job.assert_called_once_with("jid", reset_time, _TRANS_ID)


async def test_recover_job_force_held_release_fails():
    """
    Force held: release_job raises → JobRecoveryError is raised; recover_job (main job
    reset) is not called; job is left in RECOVERING for the admin to retry.
    """
    lock_time = _T
    reset_time = _T + datetime.timedelta(seconds=1)
    ts = iter([lock_time, reset_time])
    runner, mongo, condor, updates, _ = _make_runner(_timestamp_fn=lambda: next(ts))
    job = _recovery_job(state=models.JobState.RECOVERING)
    condor.get_cluster_proc_states.return_value = [ProcState.HELD, ProcState.HELD]
    condor.release_job.side_effect = IOError("condor unavailable")

    with pytest.raises(JobRecoveryError, match="Failed to release held HTCondor processes"):
        await runner.recover_job(job, force=True)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.force_recovering(),
        update_time=lock_time, recovery_cooldown=datetime.timedelta(minutes=10),
    )
    mongo.recover_subjobs.assert_called_once_with("jid", [0, 1], lock_time, reset_time)
    condor.release_job.assert_called_once_with(123)
    mongo.recover_job.assert_not_called()


async def test_recover_job_force_held_lock_fails():
    """
    Force held: lock acquisition fails (job not in RECOVERING or cooldown not yet expired);
    exception propagates; no subjob resets or HTC calls are made.
    """
    runner, mongo, condor, updates, _ = _make_runner()
    job = _recovery_job(state=models.JobState.RECOVERING)
    condor.get_cluster_proc_states.return_value = [ProcState.HELD, ProcState.HELD]
    updates.update_job_state.side_effect = InvalidJobStateError("cooldown not expired")

    with pytest.raises(InvalidJobStateError, match="cooldown not expired"):
        await runner.recover_job(job, force=True)

    condor.get_cluster_proc_states.assert_called_once_with(123)
    updates.update_job_state.assert_called_once_with(
        "jid", update_state.force_recovering(),
        update_time=_T, recovery_cooldown=datetime.timedelta(minutes=10),
    )
    mongo.recover_subjobs.assert_not_called()
    condor.release_job.assert_not_called()
    mongo.recover_job.assert_not_called()
