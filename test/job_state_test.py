import pytest
from unittest.mock import create_autospec, AsyncMock

from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import IllegalParameterError
from cdmtaskservice.images import Images
from cdmtaskservice.job_state import JobState, NoJobLogsError
from cdmtaskservice.jobflows.flowmanager import JobFlowManager
from cdmtaskservice import models
from cdmtaskservice import sites
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice.refdata import Refdata
from cdmtaskservice.s3.client import S3Client, S3ObjectMeta
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.user import CTSUser


# TODO add more tests


_USER = CTSUser(user="testuser")
_JOB_ID = "test-job-id"
_LOG_PATH = "cts-logs/container_logs/test-job-id"
_S3_STDOUT_PATH_0 = S3Paths([f"{_LOG_PATH}/container-0-stdout.txt"])
_S3_STDERR_PATH_1 = S3Paths([f"{_LOG_PATH}/container-1-stderr.txt"])


def _make_job(cluster, num_containers=2, logpath: str | None = _LOG_PATH):
    return models.AdminJobDetails.model_construct(
        user=_USER.user,
        logpath=logpath,
        job_input=models.JobInput.model_construct(
            cluster=cluster,
            num_containers=num_containers,
        ),
    )


def _make_job_state():
    mongo = create_autospec(MongoDAO, spec_set=True, instance=True)
    s3 = create_autospec(S3Client, spec_set=True, instance=True)
    jobstate = JobState(
        mongo=mongo,
        s3client=s3,
        images=create_autospec(Images, spec_set=True, instance=True),
        kafka=create_autospec(KafkaNotifier, spec_set=True, instance=True),
        refdata=create_autospec(Refdata, spec_set=True, instance=True),
        coro_manager=create_autospec(CoroutineWrangler, spec_set=True, instance=True),
        flow_manager=create_autospec(JobFlowManager, spec_set=True, instance=True),
        allowed_paths=[],
        log_path="cts-logs/",
        job_max_cpu_hours=100,
    )
    return jobstate, mongo, s3


async def test_stream_job_logs_no_logpath():
    jobstate, mongo, _ = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.KBASE, logpath=None)

    with pytest.raises(NoJobLogsError, match=f"Job ID {_JOB_ID} has no logs available"):
        await jobstate.stream_job_logs(_JOB_ID, 0, _USER)

    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)


async def test_stream_job_logs_container_num_too_large():
    jobstate, mongo, _ = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.KBASE, num_containers=2)

    with pytest.raises(IllegalParameterError, match="Container number must be < 2"):
        await jobstate.stream_job_logs(_JOB_ID, 2, _USER)

    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)


async def test_stream_job_logs_kbase_container_succeeded():
    jobstate, mongo, _ = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.KBASE)
    mongo.get_subjob.return_value = models.SubJob.model_construct(sub_id=0, exit_code=0)

    with pytest.raises(
        NoJobLogsError,
        match="logs are only uploaded for containers that exit with an error code"
    ):
        await jobstate.stream_job_logs(_JOB_ID, 0, _USER)

    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    mongo.get_subjob.assert_called_once_with(_JOB_ID, 0)


async def test_stream_job_logs_kbase_container_exit_code_none():
    jobstate, mongo, _ = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.KBASE)
    mongo.get_subjob.return_value = models.SubJob.model_construct(sub_id=0, exit_code=None)

    with pytest.raises(
        NoJobLogsError,
        match="logs are only uploaded for containers that exit with an error code"
    ):
        await jobstate.stream_job_logs(_JOB_ID, 0, _USER)

    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    mongo.get_subjob.assert_called_once_with(_JOB_ID, 0)


async def test_stream_job_logs_kbase_container_errored():
    jobstate, mongo, s3 = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.KBASE)
    mongo.get_subjob.return_value = models.SubJob.model_construct(sub_id=0, exit_code=1)
    sentinel = AsyncMock()
    s3.stream_object.return_value = sentinel

    gen, filename = await jobstate.stream_job_logs(_JOB_ID, 0, _USER)

    assert gen is sentinel
    assert filename == "container-0-stdout.txt"
    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    mongo.get_subjob.assert_called_once_with(_JOB_ID, 0)
    s3.get_object_meta.assert_not_called()
    s3.stream_object.assert_called_once_with(_S3_STDOUT_PATH_0, seek=None, length=None)


async def test_stream_job_logs_nersc_no_subjob_check():
    jobstate, mongo, s3 = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)
    sentinel = AsyncMock()
    s3.stream_object.return_value = sentinel

    gen, filename = await jobstate.stream_job_logs(_JOB_ID, 0, _USER)

    assert gen is sentinel
    assert filename == "container-0-stdout.txt"
    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    mongo.get_subjob.assert_not_called()
    s3.get_object_meta.assert_not_called()
    s3.stream_object.assert_called_once_with(_S3_STDOUT_PATH_0, seek=None, length=None)


async def test_stream_job_logs_stderr():
    jobstate, mongo, s3 = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)
    sentinel = AsyncMock()
    s3.stream_object.return_value = sentinel

    gen, filename = await jobstate.stream_job_logs(_JOB_ID, 1, _USER, stderr=True)

    assert gen is sentinel
    assert filename == "container-1-stderr.txt"
    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    s3.stream_object.assert_called_once_with(_S3_STDERR_PATH_1, seek=None, length=None)


async def test_stream_job_logs_seek_negative():
    jobstate, mongo, _ = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)

    with pytest.raises(IllegalParameterError, match="Seek parameter must be >= 0"):
        await jobstate.stream_job_logs(_JOB_ID, 0, _USER, seek=-1)

    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)


async def test_stream_job_logs_seek_zero_no_meta_call():
    jobstate, mongo, s3 = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)
    s3.stream_object.return_value = AsyncMock()

    await jobstate.stream_job_logs(_JOB_ID, 0, _USER, seek=0)

    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    s3.get_object_meta.assert_not_called()
    s3.stream_object.assert_called_once_with(_S3_STDOUT_PATH_0, seek=0, length=None)


async def test_stream_job_logs_seek_at_eof():
    jobstate, mongo, s3 = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)
    s3.get_object_meta.return_value = [S3ObjectMeta("path", "etag", 100)]

    with pytest.raises(IllegalParameterError, match="Seek parameter 100 is >= file size 100"):
        await jobstate.stream_job_logs(_JOB_ID, 0, _USER, seek=100)

    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    s3.get_object_meta.assert_called_once_with(_S3_STDOUT_PATH_0)
    s3.stream_object.assert_not_called()


async def test_stream_job_logs_seek_valid():
    jobstate, mongo, s3 = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)
    s3.get_object_meta.return_value = [S3ObjectMeta("path", "etag", 100)]
    sentinel = AsyncMock()
    s3.stream_object.return_value = sentinel

    gen, _ = await jobstate.stream_job_logs(_JOB_ID, 0, _USER, seek=50)

    assert gen is sentinel
    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    s3.get_object_meta.assert_called_once_with(_S3_STDOUT_PATH_0)
    s3.stream_object.assert_called_once_with(_S3_STDOUT_PATH_0, seek=50, length=None)


async def test_stream_job_logs_length_zero():
    jobstate, mongo, _ = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)

    with pytest.raises(IllegalParameterError, match="Length parameter must be >= 1"):
        await jobstate.stream_job_logs(_JOB_ID, 0, _USER, length=0)

    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)


async def test_stream_job_logs_length_valid():
    jobstate, mongo, s3 = _make_job_state()
    mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)
    sentinel = AsyncMock()
    s3.stream_object.return_value = sentinel

    gen, _ = await jobstate.stream_job_logs(_JOB_ID, 0, _USER, length=50)

    assert gen is sentinel
    mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    s3.get_object_meta.assert_not_called()
    s3.stream_object.assert_called_once_with(_S3_STDOUT_PATH_0, seek=None, length=50)
