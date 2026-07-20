import datetime
import pytest
import uuid
from dataclasses import dataclass
from unittest.mock import ANY, create_autospec, AsyncMock

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
from cdmtaskservice.timestamp import utcdatetime
from cdmtaskservice.user import CTSUser


# TODO add more tests


_USER = CTSUser(user="testuser")
_JOB_ID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
_TRANS_ID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
_T = utcdatetime()
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


@dataclass
class _JobStateMocks:
    jobstate: JobState
    mongo: MongoDAO
    s3: S3Client
    images: Images
    kafka: KafkaNotifier


def _make_job_state(
    job_max_cpu_hours: float = 100,
    test_mode: bool = False,
) -> _JobStateMocks:
    mongo = create_autospec(MongoDAO, spec_set=True, instance=True)
    s3 = create_autospec(S3Client, spec_set=True, instance=True)
    images = create_autospec(Images, spec_set=True, instance=True)
    kafka = create_autospec(KafkaNotifier, spec_set=True, instance=True)
    jobstate = JobState(
        mongo=mongo,
        s3client=s3,
        images=images,
        kafka=kafka,
        refdata=create_autospec(Refdata, spec_set=True, instance=True),
        coro_manager=create_autospec(CoroutineWrangler, spec_set=True, instance=True),
        flow_manager=create_autospec(JobFlowManager, spec_set=True, instance=True),
        allowed_paths=[],
        log_path="cts-logs/",
        job_max_cpu_hours=job_max_cpu_hours,
        test_mode=test_mode,
        _timestamp_fn=lambda: _T,
        _uuid_fn=iter([uuid.UUID(_JOB_ID), uuid.UUID(_TRANS_ID)]).__next__,
    )
    return _JobStateMocks(jobstate=jobstate, mongo=mongo, s3=s3, images=images, kafka=kafka)


async def test_stream_job_logs_no_logpath():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.KBASE, logpath=None)

    with pytest.raises(NoJobLogsError, match=f"Job ID {_JOB_ID} has no logs available"):
        await js.jobstate.stream_job_logs(_JOB_ID, 0, _USER)

    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)


async def test_stream_job_logs_container_num_too_large():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.KBASE, num_containers=2)

    with pytest.raises(IllegalParameterError, match="Container number must be < 2"):
        await js.jobstate.stream_job_logs(_JOB_ID, 2, _USER)

    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)


async def test_stream_job_logs_kbase_container_succeeded():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.KBASE)
    js.mongo.get_subjob.return_value = models.SubJob.model_construct(sub_id=0, exit_code=0)

    with pytest.raises(
        NoJobLogsError,
        match="logs are only uploaded for containers that exit with an error code"
    ):
        await js.jobstate.stream_job_logs(_JOB_ID, 0, _USER)

    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    js.mongo.get_subjob.assert_called_once_with(_JOB_ID, 0)


async def test_stream_job_logs_kbase_container_exit_code_none():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.KBASE)
    js.mongo.get_subjob.return_value = models.SubJob.model_construct(sub_id=0, exit_code=None)

    with pytest.raises(
        NoJobLogsError,
        match="logs are only uploaded for containers that exit with an error code"
    ):
        await js.jobstate.stream_job_logs(_JOB_ID, 0, _USER)

    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    js.mongo.get_subjob.assert_called_once_with(_JOB_ID, 0)


async def test_stream_job_logs_kbase_container_errored():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.KBASE)
    js.mongo.get_subjob.return_value = models.SubJob.model_construct(sub_id=0, exit_code=1)
    sentinel = AsyncMock()
    js.s3.stream_object.return_value = sentinel

    gen, filename = await js.jobstate.stream_job_logs(_JOB_ID, 0, _USER)

    assert gen is sentinel
    assert filename == "container-0-stdout.txt"
    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    js.mongo.get_subjob.assert_called_once_with(_JOB_ID, 0)
    js.s3.get_object_meta.assert_not_called()
    js.s3.stream_object.assert_called_once_with(_S3_STDOUT_PATH_0, seek=None, length=None)


async def test_stream_job_logs_nersc_no_subjob_check():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)
    sentinel = AsyncMock()
    js.s3.stream_object.return_value = sentinel

    gen, filename = await js.jobstate.stream_job_logs(_JOB_ID, 0, _USER)

    assert gen is sentinel
    assert filename == "container-0-stdout.txt"
    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    js.mongo.get_subjob.assert_not_called()
    js.s3.get_object_meta.assert_not_called()
    js.s3.stream_object.assert_called_once_with(_S3_STDOUT_PATH_0, seek=None, length=None)


async def test_stream_job_logs_stderr():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)
    sentinel = AsyncMock()
    js.s3.stream_object.return_value = sentinel

    gen, filename = await js.jobstate.stream_job_logs(_JOB_ID, 1, _USER, stderr=True)

    assert gen is sentinel
    assert filename == "container-1-stderr.txt"
    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    js.s3.stream_object.assert_called_once_with(_S3_STDERR_PATH_1, seek=None, length=None)


async def test_stream_job_logs_seek_negative():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)

    with pytest.raises(IllegalParameterError, match="Seek parameter must be >= 0"):
        await js.jobstate.stream_job_logs(_JOB_ID, 0, _USER, seek=-1)

    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)


async def test_stream_job_logs_seek_zero_no_meta_call():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)
    js.s3.stream_object.return_value = AsyncMock()

    await js.jobstate.stream_job_logs(_JOB_ID, 0, _USER, seek=0)

    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    js.s3.get_object_meta.assert_not_called()
    js.s3.stream_object.assert_called_once_with(_S3_STDOUT_PATH_0, seek=0, length=None)


async def test_stream_job_logs_seek_at_eof():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)
    js.s3.get_object_meta.return_value = [S3ObjectMeta("path", "etag", 100)]

    with pytest.raises(IllegalParameterError, match="Seek parameter 100 is >= file size 100"):
        await js.jobstate.stream_job_logs(_JOB_ID, 0, _USER, seek=100)

    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    js.s3.get_object_meta.assert_called_once_with(_S3_STDOUT_PATH_0)
    js.s3.stream_object.assert_not_called()


async def test_stream_job_logs_seek_valid():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)
    js.s3.get_object_meta.return_value = [S3ObjectMeta("path", "etag", 100)]
    sentinel = AsyncMock()
    js.s3.stream_object.return_value = sentinel

    gen, _ = await js.jobstate.stream_job_logs(_JOB_ID, 0, _USER, seek=50)

    assert gen is sentinel
    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    js.s3.get_object_meta.assert_called_once_with(_S3_STDOUT_PATH_0)
    js.s3.stream_object.assert_called_once_with(_S3_STDOUT_PATH_0, seek=50, length=None)


async def test_stream_job_logs_length_zero():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)

    with pytest.raises(IllegalParameterError, match="Length parameter must be >= 1"):
        await js.jobstate.stream_job_logs(_JOB_ID, 0, _USER, length=0)

    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)


async def test_stream_job_logs_length_valid():
    js = _make_job_state()
    js.mongo.get_job.return_value = _make_job(sites.Cluster.PERLMUTTER_JAWS)
    sentinel = AsyncMock()
    js.s3.stream_object.return_value = sentinel

    gen, _ = await js.jobstate.stream_job_logs(_JOB_ID, 0, _USER, length=50)

    assert gen is sentinel
    js.mongo.get_job.assert_called_once_with(_JOB_ID, as_admin=False)
    js.s3.get_object_meta.assert_not_called()
    js.s3.stream_object.assert_called_once_with(_S3_STDOUT_PATH_0, seek=None, length=50)


# KBase CPU node type limits: cpus_per_node=168, memory_per_node_gb=990, max_runtime_min=10080
_KBASE_CPU_MAX_CPUS = 84 * 2
_KBASE_CPU_MAX_MEM_BYTES = 990 * 1_000_000_000

# KBase GPU node type limits: cpus_per_node=256, memory_per_node_gb=990, gpus_per_node=4
_KBASE_GPU_MAX_CPUS = 256
_KBASE_GPU_MAX_MEM_BYTES = 990 * 1_000_000_000
_KBASE_GPU_MAX_GPUS = 4

# Runtime limits are the same between KBase node types
_KBASE_CPU_MAX_RUNTIME_MIN = 7 * 24 * 60
_KBASE_GPU_MAX_RUNTIME_MIN = 7 * 24 * 60

_INPUT_FILE = "mybucket/input/file.txt"
_OUTPUT_DIR = "mybucket/output/"
_CRC = "abc123=="
_TEST_IMAGE = models.Image.model_construct(
    registered_by="testuser",
    registered_on=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    name="ghcr.io/kbase/testimage",
    digest="sha256:" + "a" * 64,
    entrypoint=["run"],
    tag="0.1.0",
    refdata_id=None,
    default_refdata_mount_point=None,
    urls=None,
    usage_notes=None,
)
_TEST_JOB_IMAGE = models.JobImage(
    registered_by="testuser",
    registered_on=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    name="ghcr.io/kbase/testimage",
    digest="sha256:" + "a" * 64,
    entrypoint=["run"],
    tag="0.1.0",
    refdata_id=None,
    default_refdata_mount_point=None,
)


def _make_job_input(
    cluster=sites.Cluster.KBASE,
    cpus=1,
    gpus=0,
    memory_bytes=1_000_000_000,
    runtime_sec=3600,
    num_containers=1,
    input_files=None,
    output_dir=_OUTPUT_DIR,
    image="ghcr.io/kbase/testimage:latest",
):
    return models.JobInput.model_construct(
        cluster=cluster,
        cpus=cpus,
        gpus=gpus,
        memory=memory_bytes,
        runtime=datetime.timedelta(seconds=runtime_sec),
        num_containers=num_containers,
        input_files=input_files or [_INPUT_FILE],
        output_dir=output_dir,
        image=image,
        params=models.Parameters(),
    )


async def test_submit_null_job_input():
    js = _make_job_state()

    with pytest.raises(ValueError, match="job_input is required"):
        await js.jobstate.submit(None, _USER)


async def test_submit_null_user():
    js = _make_job_state()

    with pytest.raises(ValueError, match="user is required"):
        await js.jobstate.submit(_make_job_input(), None)


async def test_submit_image_allowed_at_site():
    js = _make_job_state(test_mode=True)
    job_input = _make_job_input(cluster=sites.Cluster.KBASE)
    image = _TEST_IMAGE.model_copy(update={"allowed_sites": [sites.SubmittableCluster.KBASE]})

    await _check_submit_succeeds(js, job_input, image=image)


async def test_submit_image_not_allowed_at_site():
    js = _make_job_state()
    job_input = _make_job_input(cluster=sites.Cluster.KBASE)
    js.images.get_image.return_value = _TEST_IMAGE.model_copy(
        update={"allowed_sites": [sites.SubmittableCluster.PERLMUTTER_JAWS]}
    )

    with pytest.raises(IllegalParameterError) as exc_info:
        await js.jobstate.submit(job_input, _USER)
    assert str(exc_info.value) == (
        "Image ghcr.io/kbase/testimage@sha256:" + "a" * 64
        + " is not permitted to run at site kbase"
    )


async def test_submit_compute_time_exceeds_limit():
    # 1 cpu * 1 container * 360001 sec / 3600 sec/hr = 100.000... hrs > default limit of 100
    js = _make_job_state(job_max_cpu_hours=100)
    job_input = _make_job_input(cpus=1, runtime_sec=360001)

    with pytest.raises(IllegalParameterError) as exc_info:
        await js.jobstate.submit(job_input, _USER)
    assert str(exc_info.value) == (
        "Job compute time of 100.001 CPU hours is greater than the limit of 100"
    )


async def test_submit_cpus_exceed_site_limit():
    js = _make_job_state()
    job_input = _make_job_input(cpus=_KBASE_CPU_MAX_CPUS + 1)

    with pytest.raises(IllegalParameterError) as exc_info:
        await js.jobstate.submit(job_input, _USER)
    assert str(exc_info.value) == (
        "No node type at site kbase can satisfy the requested resources "
        "(cpus=169, gpus=0, mem=1.0GB, runtime=60.0min)."
    )


async def test_submit_memory_exceeds_site_limit():
    js = _make_job_state()
    job_input = _make_job_input(memory_bytes=_KBASE_CPU_MAX_MEM_BYTES + 1)

    with pytest.raises(IllegalParameterError) as exc_info:
        await js.jobstate.submit(job_input, _USER)
    assert str(exc_info.value) == (
        "No node type at site kbase can satisfy the requested resources "
        "(cpus=1, gpus=0, mem=990.001GB, runtime=60.0min)."
    )


async def test_submit_runtime_exceeds_site_limit():
    js = _make_job_state()
    job_input = _make_job_input(runtime_sec=(_KBASE_CPU_MAX_RUNTIME_MIN * 60) + 1)

    with pytest.raises(IllegalParameterError) as exc_info:
        await js.jobstate.submit(job_input, _USER)
    assert str(exc_info.value) == (
        "No node type at site kbase can satisfy the requested resources "
        "(cpus=1, gpus=0, mem=1.0GB, runtime=10080.017min)."
    )


async def test_submit_gpus_exceed_site_limit():
    js = _make_job_state()
    job_input = _make_job_input(
        cluster=sites.Cluster.KBASE,
        gpus=_KBASE_GPU_MAX_GPUS + 1,
    )

    with pytest.raises(IllegalParameterError) as exc_info:
        await js.jobstate.submit(job_input, _USER)
    assert str(exc_info.value) == (
        "No node type at site kbase can satisfy the requested resources "
        "(cpus=1, gpus=5, mem=1.0GB, runtime=60.0min)."
    )


async def _check_submit_succeeds(js, job_input, image=_TEST_IMAGE):
    js.images.get_image.return_value = image
    js.s3.get_object_meta.return_value = [S3ObjectMeta(_INPUT_FILE, "etag", 1000, crc64nvme=_CRC)]

    job_id = await js.jobstate.submit(job_input, _USER)

    assert job_id == _JOB_ID

    js.images.get_image.assert_called_once_with(job_input.image)
    js.s3.is_paths_writeable.assert_called_once_with(
        S3Paths([_OUTPUT_DIR], no_index_in_errors=True)
    )
    expected_input_file = models.S3FileWithDataID.model_construct(
        file=_INPUT_FILE, crc64nvme=_CRC, data_id=None
    )
    expected_ji = job_input.model_copy(update={"input_files": [expected_input_file]})
    js.mongo.save_job.assert_called_once_with(
        models.AdminJobDetails(
            id=_JOB_ID,
            job_input=expected_ji,
            user="testuser",
            image=_TEST_JOB_IMAGE,
            input_file_count=1,
            state=models.JobState.CREATED,
            transition_times=[models.AdminJobStateTransition(
                state=models.JobState.CREATED,
                time=_T,
                trans_id=_TRANS_ID,
                notif_sent=False,
            )]
        )
    )
    js.kafka.update_job_state.assert_called_once_with(
        _JOB_ID, models.JobState.CREATED, _T, _TRANS_ID, callback=ANY
    )
    await js.kafka.update_job_state.call_args.kwargs["callback"]
    js.mongo.job_update_sent.assert_called_once_with(_JOB_ID, _TRANS_ID)
    js.s3.get_object_meta.assert_called_once_with(S3Paths([_INPUT_FILE]))


async def test_submit_kbase_cpu_at_site_limits():
    # compute_time = 168 cpus * 1 container * (10080 min * 60 sec/min) / 3600 sec/hr = 28224 hrs
    js = _make_job_state(job_max_cpu_hours=28225, test_mode=True)
    job_input = _make_job_input(
        cpus=_KBASE_CPU_MAX_CPUS,
        gpus=0,
        memory_bytes=_KBASE_CPU_MAX_MEM_BYTES,
        runtime_sec=_KBASE_CPU_MAX_RUNTIME_MIN * 60,
    )

    await _check_submit_succeeds(js, job_input)


async def test_submit_kbase_gpu_at_site_limits():
    # compute_time = 256 cpus * 1 container * (10080 min * 60 sec/min) / 3600 sec/hr = 43008 hrs
    js = _make_job_state(job_max_cpu_hours=43009, test_mode=True)
    job_input = _make_job_input(
        cpus=_KBASE_GPU_MAX_CPUS,
        gpus=_KBASE_GPU_MAX_GPUS,
        memory_bytes=_KBASE_GPU_MAX_MEM_BYTES,
        runtime_sec=_KBASE_GPU_MAX_RUNTIME_MIN * 60,
    )

    await _check_submit_succeeds(js, job_input)
