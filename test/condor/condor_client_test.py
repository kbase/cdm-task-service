import pytest
import tempfile
from unittest.mock import create_autospec
import htcondor2

from classad2 import ClassAd, ExprTree

from cdmtaskservice.condor.client import CondorClient, ProcState, _RETURNED_JOB_ADS
from cdmtaskservice.condor.config import CondorClientConfig
from cdmtaskservice.config_s3 import S3Config


# TODO TEST add more tests


def _make_client():
    schedd = create_autospec(htcondor2.Schedd, instance=True)
    config = CondorClientConfig(
        initial_dir=tempfile.mkdtemp(),
        service_root_url="http://localhost:5000",
        executable_url_override="http://example.com/run_job.sh",
        code_archive_url_override="http://example.com/code.tgz",
        client_group=None,
        token_path="/token",
        s3_access_secret_path="/secret",
        job_update_timeout_min=1,
        mount_prefix_override=None,
        additional_path=None,
        cache_dir="/cache",
        refdata_host_path="/refdata",
        heartbeat_interval_min=5,
    )
    s3config = create_autospec(S3Config, spec_set=True, instance=True)
    client = CondorClient(schedd, config, s3config)
    return client, schedd


async def test_get_container_classad_bad_args():
    client, _ = _make_client()
    with pytest.raises(ValueError, match="^cluster_id is required$"):
        await client.get_container_classad(None, 0)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await client.get_container_classad(0, 0)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await client.get_container_classad(-1, 0)
    with pytest.raises(ValueError, match="^container_number is required$"):
        await client.get_container_classad(123, None)
    with pytest.raises(ValueError, match="^container_number must be >= 0$"):
        await client.get_container_classad(123, -1)


async def test_get_container_classad_no_records():
    client, schedd = _make_client()
    schedd.query.return_value = []
    schedd.history.return_value = []

    with pytest.raises(
        ValueError, match="^No record found for cluster ID 123 and container number 0$"
    ):
        await client.get_container_classad(123, 0)

    constraint = "ClusterId == 123 && CTSContainerNumber == 0"
    schedd.query.assert_called_once_with(constraint=constraint, projection=_RETURNED_JOB_ADS)
    schedd.history.assert_called_once_with(constraint=constraint, projection=_RETURNED_JOB_ADS)


async def test_get_container_classad_in_active():
    """Active queue hit — history never consulted; MemoryUsage eval'd, ExprTree stringified."""
    client, schedd = _make_client()
    ca = ClassAd(
        "[ClusterId = 123; ProcId = 1; JobStatus = 2;"
        " MemoryUsage = ceiling(ResidentSetSize_RAW / 1024.0); ResidentSetSize_RAW = 524288;"
        ' Requirements = (TARGET.Arch == "X86_64")]'
    )
    assert isinstance(ca["ClusterId"], int)
    assert isinstance(ca["MemoryUsage"], ExprTree)
    assert isinstance(ca["Requirements"], ExprTree)
    schedd.query.return_value = [ca]

    result = await client.get_container_classad(123, 1)

    assert result == {
        "ClusterId": 123,
        "ProcId": 1,
        "JobStatus": 2,
        "MemoryUsage": 512,
        "ResidentSetSize_RAW": 524288,
        "Requirements": '(TARGET.Arch == "X86_64")',
    }
    schedd.query.assert_called_once_with(
        constraint="ClusterId == 123 && CTSContainerNumber == 1",
        projection=_RETURNED_JOB_ADS,
    )
    schedd.history.assert_not_called()


async def test_get_container_classad_in_history():
    """Active queue miss — falls back to history; MemoryUsage eval'd, ExprTree stringified."""
    client, schedd = _make_client()
    ca = ClassAd(
        "[ClusterId = 123; ProcId = 0; JobStatus = 4;"
        " MemoryUsage = ceiling(ResidentSetSize_RAW / 1024.0); ResidentSetSize_RAW = 524288;"
        ' Requirements = (TARGET.Arch == "X86_64")]'
    )
    assert isinstance(ca["ClusterId"], int)
    assert isinstance(ca["MemoryUsage"], ExprTree)
    assert isinstance(ca["Requirements"], ExprTree)
    schedd.query.return_value = []
    schedd.history.return_value = [ca]

    result = await client.get_container_classad(123, 0)

    assert result == {
        "ClusterId": 123,
        "ProcId": 0,
        "JobStatus": 4,
        "MemoryUsage": 512,
        "ResidentSetSize_RAW": 524288,
        "Requirements": '(TARGET.Arch == "X86_64")',
    }
    constraint = "ClusterId == 123 && CTSContainerNumber == 0"
    schedd.query.assert_called_once_with(constraint=constraint, projection=_RETURNED_JOB_ADS)
    schedd.history.assert_called_once_with(constraint=constraint, projection=_RETURNED_JOB_ADS)


async def test_get_cluster_classads_bad_args():
    client, _ = _make_client()
    with pytest.raises(ValueError, match="^cluster_id is required$"):
        await client.get_cluster_classads(None)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await client.get_cluster_classads(0)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await client.get_cluster_classads(-1)


async def test_get_cluster_classads_no_records():
    client, schedd = _make_client()
    schedd.query.return_value = []
    schedd.history.return_value = []

    with pytest.raises(ValueError, match="^No records found for cluster ID 123$"):
        await client.get_cluster_classads(123)

    schedd.query.assert_called_once_with(
        constraint="ClusterId == 123", projection=_RETURNED_JOB_ADS
    )
    schedd.history.assert_called_once_with(
        constraint="ClusterId == 123", projection=_RETURNED_JOB_ADS
    )


async def test_get_cluster_classads_all_running():
    """Active procs with no history — all in running list; MemoryUsage eval'd."""
    client, schedd = _make_client()
    ca_with_memory = ClassAd(
        "[ClusterId = 123; ProcId = 0; JobStatus = 2;"
        " MemoryUsage = ceiling(ResidentSetSize_RAW / 1024.0); ResidentSetSize_RAW = 524288]"
    )
    assert isinstance(ca_with_memory["ClusterId"], int)
    assert isinstance(ca_with_memory["MemoryUsage"], ExprTree)
    schedd.query.return_value = [
        ca_with_memory,
        ClassAd("[ClusterId = 123; ProcId = 1; JobStatus = 2]"),
    ]
    schedd.history.return_value = []

    running, complete = await client.get_cluster_classads(123)

    assert running == [
        {
            "ClusterId": 123,
            "ProcId": 0,
            "JobStatus": 2,
            "MemoryUsage": 512,
            "ResidentSetSize_RAW": 524288
        },
        {"ClusterId": 123, "ProcId": 1, "JobStatus": 2},
    ]
    assert complete == []
    schedd.query.assert_called_once_with(
        constraint="ClusterId == 123", projection=_RETURNED_JOB_ADS
    )
    schedd.history.assert_called_once_with(
        constraint="ClusterId == 123", projection=_RETURNED_JOB_ADS
    )


async def test_get_cluster_classads_all_complete():
    """History procs with no active — all in complete list; ExprTree stringified."""
    client, schedd = _make_client()
    ca_with_reqs = ClassAd(
        '[ClusterId = 123; ProcId = 0; JobStatus = 4;'
        ' Requirements = (TARGET.Arch == "X86_64")]'
    )
    assert isinstance(ca_with_reqs["ClusterId"], int)
    assert isinstance(ca_with_reqs["Requirements"], ExprTree)
    schedd.query.return_value = []
    schedd.history.return_value = [
        ca_with_reqs,
        ClassAd("[ClusterId = 123; ProcId = 1; JobStatus = 4]"),
    ]

    running, complete = await client.get_cluster_classads(123)

    assert running == []
    assert complete == [
        {
            "ClusterId": 123,
            "ProcId": 0,
            "JobStatus": 4,
            "Requirements": '(TARGET.Arch == "X86_64")'
        },
        {"ClusterId": 123, "ProcId": 1, "JobStatus": 4},
    ]


async def test_get_cluster_classads_race_dedup():
    """A proc appearing in both active query and history is removed from running."""
    client, schedd = _make_client()
    schedd.query.return_value = [
        {"ClusterId": 123, "ProcId": 0, "JobStatus": 2},
        {"ClusterId": 123, "ProcId": 1, "JobStatus": 2},  # will be superseded by history
    ]
    schedd.history.return_value = [{"ClusterId": 123, "ProcId": 1, "JobStatus": 4}]

    running, complete = await client.get_cluster_classads(123)

    assert running == [{"ClusterId": 123, "ProcId": 0, "JobStatus": 2}]
    assert complete == [{"ClusterId": 123, "ProcId": 1, "JobStatus": 4}]



async def test_get_cluster_proc_states_bad_args():
    client, _ = _make_client()
    with pytest.raises(ValueError, match="^cluster_id is required$"):
        await client.get_cluster_proc_states(None)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await client.get_cluster_proc_states(0)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await client.get_cluster_proc_states(-1)


async def test_get_cluster_proc_states_no_records():
    client, schedd = _make_client()
    schedd.query.return_value = []
    schedd.history.return_value = []

    with pytest.raises(ValueError, match="^No records found for cluster ID 123$"):
        await client.get_cluster_proc_states(123)

    schedd.query.assert_called_once_with(
        constraint="ClusterId == 123", projection=["ProcId", "JobStatus"]
    )
    schedd.history.assert_called_once_with(
        constraint="ClusterId == 123", projection=["ProcId", "JobStatus"]
    )


async def test_get_cluster_proc_states_all_complete():
    """All procs in history means the job finished cleanly."""
    client, schedd = _make_client()
    schedd.query.return_value = []
    schedd.history.return_value = [{"ProcId": 0, "JobStatus": 4}, {"ProcId": 1, "JobStatus": 4}]

    states = await client.get_cluster_proc_states(123)

    assert states == [ProcState.COMPLETE, ProcState.COMPLETE]
    schedd.query.assert_called_once_with(
        constraint="ClusterId == 123", projection=["ProcId", "JobStatus"]
    )
    schedd.history.assert_called_once_with(
        constraint="ClusterId == 123", projection=["ProcId", "JobStatus"]
    )


async def test_get_cluster_proc_states_mixed():
    """Active procs are classified; history procs override active if they race."""
    client, schedd = _make_client()
    schedd.query.return_value = [
        {"ProcId": 0, "JobStatus": 2},  # Running → RUNNING
        {"ProcId": 1, "JobStatus": 5},  # Held → HELD
        {"ProcId": 2, "JobStatus": 1},  # Idle → QUEUED
        {"ProcId": 3, "JobStatus": 7},  # Suspended → OTHER
        {"ProcId": 4, "JobStatus": 2},  # Running, will be overridden by history
        {"ProcId": 6, "JobStatus": 6},  # Transferring Output → RUNNING
    ]
    schedd.history.return_value = [
        {"ProcId": 4, "JobStatus": 4},  # Completed → COMPLETE (overrides active)
        {"ProcId": 5, "JobStatus": 3},  # Removed → CANCELED
    ]

    states = await client.get_cluster_proc_states(123)

    assert states == [
        ProcState.RUNNING,   # ProcId 0: Running
        ProcState.HELD,      # ProcId 1: Held
        ProcState.QUEUED,    # ProcId 2: Idle
        ProcState.OTHER,     # ProcId 3: Suspended
        ProcState.COMPLETE,  # ProcId 4: overridden by history
        ProcState.CANCELED,  # ProcId 5: Removed (history only)
        ProcState.RUNNING,   # ProcId 6: Transferring Output
    ]
    schedd.query.assert_called_once_with(
        constraint="ClusterId == 123", projection=["ProcId", "JobStatus"]
    )
    schedd.history.assert_called_once_with(
        constraint="ClusterId == 123", projection=["ProcId", "JobStatus"]
    )


async def test_get_cluster_proc_states_unknown_status():
    """An unrecognised JobStatus value (outside 1-7) raises ValueError."""
    client, schedd = _make_client()
    for status in (0, 8, 99):
        schedd.query.return_value = [{"ProcId": 0, "JobStatus": status}]
        schedd.history.return_value = []
        with pytest.raises(ValueError, match=f"^Unknown HTCondor job status: {status}$"):
            await client.get_cluster_proc_states(123)
        schedd.reset_mock()


async def test_release_job_bad_args():
    client, _ = _make_client()
    with pytest.raises(ValueError, match="^cluster_id is required$"):
        await client.release_job(None)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await client.release_job(0)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await client.release_job(-1)


async def test_release_job():
    client, schedd = _make_client()

    await client.release_job(123)

    schedd.act.assert_called_once_with(htcondor2.JobAction.Release, "ClusterId == 123")


async def test_cancel_job_bad_args():
    client, _ = _make_client()
    with pytest.raises(ValueError, match="^cluster_id is required$"):
        await client.cancel_job(None)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await client.cancel_job(0)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await client.cancel_job(-1)


async def test_cancel_job():
    client, schedd = _make_client()

    await client.cancel_job(123)

    schedd.act.assert_called_once_with(htcondor2.JobAction.Remove, "ClusterId == 123")
