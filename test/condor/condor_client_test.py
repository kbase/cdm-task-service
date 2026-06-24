import pytest
import tempfile
from unittest.mock import create_autospec
import htcondor2

from classad2 import ClassAd, ExprTree

from cdmtaskservice.condor.client import CondorClient, ProcDetails, ProcState, _RETURNED_JOB_ADS
from cdmtaskservice.condor.config import CondorClientConfig
from cdmtaskservice.config_s3 import S3Config


# TODO TEST add more tests


_HOLD_PROJ = ["ProcId", "JobStatus", "HoldReason", "HoldReasonCode"]


def _make_dependencies():
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
    )
    s3config = create_autospec(S3Config, spec_set=True, instance=True)
    return schedd, config, s3config


def _make_client():
    schedd, config, s3config = _make_dependencies()
    client = CondorClient(schedd, config, s3config, heartbeat_interval_min=5)
    return client, schedd


def test_proc_state_is_healthy():
    assert ProcState.QUEUED.is_healthy() is True
    assert ProcState.RUNNING.is_healthy() is True
    assert ProcState.COMPLETE.is_healthy() is True
    assert ProcState.HELD.is_healthy() is False
    assert ProcState.CANCELED.is_healthy() is False
    assert ProcState.OTHER.is_healthy() is False


def test_condor_client_bad_heartbeat_interval():
    schedd, config, s3config = _make_dependencies()
    with pytest.raises(ValueError, match="^heartbeat_interval_min is required$"):
        CondorClient(schedd, config, s3config, heartbeat_interval_min=None)
    with pytest.raises(ValueError, match="^heartbeat_interval_min must be >= 1$"):
        CondorClient(schedd, config, s3config, heartbeat_interval_min=0)


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
    """Absent proc returns _state=MISSING only; no error raised."""
    client, schedd = _make_client()
    schedd.query.return_value = []
    schedd.history.return_value = []

    result = await client.get_container_classad(123, 0)

    assert result == {"_state": ProcState.MISSING}
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
        "_state": ProcState.RUNNING,
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
        "_state": ProcState.COMPLETE,
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

    assert states == {0: ProcState.COMPLETE, 1: ProcState.COMPLETE}
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

    assert states == {
        0: ProcState.RUNNING,   # Running
        1: ProcState.HELD,      # Held
        2: ProcState.QUEUED,    # Idle
        3: ProcState.OTHER,     # Suspended
        4: ProcState.COMPLETE,  # overridden by history
        5: ProcState.CANCELED,  # Removed (history only)
        6: ProcState.RUNNING,   # Transferring Output
    }
    schedd.query.assert_called_once_with(
        constraint="ClusterId == 123", projection=["ProcId", "JobStatus"]
    )
    schedd.history.assert_called_once_with(
        constraint="ClusterId == 123", projection=["ProcId", "JobStatus"]
    )


async def test_get_cluster_proc_states_unknown_status():
    """An unrecognised JobStatus value (outside 1-7) raises ValueError."""
    client, schedd = _make_client()
    schedd.history.return_value = []
    for status in (0, 8, 99):
        schedd.query.return_value = [{"ProcId": 0, "JobStatus": status}]
        with pytest.raises(ValueError, match=f"^Unknown HTCondor job status: {status}$"):
            await client.get_cluster_proc_states(123)


async def test_get_cluster_proc_states_empty_proc_ids():
    """Empty proc_ids list returns empty dict without querying condor."""
    client, schedd = _make_client()

    states = await client.get_cluster_proc_states(123, proc_ids=[])

    assert states == {}
    schedd.query.assert_not_called()
    schedd.history.assert_not_called()


async def test_get_cluster_proc_states_proc_ids_filter():
    """The proc_ids list is pushed into the HTCondor constraint, not filtered in Python."""
    client, schedd = _make_client()
    schedd.query.return_value = [{"ProcId": 1, "JobStatus": 5}]   # Held
    schedd.history.return_value = [{"ProcId": 3, "JobStatus": 4}]  # Complete

    states = await client.get_cluster_proc_states(123, proc_ids=[1, 3])

    assert states == {1: ProcState.HELD, 3: ProcState.COMPLETE}
    constraint = "ClusterId == 123 && (ProcId == 1 || ProcId == 3)"
    proj = ["ProcId", "JobStatus"]
    schedd.query.assert_called_once_with(constraint=constraint, projection=proj)
    schedd.history.assert_called_once_with(constraint=constraint, projection=proj)


async def test_get_cluster_proc_states_proc_ids_not_found():
    """Requested proc IDs absent from both queues returns empty dict, not an error."""
    client, schedd = _make_client()
    schedd.query.return_value = []
    schedd.history.return_value = []

    states = await client.get_cluster_proc_states(123, proc_ids=[0, 1])

    assert states == {}
    constraint = "ClusterId == 123 && (ProcId == 0 || ProcId == 1)"
    proj = ["ProcId", "JobStatus"]
    schedd.query.assert_called_once_with(constraint=constraint, projection=proj)
    schedd.history.assert_called_once_with(constraint=constraint, projection=proj)


async def test_get_cluster_proc_details_bad_args():
    client, _ = _make_client()
    with pytest.raises(ValueError, match="^cluster_id is required$"):
        await client.get_cluster_proc_details(None, 1)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await client.get_cluster_proc_details(0, 1)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await client.get_cluster_proc_details(-1, 1)
    with pytest.raises(ValueError, match="^expected_procs is required$"):
        await client.get_cluster_proc_details(1, None)
    with pytest.raises(ValueError, match="^expected_procs must be >= 0$"):
        await client.get_cluster_proc_details(1, -1)
    with pytest.raises(
        ValueError,
        match=r"^expected_procs contains proc IDs less than 0: \[-3, -1\]$",
    ):
        await client.get_cluster_proc_details(1, [0, -1, 2, -3])


async def test_get_cluster_proc_details_no_records():
    """All expected procs absent from condor return as MISSING; no error raised."""
    client, schedd = _make_client()
    schedd.query.return_value = []
    schedd.history.return_value = []

    details = await client.get_cluster_proc_details(123, 2)

    assert details == {
        0: ProcDetails(state=ProcState.MISSING),
        1: ProcDetails(state=ProcState.MISSING),
    }
    constraint = "ClusterId == 123"
    schedd.query.assert_called_once_with(constraint=constraint, projection=_HOLD_PROJ)
    schedd.history.assert_called_once_with(constraint=constraint, projection=_HOLD_PROJ)


async def test_get_cluster_proc_details_mixed():
    """Held proc carries hold fields; history overrides active; no-detail held is fine."""
    client, schedd = _make_client()
    schedd.query.return_value = [
        {"ProcId": 0, "JobStatus": 2},                                          # Running
        {"ProcId": 1, "JobStatus": 5, "HoldReason": "timeout", "HoldReasonCode": 3},  # Held
        {"ProcId": 2, "JobStatus": 5},                                          # Held, no details
        {"ProcId": 3, "JobStatus": 2},                                     # will be overridden
    ]
    schedd.history.return_value = [
        {"ProcId": 3, "JobStatus": 4},   # Completed — overrides active
        {"ProcId": 4, "JobStatus": 3},   # Removed
    ]

    details = await client.get_cluster_proc_details(123, 5)

    assert details == {
        0: ProcDetails(state=ProcState.RUNNING),
        1: ProcDetails(state=ProcState.HELD, hold_reason="timeout", hold_reason_code=3),
        2: ProcDetails(state=ProcState.HELD),
        3: ProcDetails(state=ProcState.COMPLETE),
        4: ProcDetails(state=ProcState.CANCELED),
    }
    constraint = "ClusterId == 123"
    schedd.query.assert_called_once_with(constraint=constraint, projection=_HOLD_PROJ)
    schedd.history.assert_called_once_with(constraint=constraint, projection=_HOLD_PROJ)


async def test_get_cluster_proc_details_partial_missing_with_count():
    """When an int count is given, procs absent from condor fill in as MISSING."""
    client, schedd = _make_client()
    schedd.query.return_value = [{"ProcId": 0, "JobStatus": 2}]   # Running
    schedd.history.return_value = [{"ProcId": 2, "JobStatus": 4}]  # Completed

    details = await client.get_cluster_proc_details(123, 4)

    assert details == {
        0: ProcDetails(state=ProcState.RUNNING),
        1: ProcDetails(state=ProcState.MISSING),
        2: ProcDetails(state=ProcState.COMPLETE),
        3: ProcDetails(state=ProcState.MISSING),
    }
    constraint = "ClusterId == 123"
    schedd.query.assert_called_once_with(constraint=constraint, projection=_HOLD_PROJ)
    schedd.history.assert_called_once_with(constraint=constraint, projection=_HOLD_PROJ)


async def test_get_cluster_proc_details_unexpected_proc_id_raises():
    """When an int count is given, a returned proc ID >= n raises ValueError."""
    client, schedd = _make_client()
    schedd.query.return_value = [{"ProcId": 0, "JobStatus": 2}, {"ProcId": 3, "JobStatus": 4}]
    schedd.history.return_value = []

    with pytest.raises(
        ValueError,
        match=r"^HTCondor returned unexpected proc IDs \[3\] for cluster 123; expected_procs=3$",
    ):
        await client.get_cluster_proc_details(123, 3)


async def test_get_cluster_proc_details_empty_proc_ids():
    """Empty expected_procs collection returns empty dict without querying condor."""
    client, schedd = _make_client()

    details = await client.get_cluster_proc_details(123, [])

    assert details == {}
    schedd.query.assert_not_called()
    schedd.history.assert_not_called()


async def test_get_cluster_proc_details_proc_ids_filter():
    """The proc_ids list is pushed into the HTCondor constraint, not filtered in Python."""
    client, schedd = _make_client()
    schedd.query.return_value = [
        {"ProcId": 1, "JobStatus": 5, "HoldReason": "oom", "HoldReasonCode": 7}
    ]
    schedd.history.return_value = [{"ProcId": 3, "JobStatus": 4}]

    details = await client.get_cluster_proc_details(123, [1, 3])

    assert details == {
        1: ProcDetails(state=ProcState.HELD, hold_reason="oom", hold_reason_code=7),
        3: ProcDetails(state=ProcState.COMPLETE),
    }
    constraint = "ClusterId == 123 && (ProcId == 1 || ProcId == 3)"
    schedd.query.assert_called_once_with(constraint=constraint, projection=_HOLD_PROJ)
    schedd.history.assert_called_once_with(constraint=constraint, projection=_HOLD_PROJ)


async def test_get_cluster_proc_details_unknown_status():
    """An unrecognised JobStatus value raises ValueError."""
    client, schedd = _make_client()
    schedd.history.return_value = []
    for status in (0, 8, 99):
        schedd.query.return_value = [{"ProcId": 0, "JobStatus": status}]
        with pytest.raises(ValueError, match=f"^Unknown HTCondor job status: {status}$"):
            await client.get_cluster_proc_details(123, 1)


async def test_get_cluster_proc_details_proc_ids_not_found():
    """Expected proc IDs absent from both queues are returned as MISSING, not an error."""
    client, schedd = _make_client()
    schedd.query.return_value = []
    schedd.history.return_value = []

    details = await client.get_cluster_proc_details(123, [0, 1])

    assert details == {
        0: ProcDetails(state=ProcState.MISSING),
        1: ProcDetails(state=ProcState.MISSING),
    }
    constraint = "ClusterId == 123 && (ProcId == 0 || ProcId == 1)"
    schedd.query.assert_called_once_with(constraint=constraint, projection=_HOLD_PROJ)
    schedd.history.assert_called_once_with(constraint=constraint, projection=_HOLD_PROJ)


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
