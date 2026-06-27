import pytest
import tempfile
from dataclasses import dataclass
from typing import Any, Callable
from unittest.mock import create_autospec
import htcondor2

from classad2 import ClassAd, ExprTree

from cdmtaskservice.condor.client import (
    CondorClient, ProcDetails, ProcState, ProcStats, _RETURNED_JOB_ADS,
)
from cdmtaskservice.condor.config import CondorClientConfig
from cdmtaskservice.config_s3 import S3Config


# TODO TEST add more tests




# ─── shared mock ads for parametrized value-assertion tests ──────────────────

# Two procs that both finished cleanly.
# Proc 0 has cpu/runtime stats; proc 1 uses a real ClassAd with ExprTree MemoryUsage —
# together they exercise stat extraction and ExprTree evaluation for the stats spec.
_ALL_COMPLETE_ADS = [
    {"ProcId": 0, "JobStatus": 4, "RemoteUserCpu": 60.0, "RemoteSysCpu": 10.0,
     "CommittedTime": 1800},
    ClassAd(
        "[ProcId = 1; JobStatus = 4;"
        " MemoryUsage = ceiling(ResidentSetSize_RAW / 1024.0); ResidentSetSize_RAW = 524288]"
    ),
]

# Mixed scenario: every HTCondor status code covered, plus a race-dedup case.
# Hold fields are always present so detail-aware methods can read them; state-only
# methods ignore them. Proc 6 appears in both active and history to test dedup.
_MIXED_ACTIVE_ADS = [
    {"ProcId": 0, "JobStatus": 2},
    {"ProcId": 1, "JobStatus": 5, "HoldReason": "timeout", "HoldReasonCode": 3,
     "MemoryUsage": 400, "CommittedTime": 900},
    {"ProcId": 2, "JobStatus": 5},
    {"ProcId": 3, "JobStatus": 1},
    {"ProcId": 4, "JobStatus": 7},
    {"ProcId": 5, "JobStatus": 6},
    {"ProcId": 6, "JobStatus": 2},
]
# Proc 6 in history overrides the active entry; its stats come from the history record.
_MIXED_HISTORY_ADS = [
    {"ProcId": 6, "JobStatus": 4, "RemoteUserCpu": 30.0, "RemoteSysCpu": 5.0},
    {"ProcId": 7, "JobStatus": 3},
]

# Partial-present scenario: procs 0 and 2 are in condor; procs 1 and 3 are absent.
_PARTIAL_ACTIVE_ADS  = [{"ProcId": 0, "JobStatus": 2}]
_PARTIAL_HISTORY_ADS = [{"ProcId": 2, "JobStatus": 4}]

# Collection-filter scenario: two specific procs requested, one held with details.
_FILTER_ACTIVE_ADS  = [{"ProcId": 1, "JobStatus": 5, "HoldReason": "oom", "HoldReasonCode": 7}]
_FILTER_HISTORY_ADS = [{"ProcId": 3, "JobStatus": 4}]


# ─── per-method spec ─────────────────────────────────────────────────────────

@dataclass
class _ProcMapSpec:
    """Describes one _fetch_proc_map-based client method for parametrized tests."""
    label: str
    call: Callable          # (client, cluster_id, expected_procs) -> coroutine
    missing: Any            # MISSING sentinel value
    projection: list        # expected HTCondor projection for constraint assertions
    # Hardcoded expected results for value-assertion tests:
    all_complete_expected: dict
    mixed_expected: dict
    partial_missing_expected: dict
    filter_expected: dict


_STATES_SPEC = _ProcMapSpec(
    label="states",
    call=lambda c, cid, ep: c.get_cluster_proc_states(cid, ep),
    missing=ProcState.MISSING,
    projection=["ProcId", "JobStatus"],
    all_complete_expected={0: ProcState.COMPLETE, 1: ProcState.COMPLETE},
    mixed_expected={
        0: ProcState.RUNNING,
        1: ProcState.HELD,
        2: ProcState.HELD,
        3: ProcState.QUEUED,
        4: ProcState.OTHER,
        5: ProcState.RUNNING,
        6: ProcState.COMPLETE,
        7: ProcState.CANCELED,
    },
    partial_missing_expected={
        0: ProcState.RUNNING,
        1: ProcState.MISSING,
        2: ProcState.COMPLETE,
        3: ProcState.MISSING,
    },
    filter_expected={1: ProcState.HELD, 3: ProcState.COMPLETE},
)

_DETAILS_SPEC = _ProcMapSpec(
    label="details",
    call=lambda c, cid, ep: c.get_cluster_proc_details(cid, ep),
    missing=ProcDetails(state=ProcState.MISSING),
    projection=["ProcId", "JobStatus", "HoldReason", "HoldReasonCode"],
    all_complete_expected={
        0: ProcDetails(state=ProcState.COMPLETE),
        1: ProcDetails(state=ProcState.COMPLETE),
    },
    mixed_expected={
        0: ProcDetails(state=ProcState.RUNNING),
        1: ProcDetails(state=ProcState.HELD, hold_reason="timeout", hold_reason_code=3),
        2: ProcDetails(state=ProcState.HELD),
        3: ProcDetails(state=ProcState.QUEUED),
        4: ProcDetails(state=ProcState.OTHER),
        5: ProcDetails(state=ProcState.RUNNING),
        6: ProcDetails(state=ProcState.COMPLETE),
        7: ProcDetails(state=ProcState.CANCELED),
    },
    partial_missing_expected={
        0: ProcDetails(state=ProcState.RUNNING),
        1: ProcDetails(state=ProcState.MISSING),
        2: ProcDetails(state=ProcState.COMPLETE),
        3: ProcDetails(state=ProcState.MISSING),
    },
    filter_expected={
        1: ProcDetails(state=ProcState.HELD, hold_reason="oom", hold_reason_code=7),
        3: ProcDetails(state=ProcState.COMPLETE),
    },
)

_STATS_SPEC = _ProcMapSpec(
    label="stats",
    call=lambda c, cid, ep: c.get_cluster_proc_stats(cid, ep),
    missing=ProcStats(state=ProcState.MISSING),
    projection=["ProcId", "JobStatus", "MemoryUsage", "RemoteUserCpu", "RemoteSysCpu",
                "CommittedTime"],
    all_complete_expected={
        # proc 0: cpu/runtime present, no memory; proc 1: ExprTree MemoryUsage, no cpu/runtime
        0: ProcStats(state=ProcState.COMPLETE, cpu_hours=70.0 / 3600, runtime_seconds=1800.0),
        1: ProcStats(state=ProcState.COMPLETE, max_memory=512 * 1024 * 1024),
    },
    mixed_expected={
        0: ProcStats(state=ProcState.RUNNING),
        1: ProcStats(state=ProcState.HELD, max_memory=400 * 1024 * 1024, runtime_seconds=900.0),
        2: ProcStats(state=ProcState.HELD),
        3: ProcStats(state=ProcState.QUEUED),
        4: ProcStats(state=ProcState.OTHER),
        5: ProcStats(state=ProcState.RUNNING),
        6: ProcStats(state=ProcState.COMPLETE, cpu_hours=35.0 / 3600),
        7: ProcStats(state=ProcState.CANCELED),
    },
    partial_missing_expected={
        0: ProcStats(state=ProcState.RUNNING),
        1: ProcStats(state=ProcState.MISSING),
        2: ProcStats(state=ProcState.COMPLETE),
        3: ProcStats(state=ProcState.MISSING),
    },
    filter_expected={
        1: ProcStats(state=ProcState.HELD),
        3: ProcStats(state=ProcState.COMPLETE),
    },
)

_PROC_MAP_SPECS = [_STATES_SPEC, _DETAILS_SPEC, _STATS_SPEC]


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
    assert ProcState.MISSING.is_healthy() is False


def test_proc_state_is_queued_or_running():
    assert ProcState.QUEUED.is_queued_or_running() is True
    assert ProcState.RUNNING.is_queued_or_running() is True
    assert ProcState.COMPLETE.is_queued_or_running() is False
    assert ProcState.HELD.is_queued_or_running() is False
    assert ProcState.CANCELED.is_queued_or_running() is False
    assert ProcState.OTHER.is_queued_or_running() is False
    assert ProcState.MISSING.is_queued_or_running() is False


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


# ─── parametrized tests covering all _fetch_proc_map-based methods ───────────


@pytest.mark.parametrize("spec", _PROC_MAP_SPECS, ids=lambda s: s.label)
async def test_proc_map_bad_args(spec):
    client, _ = _make_client()
    with pytest.raises(ValueError, match="^cluster_id is required$"):
        await spec.call(client, None, 1)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await spec.call(client, 0, 1)
    with pytest.raises(ValueError, match="^cluster_id must be >= 1$"):
        await spec.call(client, -1, 1)
    with pytest.raises(ValueError, match="^expected_procs is required$"):
        await spec.call(client, 1, None)
    with pytest.raises(ValueError, match="^expected_procs must be >= 0$"):
        await spec.call(client, 1, -1)
    with pytest.raises(
        ValueError,
        match=r"^expected_procs contains proc IDs less than 0: \[-3, -1\]$",
    ):
        await spec.call(client, 1, [0, -1, 2, -3])


@pytest.mark.parametrize("spec", _PROC_MAP_SPECS, ids=lambda s: s.label)
async def test_proc_map_no_records(spec):
    """All expected procs absent from condor return as MISSING; no error raised."""
    client, schedd = _make_client()
    schedd.query.return_value = []
    schedd.history.return_value = []

    result = await spec.call(client, 123, 2)

    assert result == {0: spec.missing, 1: spec.missing}
    schedd.query.assert_called_once_with(
        constraint="ClusterId == 123", projection=spec.projection
    )
    schedd.history.assert_called_once_with(
        constraint="ClusterId == 123", projection=spec.projection
    )


@pytest.mark.parametrize("spec", _PROC_MAP_SPECS, ids=lambda s: s.label)
async def test_proc_map_all_complete(spec):
    """All procs in history means the job finished cleanly."""
    client, schedd = _make_client()
    schedd.query.return_value = []
    schedd.history.return_value = _ALL_COMPLETE_ADS

    result = await spec.call(client, 123, 2)

    assert result == spec.all_complete_expected
    schedd.query.assert_called_once_with(
        constraint="ClusterId == 123", projection=spec.projection
    )
    schedd.history.assert_called_once_with(
        constraint="ClusterId == 123", projection=spec.projection
    )


@pytest.mark.parametrize("spec", _PROC_MAP_SPECS, ids=lambda s: s.label)
async def test_proc_map_mixed(spec):
    """Active procs classified; history overrides active on race; all status codes exercised."""
    client, schedd = _make_client()
    schedd.query.return_value = _MIXED_ACTIVE_ADS
    schedd.history.return_value = _MIXED_HISTORY_ADS

    result = await spec.call(client, 123, 8)

    assert result == spec.mixed_expected
    schedd.query.assert_called_once_with(
        constraint="ClusterId == 123", projection=spec.projection
    )
    schedd.history.assert_called_once_with(
        constraint="ClusterId == 123", projection=spec.projection
    )


@pytest.mark.parametrize("spec", _PROC_MAP_SPECS, ids=lambda s: s.label)
async def test_proc_map_partial_missing_with_count(spec):
    """When an int count is given, procs absent from condor fill in as MISSING."""
    client, schedd = _make_client()
    schedd.query.return_value = _PARTIAL_ACTIVE_ADS
    schedd.history.return_value = _PARTIAL_HISTORY_ADS

    result = await spec.call(client, 123, 4)

    assert result == spec.partial_missing_expected
    schedd.query.assert_called_once_with(
        constraint="ClusterId == 123", projection=spec.projection
    )
    schedd.history.assert_called_once_with(
        constraint="ClusterId == 123", projection=spec.projection
    )


@pytest.mark.parametrize("spec", _PROC_MAP_SPECS, ids=lambda s: s.label)
async def test_proc_map_unexpected_proc_id_raises(spec):
    """When an int count is given, a returned proc ID >= n raises ValueError."""
    client, schedd = _make_client()
    schedd.query.return_value = [{"ProcId": 0, "JobStatus": 2}, {"ProcId": 3, "JobStatus": 4}]
    schedd.history.return_value = []

    with pytest.raises(
        ValueError,
        match=r"^HTCondor returned unexpected proc IDs \[3\] for cluster 123$",
    ):
        await spec.call(client, 123, 3)


@pytest.mark.parametrize("spec", _PROC_MAP_SPECS, ids=lambda s: s.label)
async def test_proc_map_unknown_status(spec):
    """An unrecognised JobStatus value raises ValueError."""
    client, schedd = _make_client()
    schedd.history.return_value = []
    for status in (0, 8, 99):
        schedd.query.return_value = [{"ProcId": 0, "JobStatus": status}]
        with pytest.raises(ValueError, match=f"^Unknown HTCondor job status: {status}$"):
            await spec.call(client, 123, 1)


@pytest.mark.parametrize("spec", _PROC_MAP_SPECS, ids=lambda s: s.label)
async def test_proc_map_empty_proc_ids(spec):
    """Empty expected_procs collection returns empty dict without querying condor."""
    client, schedd = _make_client()

    result = await spec.call(client, 123, [])

    assert result == {}
    schedd.query.assert_not_called()
    schedd.history.assert_not_called()


@pytest.mark.parametrize("spec", _PROC_MAP_SPECS, ids=lambda s: s.label)
async def test_proc_map_proc_ids_filter(spec):
    """The proc_ids collection is pushed into the HTCondor constraint, not filtered in Python."""
    client, schedd = _make_client()
    schedd.query.return_value = _FILTER_ACTIVE_ADS
    schedd.history.return_value = _FILTER_HISTORY_ADS

    result = await spec.call(client, 123, [1, 3])

    assert result == spec.filter_expected
    constraint = "ClusterId == 123 && (ProcId == 1 || ProcId == 3)"
    schedd.query.assert_called_once_with(constraint=constraint, projection=spec.projection)
    schedd.history.assert_called_once_with(constraint=constraint, projection=spec.projection)


@pytest.mark.parametrize("spec", _PROC_MAP_SPECS, ids=lambda s: s.label)
async def test_proc_map_proc_ids_not_found(spec):
    """Expected proc IDs absent from both queues are returned as MISSING, not an error."""
    client, schedd = _make_client()
    schedd.query.return_value = []
    schedd.history.return_value = []

    result = await spec.call(client, 123, [0, 1])

    assert result == {0: spec.missing, 1: spec.missing}
    constraint = "ClusterId == 123 && (ProcId == 0 || ProcId == 1)"
    schedd.query.assert_called_once_with(constraint=constraint, projection=spec.projection)
    schedd.history.assert_called_once_with(constraint=constraint, projection=spec.projection)


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
