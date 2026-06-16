import aiohttp
import asyncio
from collections import namedtuple
import datetime
import itertools
import json
import logging
import os
from pathlib import Path
import pytest
import signal
from unittest.mock import AsyncMock, MagicMock, PropertyMock, call, create_autospec, patch
import uuid


from cdmtaskservice.externalexecution.config import Config
from cdmtaskservice.externalexecution.container_runner import (
    ContainerCreator,
    ContainerResult,
    RunningContainer,
)
from cdmtaskservice.externalexecution.executor import (
    Executor,
    FatalExecutorError,
)
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths


# ─────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────

_JOB_ID = uuid.UUID("b9faffb2-453a-4ebe-9bba-1b96636cb3b1")
_CONTAINER_NUM = 0
_CTS_URL = "http://cts:8080"
_S3_LOG_PATH = "bucket/logs"
_T = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)


def _ts(n: int) -> str:
    return (_T + datetime.timedelta(seconds=n)).isoformat()


def _advancing_ts():
    g = ((_T + datetime.timedelta(seconds=n)) for n in itertools.count())
    return lambda: next(g)


# CRC-64/NVME of an empty file: checksum stays 0, encodes to 8 zero bytes = AAAAAAAAAAA=
_EMPTY_CRC = "AAAAAAAAAAA="
_LOG_PREFIX = f"cts-{_JOB_ID}-{_CONTAINER_NUM}-container"
_JOB_URL = f"{_CTS_URL}/admin/jobs/{_JOB_ID}"
_UPDATE_BASE = (
    f"{_CTS_URL}/external_exec/jobs/{_JOB_ID}/container/{_CONTAINER_NUM}/update/"
)
_HEARTBEAT_URL = (
    f"{_CTS_URL}/external_exec/jobs/{_JOB_ID}/container/{_CONTAINER_NUM}/heartbeat"
)
# Stats fields returned by the mock ContainerResult — reused to keep call assertions short.
_RESULT_STATS = {"cpu_hours": 0.5, "max_memory_bytes": 1024, "runtime_seconds": 5.0}

_VER_RESP = {"version": "1.0", "git_hash": "abc123"}

# A minimal valid AdminJobDetails JSON that model_validate() accepts.
# input_files uses S3FileWithDataID format so ArgumentGenerator can run for real.
_JOB_JSON = {
    "id": str(_JOB_ID),
    "state": "job_submitted",
    "transition_times": [
        {
            "state": "created",
            "time": "2025-01-01T00:00:00Z",
            "trans_id": "t1",
            "notif_sent": True,
        }
    ],
    "user": "testuser",
    "admin_meta": {},
    "job_input": {
        "cluster": "kbase",
        "image": "myimage@sha256:abc",
        "params": {},
        "num_containers": 1,
        "cpus": 1,
        "memory": 10000000,
        "runtime": 60,
        "output_dir": "bucket/outputs",
        "input_files": [{"file": "bucket/input.txt", "crc64nvme": _EMPTY_CRC}],
    },
    "image": {
        "name": "myimage",
        "digest": "sha256:abc",
        "entrypoint": ["cmd"],
        "registered_by": "testuser",
        "registered_on": "2025-01-01T00:00:00Z",
    },
    "input_file_count": 1,
}


def _make_job_json(*, refdata_id=None, declobber=False, input_crc=_EMPTY_CRC):
    import copy
    j = copy.deepcopy(_JOB_JSON)
    j["job_input"]["input_files"] = [{"file": "bucket/input.txt", "crc64nvme": input_crc}]
    if declobber:
        j["job_input"]["params"]["declobber"] = True
    if refdata_id:
        j["image"]["refdata_id"] = refdata_id
        j["image"]["default_refdata_mount_point"] = "/refdata"
    return j


# ─────────────────────────────────────────────────
# File-system helpers
# ─────────────────────────────────────────────────

def _create_input_file(tmp_path: Path) -> Path:
    """Create the empty input file the download mock is expected to populate."""
    d = tmp_path / "__input__"
    d.mkdir(exist_ok=True)
    f = d / "input.txt"
    f.write_bytes(b"")
    return f


# ─────────────────────────────────────────────────
# Config mock
# ─────────────────────────────────────────────────

def _make_cfg(
    *,
    heartbeat_interval_min=100000,
    job_timeout_min=100000,
    job_update_timeout_min=60,
    mount_prefix_override=None,
):
    """Create a mock Config. Pydantic fields are set via PropertyMock (spec_set blocks them)."""
    cfg = create_autospec(Config, spec_set=True, instance=True)
    type(cfg).job_id = PropertyMock(return_value=_JOB_ID)
    type(cfg).container_number = PropertyMock(return_value=_CONTAINER_NUM)
    # Trailing slash to exercise rstrip("/") in __init__
    type(cfg).cts_url = PropertyMock(return_value=_CTS_URL + "/")
    type(cfg).heartbeat_interval_min = PropertyMock(return_value=heartbeat_interval_min)
    type(cfg).job_timeout_min = PropertyMock(return_value=job_timeout_min)
    type(cfg).job_update_timeout_min = PropertyMock(return_value=job_update_timeout_min)
    type(cfg).s3_url = PropertyMock(return_value="http://s3:9000")
    type(cfg).s3_access_key = PropertyMock(return_value="key")
    type(cfg).s3_insecure = PropertyMock(return_value=False)
    type(cfg).s3_error_log_path = PropertyMock(return_value=_S3_LOG_PATH)
    type(cfg).refdata_host_path = PropertyMock(return_value="/localrefdata")
    type(cfg).mount_prefix_override = PropertyMock(return_value=mount_prefix_override)
    cfg.get_cts_token.return_value = "mytoken"
    cfg.get_s3_access_secret.return_value = "mysecret"
    return cfg


# ─────────────────────────────────────────────────
# Session / response helpers
# ─────────────────────────────────────────────────

def _resp(status, json_data=None, text_data=""):
    r = MagicMock()
    r.status = status
    r.json = AsyncMock(return_value=json_data)
    r.text = AsyncMock(return_value=text_data)
    return r


def _as_cm(resp):
    """Wrap a response in an async context manager."""
    m = MagicMock()
    m.__aenter__ = AsyncMock(return_value=resp)
    m.__aexit__ = AsyncMock(return_value=False)
    return m


def _r200(data):
    return _as_cm(_resp(200, json_data=data))


def _r204():
    return _as_cm(_resp(204))


def _rerr(status, msg, appcode=None):
    data = {"error": {"message": msg}}
    if appcode:
        data["error"]["appcode"] = appcode
    return _as_cm(_resp(status, json_data=data))


def _rnonjson(status):
    r = _resp(status)
    r.json = AsyncMock(side_effect=json.JSONDecodeError("bad json", "", 0))
    r.text = AsyncMock(return_value="<html>gateway error</html>")
    return _as_cm(r)


def _make_session(*, job_json=_JOB_JSON):
    sess = create_autospec(aiohttp.ClientSession, spec_set=True, instance=True)

    def _get(url, **_kw):
        if url == _CTS_URL:
            return _r200(_VER_RESP)
        if url == _JOB_URL:
            return _r200(job_json) if job_json else _rerr(404, "not found")
        raise AssertionError(f"Unexpected GET: {url}")

    sess.get.side_effect = _get
    sess.put.side_effect = lambda *_a, **_kw: _r204()
    return sess


# ─────────────────────────────────────────────────
# Dependency mocks
# ─────────────────────────────────────────────────

def _make_runner(exit_code=0):
    r = create_autospec(RunningContainer, spec_set=True, instance=True)
    result = ContainerResult(
        exit_code=exit_code,
        runtime_seconds=5.0,
        cpu_hours=0.5,
        max_memory_bytes=1024,
    )
    r.__aenter__.return_value = r
    r.__aexit__.return_value = None
    r.wait.return_value = result
    r.cancel.return_value = result
    return r, result


_ExeResult = namedtuple("_ExeResult", ["exe", "sess", "s3", "creator"])


def _make_executor(
    *,
    working_dir=None,
    include_s3=True,
    job_json=_JOB_JSON,
    heartbeat_interval_min=100000,
    job_timeout_min=100000,
    job_update_timeout_min=60,
    mount_prefix_override=None,
):
    cfg = _make_cfg(
        heartbeat_interval_min=heartbeat_interval_min,
        job_timeout_min=job_timeout_min,
        job_update_timeout_min=job_update_timeout_min,
        mount_prefix_override=mount_prefix_override,
    )
    sess = _make_session(job_json=job_json)
    s3 = create_autospec(S3Client, spec_set=True, instance=True) if include_s3 else None
    creator = create_autospec(ContainerCreator, spec_set=True, instance=True)
    extra = {"working_dir": working_dir} if working_dir is not None else {}
    exe = Executor(
        cfg,
        _container_creator=creator,
        _session=sess,
        _s3_client=s3,
        _timestamp_fn=_advancing_ts(),
        **extra,
    )
    return _ExeResult(exe=exe, sess=sess, s3=s3, creator=creator)


# ─────────────────────────────────────────────────
# close()
# ─────────────────────────────────────────────────

async def test_close_closes_session_and_s3_client():
    r = _make_executor()

    await r.exe.close()

    r.sess.close.assert_called_once_with()
    r.s3.close.assert_called_once_with()


async def test_close_without_s3_client_only_closes_session():
    r = _make_executor(include_s3=False)

    await r.exe.close()

    r.sess.close.assert_called_once_with()


async def test_close_via_async_context_manager():
    r = _make_executor()

    async with r.exe:
        pass

    r.sess.close.assert_called_once_with()
    r.s3.close.assert_called_once_with()


# ─────────────────────────────────────────────────
# execute() — success path
# ─────────────────────────────────────────────────

async def test_execute_success_returns_zero_with_correct_state_transitions(tmp_path):
    _create_input_file(tmp_path)
    outdir = tmp_path / "__output__"
    outdir.mkdir()
    result_file = outdir / "result.txt"
    result_file.write_bytes(b"hello world")
    result_crc = "jSnVw/bqjr4="

    runner, _ = _make_runner(exit_code=0)
    r = _make_executor(working_dir=tmp_path)
    r.creator.start_container.return_value = runner

    ret = await r.exe.execute()

    assert ret == 0

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "upload_submitting",
             json={"time": _ts(2), "exit_code": 0, **_RESULT_STATS}),
        call(_UPDATE_BASE + "upload_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "complete", json={
            "time": _ts(4),
            "outputs": [{"file": "bucket/outputs/result.txt", "crc64nvme": result_crc}],
        }),
    ]

    r.creator.start_container.assert_called_once_with(
        "myimage@sha256:abc",
        tmp_path / f"{_LOG_PREFIX}.out",
        tmp_path / f"{_LOG_PREFIX}.err",
        mounts={
            str(tmp_path / "__input__"): ("/input_files", True),
            str(tmp_path / "__output__"): ("/output_files", True),
        },
        command=["cmd"],
        env=[],
    )
    runner.wait.assert_called_once_with()
    runner.cancel.assert_not_called()
    r.s3.download_objects_to_file.assert_called_once_with(
        S3Paths(["bucket/input.txt"]),
        [tmp_path / "__input__" / "input.txt"],
    )
    r.s3.upload_objects_from_file.assert_called_once_with(
        S3Paths(["bucket/outputs/result.txt"]),
        [result_file],
        [result_crc],
    )


async def test_execute_logs_cts_service_version(tmp_path, caplog):
    _create_input_file(tmp_path)
    (tmp_path / "__output__").mkdir()

    runner, _ = _make_runner(exit_code=0)
    r = _make_executor(working_dir=tmp_path)
    r.creator.start_container.return_value = runner

    with caplog.at_level(logging.INFO, logger="cdmtaskservice.externalexecution.executor"):
        await r.exe.execute()

    assert _VER_RESP["version"] in caplog.text
    assert _VER_RESP["git_hash"] in caplog.text

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "upload_submitting",
             json={"time": _ts(2), "exit_code": 0, **_RESULT_STATS}),
        call(_UPDATE_BASE + "upload_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "complete", json={"time": _ts(4)}),
    ]
    r.s3.upload_objects_from_file.assert_not_called()


# ─────────────────────────────────────────────────
# execute() — error paths
# ─────────────────────────────────────────────────

async def test_execute_container_nonzero_exit_triggers_error_processing(tmp_path):
    _create_input_file(tmp_path)

    runner, _ = _make_runner(exit_code=2)
    r = _make_executor(working_dir=tmp_path)
    r.creator.start_container.return_value = runner

    ret = await r.exe.execute()

    assert ret == 2

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "error_processing_submitting",
             json={"time": _ts(2), "exit_code": 2, **_RESULT_STATS}),
        call(_UPDATE_BASE + "error_processing_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "error",
             json={"time": _ts(4), "admin_error": "Container exit code: 2"}),
    ]
    runner.wait.assert_called_once_with()
    runner.cancel.assert_not_called()
    r.s3.upload_objects_from_file.assert_called_once_with(
        S3Paths(["bucket/logs/container-0-stdout.txt", "bucket/logs/container-0-stderr.txt"]),
        [tmp_path / f"{_LOG_PREFIX}.out", tmp_path / f"{_LOG_PREFIX}.err"],
        [_EMPTY_CRC, _EMPTY_CRC],
    )


async def test_execute_exception_during_download_updates_error_state(tmp_path):
    r = _make_executor(working_dir=tmp_path)
    r.s3.download_objects_to_file.side_effect = OSError("disk full")

    ret = await r.exe.execute()

    assert ret == -1

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    tb = r.sess.put.call_args.kwargs["json"]["traceback"]
    assert "disk full" in tb
    r.sess.put.assert_called_once_with(
        _UPDATE_BASE + "error",
        json={"time": _ts(0), "admin_error": "disk full", "traceback": tb},
    )


async def test_execute_fatal_error_during_job_submitted_update_cancels_runner(tmp_path):
    """A fatal error updating JOB_SUBMITTED cancels the runner via the async context manager."""
    _create_input_file(tmp_path)

    runner, _ = _make_runner()
    r = _make_executor(working_dir=tmp_path)
    r.creator.start_container.return_value = runner

    def put_side_effect(url, **_kw):
        if url == _UPDATE_BASE + "job_submitted":
            return _rerr(400, "job update failed", appcode=42)
        return _r204()

    r.sess.put.side_effect = put_side_effect

    ret = await r.exe.execute()

    assert ret == -1

    runner.__aenter__.assert_called_once_with()
    exc_val = runner.__aexit__.call_args.args[1]  # exception instance
    exc_tb = runner.__aexit__.call_args.args[2]
    assert str(exc_val) == "Failed to update job state in the CDM Task Service: job update failed"
    # shows that the container runner can clean up when this exception fires
    runner.__aexit__.assert_called_once_with(FatalExecutorError, exc_val, exc_tb)
    runner.wait.assert_not_called()
    runner.cancel.assert_not_called()

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    tb = r.sess.put.call_args.kwargs["json"]["traceback"]
    assert "Failed to update job state in the CDM Task Service: job update failed" in tb
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "error", json={
            "time": _ts(2),
            "admin_error": "Failed to update job state in the CDM Task Service: job update failed",
            "traceback": tb,
        }),
    ]


async def test_execute_get_job_fatal_error_propagates(tmp_path):
    """GET /admin/jobs returns appcode → FatalExecutorError propagates without state update."""
    r = _make_executor(working_dir=tmp_path)

    def _get(url, **_kw):
        if url == _CTS_URL:
            return _r200(_VER_RESP)
        if url == _JOB_URL:
            return _rerr(400, "job not found", appcode=30010)
        raise AssertionError(f"Unexpected GET: {url}")

    r.sess.get.side_effect = _get

    with pytest.raises(
        FatalExecutorError, match="Failed to get job from the CDM Task Service: job not found"
    ):
        await r.exe.execute()

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    r.sess.put.assert_not_called()


async def test_execute_service_root_fatal_error_propagates(tmp_path):
    """GET / returns appcode → FatalExecutorError propagates before any state update."""
    r = _make_executor(working_dir=tmp_path)
    r.sess.get.side_effect = lambda url, **_kw: _rerr(400, "bad request", appcode=10001)

    with pytest.raises(
        FatalExecutorError, match="Failed to contact CDM Task Service: bad request"
    ):
        await r.exe.execute()

    r.sess.get.assert_called_once_with(_CTS_URL)
    r.sess.put.assert_not_called()


# ─────────────────────────────────────────────────
# execute() — state update retry behavior
# ─────────────────────────────────────────────────

async def test_execute_update_state_retries_on_transient_server_error(tmp_path):
    """A 503 on the first JOB_SUBMITTING PUT triggers a retry; execute() still returns 0."""
    _create_input_file(tmp_path)
    (tmp_path / "__output__").mkdir()

    runner, _ = _make_runner(exit_code=0)
    r = _make_executor(working_dir=tmp_path)
    r.creator.start_container.return_value = runner

    # Fail only the very first JOB_SUBMITTING call; all others succeed.
    job_submitting_url = _UPDATE_BASE + "job_submitting"
    r.sess.put.side_effect = [
        _rerr(503, "server busy"), _r204(), _r204(), _r204(), _r204(), _r204()
    ]

    # Patch sleep so the 5-second default backoff doesn't slow the test.
    with patch("asyncio.sleep", AsyncMock()):
        ret = await r.exe.execute()

    assert ret == 0
    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(job_submitting_url, json={"time": _ts(0)}),
        # _ts(1) is consumed by the retry log message in _update_job_state_loop
        call(job_submitting_url, json={"time": _ts(2)}),  # retry after 503
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "upload_submitting",
             json={"time": _ts(4), "exit_code": 0, **_RESULT_STATS}),
        call(_UPDATE_BASE + "upload_submitted", json={"time": _ts(5)}),
        call(_UPDATE_BASE + "complete", json={"time": _ts(6)}),
    ]


async def test_execute_update_state_times_out_and_raises_fatal(tmp_path):
    """If state updates keep failing past job_update_timeout_min, FatalExecutorError propagates."""
    # job_update_timeout_min=0: first retry immediately exceeds timeout → FatalExecutorError
    # without sleeping, so no asyncio.sleep patch needed.
    r = _make_executor(job_update_timeout_min=0, working_dir=tmp_path)
    r.s3.download_objects_to_file.side_effect = OSError("forced failure")
    r.sess.put.side_effect = lambda *_a, **_kw: _rerr(503, "still busy")

    with pytest.raises(FatalExecutorError, match="Timed out trying to update job state"):
        await r.exe.execute()

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    tb = r.sess.put.call_args.kwargs["json"]["traceback"]
    assert "forced failure" in tb
    r.sess.put.assert_called_once_with(
        _UPDATE_BASE + "error",
        json={"time": _ts(0), "admin_error": "forced failure", "traceback": tb},
    )


async def test_execute_update_state_retries_on_non_json_response(tmp_path):
    """A non-JSON response from the service is treated as a transient error and retried."""
    r = _make_executor(working_dir=tmp_path)
    r.s3.download_objects_to_file.side_effect = OSError("forced failure")
    r.sess.put.side_effect = [_rnonjson(502), _r204()]

    with patch("asyncio.sleep", AsyncMock()):
        ret = await r.exe.execute()

    assert ret == -1
    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    tb = r.sess.put.call_args.kwargs["json"]["traceback"]
    assert "forced failure" in tb
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "error",
             json={"time": _ts(0), "admin_error": "forced failure", "traceback": tb}),
        # _ts(1) is consumed by the retry log message in _update_job_state_loop
        call(_UPDATE_BASE + "error",
             json={"time": _ts(2), "admin_error": "forced failure", "traceback": tb}),
    ]


async def test_execute_update_state_retry_backoff_sequence(tmp_path):
    """Retry sleeps follow the exponential backoff sequence and cap at the maximum value."""
    r = _make_executor(working_dir=tmp_path)
    r.s3.download_objects_to_file.side_effect = OSError("fail")
    # 8 failures exhausts the 7-element sequence and confirms the cap fires on the 8th.
    r.sess.put.side_effect = [*[_rerr(503, "busy")] * 8, _r204()]

    mock_sleep = AsyncMock()
    with patch("asyncio.sleep", mock_sleep):
        ret = await r.exe.execute()

    assert ret == -1
    assert mock_sleep.call_args_list == [
        call(5), call(10), call(30), call(60), call(120), call(300), call(600), call(600),
    ]


# ─────────────────────────────────────────────────
# execute() — timeout paths
# ─────────────────────────────────────────────────

async def test_execute_timeout_task_fires_and_cancels_execute_during_download(tmp_path):
    """_timeout_task fires and cancels the execute task."""
    r = _make_executor(working_dir=tmp_path, job_timeout_min=0)

    async def blocking_download(*_a, **_kw):
        await asyncio.Event().wait()  # blocks until the timeout task cancels the execute task

    r.s3.download_objects_to_file.side_effect = blocking_download

    ret = await r.exe.execute()

    assert ret == -1
    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    # Heartbeat fires once at task startup before its first sleep; timeout fires next.
    assert r.sess.put.call_args_list == [
        call(_HEARTBEAT_URL),
        call(_UPDATE_BASE + "error", json={
            "time": _ts(0),
            "admin_error": "Job timed out after 0.000 days during the download phase",
        }),
    ]


async def test_execute_timeout_during_container_processes_timeout_error(tmp_path):
    _create_input_file(tmp_path)

    runner, result = _make_runner(exit_code=137)  # 128 + SIGKILL
    r = _make_executor(working_dir=tmp_path, job_timeout_min=0)
    r.creator.start_container.return_value = runner

    # wait() blocks until _timeout_task fires cancel(); cancel() unblocks wait().
    # This mirrors production: the timeout task stops the container, letting wait() return.
    cancel_fired = asyncio.Event()

    async def wait_for_cancel():
        await cancel_fired.wait()
        return result

    async def cancel_and_unblock():
        cancel_fired.set()
        return result

    runner.wait.side_effect = wait_for_cancel
    runner.cancel.side_effect = cancel_and_unblock

    ret = await r.exe.execute()

    assert ret == -1
    log_err = tmp_path / f"{_LOG_PREFIX}.err"
    assert log_err.exists()
    assert b"exceeded the maximum allowed runtime" in log_err.read_bytes()

    stderr_crc = "F4LlnYVJVpM="  # CRC64/NVME of the 0.000-days timeout message
    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_HEARTBEAT_URL),
        call(_UPDATE_BASE + "error_processing_submitting",
             json={"time": _ts(2), "exit_code": 137, **_RESULT_STATS}),
        call(_UPDATE_BASE + "error_processing_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "error",
             json={"time": _ts(4), "admin_error": "Container exit code: 137"}),
    ]
    runner.cancel.assert_called_once_with()  # called by _timeout_task
    runner.wait.assert_called_once_with()    # called by _run_container (else branch)
    r.s3.upload_objects_from_file.assert_called_once_with(
        S3Paths(["bucket/logs/container-0-stdout.txt", "bucket/logs/container-0-stderr.txt"]),
        [tmp_path / f"{_LOG_PREFIX}.out", log_err],
        [_EMPTY_CRC, stderr_crc],
    )


async def test_execute_timeout_during_start_container_detected_at_timed_out_check(tmp_path):
    """Timeout fires while start_container is running (self._runner still None).

    _timeout_task skips the runner.cancel() because runner isn't assigned yet.
    _run_container detects _timed_out at the if self._timed_out: check inside
    async with runner: and calls cancel() there instead.
    """
    _create_input_file(tmp_path)

    runner, _ = _make_runner(exit_code=137)  # 128 + SIGKILL
    r = _make_executor(working_dir=tmp_path, job_timeout_min=0)

    # Two yields: 1st lets heartbeat fire and timeout_task queue its sleep(0) callback;
    # 2nd lets timeout fire before start_container returns (self._runner still None).
    async def blocking_start(*_a, **_kw):
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return runner

    r.creator.start_container.side_effect = blocking_start

    ret = await r.exe.execute()

    assert ret == -1
    log_err = tmp_path / f"{_LOG_PREFIX}.err"
    assert log_err.exists()
    assert b"exceeded the maximum allowed runtime" in log_err.read_bytes()

    stderr_crc = "F4LlnYVJVpM="  # CRC64/NVME of the 0.000-days timeout message
    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    # Heartbeat fires between job_submitting and job_submitted because start_container blocks.
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_HEARTBEAT_URL),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "error_processing_submitting",
             json={"time": _ts(2), "exit_code": 137, **_RESULT_STATS}),
        call(_UPDATE_BASE + "error_processing_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "error",
             json={"time": _ts(4), "admin_error": "Container exit code: 137"}),
    ]
    # assert_called_once also proves runner was None when timeout fired: if runner had been set,
    # _timeout_task would have called cancel() too, making the count 2.
    runner.cancel.assert_called_once_with()  # called by _run_container's self._timed_out: branch
    runner.wait.assert_not_called()          # else branch not taken
    r.s3.upload_objects_from_file.assert_called_once_with(
        S3Paths(["bucket/logs/container-0-stdout.txt", "bucket/logs/container-0-stderr.txt"]),
        [tmp_path / f"{_LOG_PREFIX}.out", log_err],
        [_EMPTY_CRC, stderr_crc],
    )


async def test_execute_nontimeout_cancel_reraises(tmp_path):
    r = _make_executor(working_dir=tmp_path)
    r.s3.download_objects_to_file.side_effect = asyncio.CancelledError()
    # _timed_out stays False → _handle_cancel returns False → CancelledError re-raised

    with pytest.raises(asyncio.CancelledError):
        await r.exe.execute()

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    r.sess.put.assert_not_called()


async def test_execute_timeout_during_upload_handles_cancelled_error(tmp_path):
    _create_input_file(tmp_path)
    outdir = tmp_path / "__output__"
    outdir.mkdir()
    (outdir / "out.txt").write_bytes(b"")

    runner, _ = _make_runner(exit_code=0)
    r = _make_executor(working_dir=tmp_path, job_timeout_min=0)
    r.creator.start_container.return_value = runner

    async def blocking_upload(*_a, **_kw):
        await asyncio.Event().wait()  # blocks until _timeout_task cancels the execute task

    r.s3.upload_objects_from_file.side_effect = blocking_upload

    ret = await r.exe.execute()

    assert ret == -1
    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "upload_submitting",
             json={"time": _ts(2), "exit_code": 0, **_RESULT_STATS}),
        call(_UPDATE_BASE + "upload_submitted", json={"time": _ts(3)}),
        call(_HEARTBEAT_URL),
        call(_UPDATE_BASE + "error", json={
            "time": _ts(4),
            "admin_error": "Job timed out after 0.000 days during the upload phase",
        }),
    ]
    runner.wait.assert_called_once_with()
    runner.cancel.assert_not_called()


async def test_execute_timeout_during_error_processing_is_ignored(tmp_path, caplog):
    """_timeout_task fires while phase is _PHASE_ERROR_PROCESSING and does nothing.

    The elif self._phase != _PHASE_ERROR_PROCESSING guard lets error processing always
    complete, even when the job has already timed out.
    """
    _create_input_file(tmp_path)

    runner, _ = _make_runner(exit_code=2)
    r = _make_executor(working_dir=tmp_path, job_timeout_min=0)
    r.creator.start_container.return_value = runner

    upload_started = asyncio.Event()
    upload_can_finish = asyncio.Event()

    async def blocking_upload(*_a, **_kw):
        upload_started.set()
        await upload_can_finish.wait()

    r.s3.upload_objects_from_file.side_effect = blocking_upload

    with caplog.at_level(logging.INFO, logger="cdmtaskservice.externalexecution.executor"):
        exe_task = asyncio.create_task(r.exe.execute())
        await upload_started.wait()
        # One extra yield lets timeout_task's sleep(0) resolve and fire while the upload
        # is still blocked — proving the log is from during error processing, not after.
        await asyncio.sleep(0)
        assert "Timeout fired during error processing" in caplog.text
        upload_can_finish.set()
        ret = await exe_task

    assert ret == 2
    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "error_processing_submitting",
             json={"time": _ts(2), "exit_code": 2, **_RESULT_STATS}),
        call(_UPDATE_BASE + "error_processing_submitted", json={"time": _ts(3)}),
        call(_HEARTBEAT_URL),
        call(_UPDATE_BASE + "error",
             json={"time": _ts(4), "admin_error": "Container exit code: 2"}),
    ]
    runner.wait.assert_called_once_with()
    runner.cancel.assert_not_called()


# ─────────────────────────────────────────────────
# execute() — heartbeat background task
# ─────────────────────────────────────────────────

async def test_heartbeat_fires_to_correct_url_during_execute(tmp_path):
    """With heartbeat_interval_min=0 the heartbeat PUT fires at least once during execute()."""
    _create_input_file(tmp_path)
    (tmp_path / "__output__").mkdir()

    runner, _ = _make_runner(exit_code=0)
    r = _make_executor(heartbeat_interval_min=0, working_dir=tmp_path)
    r.creator.start_container.return_value = runner

    # AsyncMock resolves synchronously, so execute() never genuinely yields to the event
    # loop and the heartbeat task never gets scheduled. A real asyncio.sleep(0) in the
    # download step is the minimal genuine yield that lets the heartbeat task run.
    async def yielding_download(*_a, **_kw):
        await asyncio.sleep(0)

    r.s3.download_objects_to_file.side_effect = yielding_download

    ret = await r.exe.execute()

    assert ret == 0
    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_HEARTBEAT_URL),
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "upload_submitting",
             json={"time": _ts(2), "exit_code": 0, **_RESULT_STATS}),
        call(_UPDATE_BASE + "upload_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "complete", json={"time": _ts(4)}),
    ]


async def test_heartbeat_failure_logged_as_warning(tmp_path, caplog):
    """A failed heartbeat PUT is logged as a warning and does not abort execute()."""
    _create_input_file(tmp_path)
    (tmp_path / "__output__").mkdir()

    runner, _ = _make_runner(exit_code=0)
    r = _make_executor(heartbeat_interval_min=0, working_dir=tmp_path)
    r.creator.start_container.return_value = runner

    def put_side_effect(url, **_kw):
        if url == _HEARTBEAT_URL:
            return _rerr(503, "service unavailable")
        return _r204()

    r.sess.put.side_effect = put_side_effect

    async def yielding_download(*_a, **_kw):
        await asyncio.sleep(0)

    r.s3.download_objects_to_file.side_effect = yielding_download

    with caplog.at_level(logging.WARNING, logger="cdmtaskservice.externalexecution.executor"):
        ret = await r.exe.execute()

    assert ret == 0
    assert "Heartbeat failed" in caplog.text
    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_HEARTBEAT_URL),
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "upload_submitting",
             json={"time": _ts(2), "exit_code": 0, **_RESULT_STATS}),
        call(_UPDATE_BASE + "upload_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "complete", json={"time": _ts(4)}),
    ]


# ─────────────────────────────────────────────────
# execute() — timer interval configuration
# ─────────────────────────────────────────────────

async def test_heartbeat_interval_sleep_uses_configured_value(tmp_path):
    """The heartbeat background task sleeps heartbeat_interval_min * 60 seconds between beats.

    job_timeout_min=0 provides a clean exit: the timeout fires and cancels the blocked download,
    letting execute() return after the heartbeat has had a chance to sleep once.
    """
    r = _make_executor(working_dir=tmp_path, heartbeat_interval_min=7, job_timeout_min=0)

    # Record sleep args while still yielding so background tasks actually get event-loop time.
    original_sleep = asyncio.sleep
    sleep_args = []
    async def recording_sleep(n):
        sleep_args.append(n)
        await original_sleep(0)

    async def blocking_download(*_a, **_kw):
        await asyncio.Event().wait()

    r.s3.download_objects_to_file.side_effect = blocking_download

    with patch("asyncio.sleep", recording_sleep):
        await r.exe.execute()

    assert 7 * 60 in sleep_args


async def test_timeout_sleep_uses_configured_value(tmp_path):
    """The timeout background task sleeps job_timeout_min * 60 seconds before firing."""
    r = _make_executor(working_dir=tmp_path, job_timeout_min=42)

    original_sleep = asyncio.sleep
    sleep_args = []
    async def recording_sleep(n):
        sleep_args.append(n)
        await original_sleep(0)

    async def blocking_download(*_a, **_kw):
        await asyncio.Event().wait()

    r.s3.download_objects_to_file.side_effect = blocking_download

    with patch("asyncio.sleep", recording_sleep):
        await r.exe.execute()

    assert 42 * 60 in sleep_args


# ─────────────────────────────────────────────────
# execute() — background task cleanup
# ─────────────────────────────────────────────────

async def test_execute_cancels_heartbeat_and_timeout_tasks_on_exit(tmp_path):
    """execute() must cancel both background tasks in its finally block."""
    _create_input_file(tmp_path)
    (tmp_path / "__output__").mkdir()

    heartbeat_mock_task = MagicMock()
    timeout_mock_task = MagicMock()
    task_queue = iter([heartbeat_mock_task, timeout_mock_task])

    def fake_create_task(coro):
        coro.close()
        return next(task_queue)

    runner, _ = _make_runner(exit_code=0)
    r = _make_executor(working_dir=tmp_path)
    r.creator.start_container.return_value = runner

    with patch("asyncio.create_task", side_effect=fake_create_task):
        with patch("asyncio.gather", AsyncMock()):
            await r.exe.execute()

    heartbeat_mock_task.cancel.assert_called_once_with()
    timeout_mock_task.cancel.assert_called_once_with()

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "upload_submitting",
             json={"time": _ts(2), "exit_code": 0, **_RESULT_STATS}),
        call(_UPDATE_BASE + "upload_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "complete", json={"time": _ts(4)}),
    ]


# ─────────────────────────────────────────────────
# execute() — signal handling
# ─────────────────────────────────────────────────

async def test_signal_handler_cancels_runner_and_calls_sys_exit(tmp_path):
    _create_input_file(tmp_path)

    runner, _ = _make_runner()
    r = _make_executor(working_dir=tmp_path)
    r.creator.start_container.return_value = runner

    # Block in runner.wait() so execute() is still running when we send the signal.
    # By that point _runner is assigned and signal handlers are installed.
    wait_started = asyncio.Event()

    async def blocking_wait():
        wait_started.set()
        await asyncio.Event().wait()  # blocks until the task is cancelled

    runner.wait.side_effect = blocking_wait

    with patch("sys.exit") as mock_exit:
        exe_task = asyncio.create_task(r.exe.execute())
        await wait_started.wait()
        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.sleep(0.1)
        exe_task.cancel()
        await asyncio.gather(exe_task, return_exceptions=True)

    runner.cancel.assert_called_once_with()
    mock_exit.assert_called_once_with(128 + signal.SIGTERM)

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    # Heartbeat fires once after runner.wait() yields to the event loop.
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_HEARTBEAT_URL),
    ]


async def test_signal_handler_without_runner_calls_sys_exit(tmp_path):
    # Block in download so execute() is running (signal handlers installed) but _runner is None.
    download_started = asyncio.Event()

    async def blocking_download(*_a, **_kw):
        download_started.set()
        await asyncio.Event().wait()

    r = _make_executor(working_dir=tmp_path)
    r.s3.download_objects_to_file.side_effect = blocking_download

    with patch("sys.exit") as mock_exit:
        exe_task = asyncio.create_task(r.exe.execute())
        await download_started.wait()
        os.kill(os.getpid(), signal.SIGINT)
        await asyncio.sleep(0.1)
        exe_task.cancel()
        await asyncio.gather(exe_task, return_exceptions=True)

    mock_exit.assert_called_once_with(128 + signal.SIGINT)

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    # Heartbeat fires once after blocking_download yields to the event loop.
    assert r.sess.put.call_args_list == [call(_HEARTBEAT_URL)]


# ─────────────────────────────────────────────────
# execute() — container mount details
# ─────────────────────────────────────────────────

async def test_execute_mounts_refdata_when_image_has_refdata_id(tmp_path):
    _create_input_file(tmp_path)

    runner, _ = _make_runner()
    r = _make_executor(working_dir=tmp_path, job_json=_make_job_json(refdata_id="ref-abc-123"))
    r.creator.start_container.return_value = runner

    await r.exe.execute()

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "upload_submitting",
             json={"time": _ts(2), "exit_code": 0, **_RESULT_STATS}),
        call(_UPDATE_BASE + "upload_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "complete", json={"time": _ts(4)}),
    ]
    r.creator.start_container.assert_called_once_with(
        "myimage@sha256:abc",
        tmp_path / f"{_LOG_PREFIX}.out",
        tmp_path / f"{_LOG_PREFIX}.err",
        mounts={
            str(tmp_path / "__input__"): ("/input_files", True),
            str(tmp_path / "__output__"): ("/output_files", True),
            str(Path("/localrefdata") / "ref-abc-123"): ("/refdata", False),
        },
        command=["cmd"],
        env=[],
    )


async def test_execute_no_refdata_mount_when_image_has_none(tmp_path):
    _create_input_file(tmp_path)

    runner, _ = _make_runner()
    r = _make_executor(working_dir=tmp_path)
    r.creator.start_container.return_value = runner

    await r.exe.execute()

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "upload_submitting",
             json={"time": _ts(2), "exit_code": 0, **_RESULT_STATS}),
        call(_UPDATE_BASE + "upload_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "complete", json={"time": _ts(4)}),
    ]
    r.creator.start_container.assert_called_once_with(
        "myimage@sha256:abc",
        tmp_path / f"{_LOG_PREFIX}.out",
        tmp_path / f"{_LOG_PREFIX}.err",
        mounts={
            str(tmp_path / "__input__"): ("/input_files", True),
            str(tmp_path / "__output__"): ("/output_files", True),
        },
        command=["cmd"],
        env=[],
    )


async def test_execute_mount_prefix_override_applies_to_all_host_paths(tmp_path):
    _create_input_file(tmp_path)

    replacement = "/override"
    mount_prefix = f"{tmp_path}:{replacement}"

    runner, _ = _make_runner()
    r = _make_executor(working_dir=tmp_path, mount_prefix_override=mount_prefix)
    r.creator.start_container.return_value = runner

    await r.exe.execute()

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "upload_submitting",
             json={"time": _ts(2), "exit_code": 0, **_RESULT_STATS}),
        call(_UPDATE_BASE + "upload_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "complete", json={"time": _ts(4)}),
    ]
    r.creator.start_container.assert_called_once_with(
        "myimage@sha256:abc",
        tmp_path / f"{_LOG_PREFIX}.out",
        tmp_path / f"{_LOG_PREFIX}.err",
        mounts={
            f"{replacement}/__input__": ("/input_files", True),
            f"{replacement}/__output__": ("/output_files", True),
        },
        command=["cmd"],
        env=[],
    )


async def test_execute_declobber_appends_container_number_to_output_path(tmp_path):
    _create_input_file(tmp_path)
    result_file = tmp_path / "__output__" / str(_CONTAINER_NUM) / "result.txt"
    result_file.parent.mkdir(parents=True)
    result_file.write_bytes(b"hello world")
    result_crc = "jSnVw/bqjr4="

    runner, _ = _make_runner()
    r = _make_executor(working_dir=tmp_path, job_json=_make_job_json(declobber=True))
    r.creator.start_container.return_value = runner

    await r.exe.execute()

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "upload_submitting",
             json={"time": _ts(2), "exit_code": 0, **_RESULT_STATS}),
        call(_UPDATE_BASE + "upload_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "complete", json={
            "time": _ts(4),
            "outputs": [{"file": "bucket/outputs/0/result.txt", "crc64nvme": result_crc}],
        }),
    ]
    r.creator.start_container.assert_called_once_with(
        "myimage@sha256:abc",
        tmp_path / f"{_LOG_PREFIX}.out",
        tmp_path / f"{_LOG_PREFIX}.err",
        mounts={
            str(tmp_path / "__input__"): ("/input_files", True),
            str(tmp_path / "__output__" / str(_CONTAINER_NUM)): ("/output_files", True),
        },
        command=["cmd"],
        env=[],
    )
    r.s3.upload_objects_from_file.assert_called_once_with(
        S3Paths(["bucket/outputs/0/result.txt"]),
        [result_file],
        [result_crc],
    )


async def test_execute_logs_container_exit_code_and_memory(tmp_path, caplog):
    _create_input_file(tmp_path)
    (tmp_path / "__output__").mkdir()

    runner, _ = _make_runner(exit_code=0)
    r = _make_executor(working_dir=tmp_path)
    r.creator.start_container.return_value = runner

    with caplog.at_level(logging.INFO, logger="cdmtaskservice.externalexecution.executor"):
        await r.exe.execute()

    assert "Container exited with code 0" in caplog.text
    assert "1024" in caplog.text  # max_memory_bytes from _make_runner

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    assert r.sess.put.call_args_list == [
        call(_UPDATE_BASE + "job_submitting", json={"time": _ts(0)}),
        call(_UPDATE_BASE + "job_submitted", json={"time": _ts(1)}),
        call(_UPDATE_BASE + "upload_submitting",
             json={"time": _ts(2), "exit_code": 0, **_RESULT_STATS}),
        call(_UPDATE_BASE + "upload_submitted", json={"time": _ts(3)}),
        call(_UPDATE_BASE + "complete", json={"time": _ts(4)}),
    ]


# ─────────────────────────────────────────────────
# execute() — download checksum
# ─────────────────────────────────────────────────

async def test_execute_checksum_mismatch_updates_error_state(tmp_path):
    """A CRC mismatch after download is treated as an error: state → ERROR, ret == -1."""
    _create_input_file(tmp_path)  # empty file → real CRC is _EMPTY_CRC

    # Job JSON has a different CRC → mismatch detected by the real crc64nvme_b64 call.
    r = _make_executor(working_dir=tmp_path, job_json=_make_job_json(input_crc="4ekt2WB1KO4="))

    ret = await r.exe.execute()

    assert ret == -1

    tb = r.sess.put.call_args.kwargs["json"]["traceback"]
    assert "ChecksumMismatchError" in tb

    assert r.sess.get.call_args_list == [call(_CTS_URL), call(_JOB_URL)]
    r.sess.put.assert_called_once_with(
        _UPDATE_BASE + "error",
        json={
            "time": _ts(0),
            "admin_error": (
                f"The expected CRC64/NMVE checksum '4ekt2WB1KO4=' for the path "
                f"'bucket/input.txt' does not match the actual checksum '{_EMPTY_CRC}'"
            ),
            "traceback": tb,
        },
    )
