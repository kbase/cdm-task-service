"""
The main CTS external executor class.
"""

import aiohttp
import asyncio
import datetime
import json
import logging
from pathlib import Path
import signal
import sys
import time
import traceback
import uuid
from typing import Callable, TextIO, Any

from cdmtaskservice.argument_generator import ArgumentGenerator, SCRIPT_FILENAME
from cdmtaskservice.exceptions import ChecksumMismatchError
from cdmtaskservice.externalexecution.config import Config
from cdmtaskservice.externalexecution.container_runner import (
    ContainerCreator,
    ContainerResult,
    RunningContainer,
)
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice.jobflows.container_filenames import get_filenames_for_container
from cdmtaskservice import models
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.s3.remote import crc64nvme_b64
from cdmtaskservice.version import VERSION
from cdmtaskservice.timestamp import utcdatetime


_EXP_BACKOFF_SEC = [5, 10, 30, 60, 120, 300, 600]

_INPUT = "__input__"
_OUTPUT = "__output__"

_PHASE_INIT = "init"
_PHASE_DOWNLOAD = "download"
_PHASE_CONTAINER = "container"
_PHASE_ERROR_PROCESSING = "error_processing"
_PHASE_UPLOAD = "upload"


class Executor:
    """ The executor. Note that the executor is not concurrency safe. """

    def __init__(
        self,
        cfg: Config,
        *,
        working_dir: Path = Path(),
        _session: aiohttp.ClientSession | None = None,
        _s3_client: S3Client | None = None,
        _container_creator: ContainerCreator | None = None,
        _download_delay_sec: float = 0,
        _upload_delay_sec: float = 0,
        _error_upload_delay_sec: float = 0,
        _timestamp_fn: Callable[[], datetime.datetime] = utcdatetime,
        _heartbeat_fn: Callable[[uuid.UUID], Any] | None = None,
    ):
        """
        Create the executor from the configuration.

        cfg - the executor configuration.
        working_dir - the directory used as the root for all file operations (input downloads,
            output uploads, container log files). Tilde and relative paths are expanded and
            resolved at construction time. Defaults to the current working directory.
        """
        self._cfg = cfg
        self._container_creator = _container_creator or ContainerCreator()
        self._download_delay_sec = _download_delay_sec
        self._upload_delay_sec = _upload_delay_sec
        self._error_upload_delay_sec = _error_upload_delay_sec
        self._timestamp_fn = _timestamp_fn
        self._heartbeat_fn = _heartbeat_fn if _heartbeat_fn is not None else self._heartbeat_loop
        self._workdir = working_dir.expanduser().resolve()
        self._url = self._cfg.cts_url.rstrip("/")
        self._sess = _session or aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {cfg.get_cts_token()}"}
        )
        self._logr = logging.getLogger(__name__)
        self._args = None
        self._s3cli = _s3_client  # created lazily if not injected
        self._runner: RunningContainer | None = None
        self._timed_out = False
        self._phase = _PHASE_INIT
        self._execute_task: asyncio.Task | None = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """ Close any resources associated with the executor. """
        await self._sess.close()
        if self._s3cli:
            await self._s3cli.close()

    async def _heartbeat_loop(self, job_id: uuid.UUID):
        url = (
            f"{self._url}/external_exec/jobs/{job_id}"
            f"/container/{self._cfg.container_number}/heartbeat"
        )
        while True:
            try:
                async with self._sess.put(url) as resp:
                    await self._check_resp(resp, "Heartbeat failed")
            except Exception as e:
                self._logr.warning("Heartbeat failed, will retry next interval: %s", e)
            await asyncio.sleep(self._cfg.heartbeat_interval_min * 60)

    async def _timeout_task(self) -> None:
        await asyncio.sleep(self._cfg.job_timeout_min * 60)
        timeout_days = self._cfg.job_timeout_min / (60 * 24)
        self._logr.info(f"Job timed out after {timeout_days:.3f} days")
        self._timed_out = True
        if self._phase == _PHASE_CONTAINER:
            if self._runner:
                await self._runner.cancel()
            # Now allow the error processing to continue and upload logs
        elif self._phase == _PHASE_ERROR_PROCESSING:
            self._logr.info(
                "Timeout fired during error processing, allowing error processing to complete"
            )
        else:
            # Download or upload is in progress — cancel the execute task so state can
            # be updated before condor's periodic hold fires.
            # Running out of time during upload should be rare vs.
            # running out while running the container, so for now we just error out the job.
            # If we start seeing upload timeouts a lot rethink - admins get a specific error
            # message but users only see a generic error.
            if self._execute_task:
                self._execute_task.cancel()

    def _setup_signal_handlers(self):
        async def on_signal(signum: int):
            self._logr.info(f"Received signal {signum}, stopping")
            if self._runner:
                await self._runner.cancel()
            sys.exit(128 + signum)

        def _retrieve_exc(task):
            # Calling exception() marks it as retrieved, suppressing asyncio's
            # "Task exception was never retrieved" warning for the SystemExit we raise.
            if not task.cancelled():
                task.exception()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(on_signal(s)).add_done_callback(
                    _retrieve_exc
                ),
            )

    async def execute(self) -> int:
        """
        Run the executor.
        Returns 0 for success, a positive integer for the container exit code, or -1 if an error
        occurred other than a container error.
        """
        await self._log_service_ver()
        self._execute_task = asyncio.current_task()
        self._setup_signal_handlers()
        heartbeat_task = asyncio.create_task(self._heartbeat_fn(self._cfg.job_id))
        timeout_task = asyncio.create_task(self._timeout_task())
        try:
            # If we can't get the job, we presumably can't update the job either,
            # so we just throw any exceptions.
            job = await self._get_job()
            # we assume here that the service has already error checked the job input
            try:
                self._args = ArgumentGenerator(job).get_container_arguments(
                    self._cfg.container_number
                )
                self._phase = _PHASE_DOWNLOAD
                await self._download_files(job)
                self._phase = _PHASE_CONTAINER
                result = await self._run_container(job)
                if self._timed_out:
                    self._phase = _PHASE_ERROR_PROCESSING
                    self._append_timeout_to_logs(job)
                    await self._process_error_state(job, result)
                    return -1
                if result.exit_code > 0:
                    self._phase = _PHASE_ERROR_PROCESSING
                    await self._process_error_state(job, result)
                else:
                    self._phase = _PHASE_UPLOAD
                    await self._process_complete_state(job, result)
                return result.exit_code
            except asyncio.CancelledError:
                if not await self._handle_cancel(job, self._phase):
                    raise
                return -1
            except Exception as e:
                self._phase = _PHASE_ERROR_PROCESSING
                self._logr.exception(f"Job failed: {e}")
                await self._update_job_state_loop(job, models.JobState.ERROR, exception=e)
                return -1
        finally:
            timeout_task.cancel()
            heartbeat_task.cancel()
            results = await asyncio.gather(timeout_task, heartbeat_task, return_exceptions=True)
            for task_result in results:
                if (
                    isinstance(task_result, Exception)
                    and not isinstance(task_result, asyncio.CancelledError)
                ):
                    self._logr.error("Background task failed unexpectedly", exc_info=task_result)

    async def _handle_cancel(
        self, job: models.AdminJobDetails, phase_at_cancel: str
    ) -> bool:
        """
        Handle a CancelledError raised in execute(). Sets phase to error_processing.
        Returns True if the cancel was a timeout and job state was updated, False if the
        caller should re-raise (non-timeout cancellation).
        """
        self._phase = _PHASE_ERROR_PROCESSING
        if not self._timed_out:
            return False
        timeout_days = self._cfg.job_timeout_min / (60 * 24)
        admin_error = (
            f"Job timed out after {timeout_days:.3f} days "
            f"during the {phase_at_cancel} phase"
        )
        self._logr.info(
            f"Job timed out during {phase_at_cancel} phase, updating state to ERROR"
        )
        await self._update_job_state_loop(job, models.JobState.ERROR, admin_error=admin_error)
        return True

    async def _check_resp(self, resp: aiohttp.ClientResponse, action: str
    ) -> dict[str, Any] | None:
        if resp.status == 204:  # no content
            return None
        try:
            resjson = await resp.json()
        except Exception:
            err = "Non-JSON response from CDM Task Service, status code: " + str(resp.status)
            # TODO TEST logging
            self._logr.exception("%s, response:\n%s", err, await resp.text())
            raise RetryableExecutorError(err)
        if resp.status != 200:
            # assume we're talking to the CTS at this point
            self._logr.error(f"{action}. Response contents:\n{json.dumps(resjson, indent=2)}")
            appcode = resjson["error"].get("appcode")
            msg = f"{action}: {resjson['error']['message']}"
            if appcode:
                # If there's an appcode, something is very wrong
                raise FatalExecutorError(msg)
            # TODO ERRORHANDLING we'll need to see what other errors are possible here
            raise RetryableExecutorError(msg)
        return resjson

    async def _log_service_ver(self):
        async with self._sess.get(self._url) as resp:
            root = await self._check_resp(resp, "Failed to contact CDM Task Service")
        self._logr.info(f"CTS version: {root['version']} githash: {root['git_hash']}")

    async def _get_job(self) -> models.AdminJobDetails:
        # TODO RELIABILITY retries. Tenatcity might be useful
        url = f"{self._url}/admin/jobs/{self._cfg.job_id}"
        async with self._sess.get(url) as resp:
            jobjson = await self._check_resp(resp, "Failed to get job from the CDM Task Service")
        return models.AdminJobDetails.model_validate(jobjson)

    async def _update_job_state_loop(
        self,
        job: models.AdminJobDetails,
        state: models.JobState,
        *,
        exit_code: int = None,
        cpu_hours: float = None,
        max_memory_bytes: int = None,
        runtime_seconds: float = None,
        outputs: list[models.S3File] = None,
        admin_error: str = None,
        exception: Exception = None
    ):
        # Considered making a queue so the job could continue while attempting to update
        # but that seems like too much complexity for something that doesn't happen very often
        # and may mean that S3 is down as well
        start = time.monotonic()
        backoff_counter = 0
        while True:
            try:
                await self._update_job_state(
                    job,
                    state,
                    exit_code=exit_code,
                    cpu_hours=cpu_hours,
                    max_memory_bytes=max_memory_bytes,
                    runtime_seconds=runtime_seconds,
                    outputs=outputs,
                    admin_error=admin_error,
                    exception=exception
                )
                return
            except FatalExecutorError:
                raise
            except Exception as e:
                # Will need to figure out what kinds of errors we get here  and add to immediate
                # fail block
                backoff = self._get_backoff(backoff_counter)
                if time.monotonic() - start + backoff >= self._cfg.job_update_timeout_min * 60:
                    raise FatalExecutorError(
                        f"Timed out trying to update job state to {state.value}. "
                        + f"{time.monotonic() - start} sec elapsed, "
                        + f"next wait period is {backoff} sec: {e}"
                    ) from e
                self._logr.exception(
                    f"Failed updating job state to {state.value} at "
                    + f"{self._timestamp_fn().isoformat()}, trying again in {backoff} seconds: {e}"
                )
                backoff_counter += 1
                await asyncio.sleep(backoff)

    def _get_backoff(self, counter):
        if counter >= len(_EXP_BACKOFF_SEC):
            return _EXP_BACKOFF_SEC[-1]
        return _EXP_BACKOFF_SEC[counter]

    async def _update_job_state(
        self,
        job: models.AdminJobDetails,
        state: models.JobState,
        *,
        outputs: list[models.S3File] = None,
        exit_code: int = None,
        cpu_hours: float = None,
        max_memory_bytes: int = None,
        runtime_seconds: float = None,
        admin_error: str = None,
        exception: Exception = None
    ):
        url = (
            f"{self._url}/external_exec/jobs/{job.id}/"
            + f"container/{self._cfg.container_number}/update/{state.value}"
        )
        data = {"time": self._timestamp_fn().isoformat()}
        if exception:
            data["admin_error"] = str(exception)
            data["traceback"] = traceback.format_exc()
        elif admin_error:
            data["admin_error"] = admin_error
        if exit_code is not None:
            data['exit_code'] = exit_code
        if cpu_hours is not None:
            data['cpu_hours'] = cpu_hours
        if max_memory_bytes is not None:
            data['max_memory_bytes'] = max_memory_bytes
        if runtime_seconds is not None:
            data['runtime_seconds'] = runtime_seconds
        if outputs:
            data["outputs"] = [o.model_dump() for o in outputs]
        async with self._sess.put(url, json=data) as resp:
            await self._check_resp(resp, "Failed to update job state in the CDM Task Service")

    async def _download_files(self, job: models.AdminJobDetails) -> None:
        s3paths = []
        local_paths = []
        root = self._workdir / _INPUT
        filerecs = []
        for i, (obj, loc) in enumerate(self._args.files.items(), start=1):
            s3paths.append(obj.file)
            local_paths.append(root / loc)
            filerecs.append(f"""
File #{i} CRC64NVME: {obj.crc64nvme}
S3 Path: {obj.file}
Local relative path: {loc}
"""
            )
        self._logr.info("Downloading files:" + "===".join(filerecs))
        if not self._s3cli:
            self._s3cli = await S3Client.create(
                self._cfg.s3_url,
                self._cfg.s3_access_key,
                self._cfg.get_s3_access_secret(),
                insecure_ssl=self._cfg.s3_insecure
            )
        if self._download_delay_sec:
            await asyncio.sleep(self._download_delay_sec)
        # TODO PERFORMACE configure concurrency
        await self._s3cli.download_objects_to_file(S3Paths(s3paths), local_paths)
        for obj, loc in self._args.files.items():
            # Could parallelize. Probably not worth it
            crc = crc64nvme_b64(root / loc)
            if crc != obj.crc64nvme:
                raise ChecksumMismatchError(
                    f"The expected CRC64/NMVE checksum '{obj.crc64nvme}' for the path "
                    + f"'{obj.file}' does not match the actual checksum '{crc}'"
                )
        if job.job_input.script:
            (root / SCRIPT_FILENAME).chmod(0o755)

    def _get_log_prefix(self, job: models.AdminJobDetails):
        return f"cts-{job.id}-{self._cfg.container_number}-container"

    def _build_mounts(self, job: models.AdminJobDetails) -> dict[str, tuple[str, bool]]:
        input_ = self._workdir / _INPUT
        output = self._workdir / _OUTPUT
        if job.job_input.params.declobber:
            output = output / str(self._cfg.container_number)
        # Needs to be global write since the job container user is unknown
        output.mkdir(0o777, parents=True, exist_ok=True)
        output.chmod(0o777)  # bypass process umask
        if self._cfg.mount_prefix_override:
            prefix, replace = [Path(x) for x in self._cfg.mount_prefix_override.split(":")]
            relative_in = input_.relative_to(prefix)
            relative_out = output.relative_to(prefix)
            input_ = replace / relative_in
            output = replace / relative_out
        mounts = {
            # if people want to write to their input directory, ok
            str(input_): (job.job_input.params.input_mount_point, True),
            str(output): (job.job_input.params.output_mount_point, True),
        }
        if job.image.refdata_id:
            host_mount = str(Path(self._cfg.refdata_host_path) / job.image.refdata_id)
            # Don't allow refdata write
            mounts[host_mount] = (job.get_refdata_mount_point(), False)
            self._logr.info(
                f"Mounting host refdata at '{host_mount}' to '{job.get_refdata_mount_point()}' "
                + "in container"
            )
        return mounts

    async def _run_container(
        self, job: models.AdminJobDetails
    ) -> ContainerResult:
        await self._update_job_state_loop(job, models.JobState.JOB_SUBMITTING)
        mounts = self._build_mounts(job)
        stdout_path = self._workdir / (self._get_log_prefix(job) + ".out")
        stderr_path = self._workdir / (self._get_log_prefix(job) + ".err")
        # Touch before starting so error processing always has files to upload,
        # even if the container crashes immediately or times out during start_container.
        stdout_path.touch()
        stderr_path.touch()
        self._logr.info(
            f"Starting image {job.image.name_with_digest} with command:\n{self._args.args}"
        )
        self._runner = await self._container_creator.start_container(
            job.image.name_with_digest,
            stdout_path,
            stderr_path,
            mounts=mounts,
            command=self._args.args,
            env=self._args.env,
            gpus=job.job_input.gpus,
        )
        async with self._runner:
            # inside the context manager so the container gets canceled if this line raises
            await self._update_job_state_loop(job, models.JobState.JOB_SUBMITTED)
            # If timeout fired during start_container, self._runner wasn't assigned so the
            # timeout task couldn't cancel it then; cancel here and proceed to error processing.
            if self._timed_out:
                result = await self._runner.cancel()
            else:
                result = await self._runner.wait()
        self._logr.info(
            f"Container exited with code {result.exit_code}, "
            f"max memory: {result.max_memory_bytes} bytes, "
            f"CPU hours: {result.cpu_hours}"
        )
        return result

    def _append_timeout_to_logs(self, job: models.AdminJobDetails) -> None:
        timeout_days = self._cfg.job_timeout_min / (60 * 24)
        msg = (
            f"\n=== Job exceeded the maximum allowed runtime of "
            f"{timeout_days:.3f} days and was terminated ===\n"
        ).encode()
        stderr_path = self._workdir / (self._get_log_prefix(job) + ".err")
        with open(stderr_path, "ab") as f:
            f.write(msg)

    async def _process_error_state(
        self, job: models.AdminJobDetails, result: ContainerResult
    ):
        await self._update_job_state_loop(
            job, models.JobState.ERROR_PROCESSING_SUBMITTING,
            exit_code=result.exit_code,
            cpu_hours=result.cpu_hours,
            max_memory_bytes=result.max_memory_bytes,
            runtime_seconds=result.runtime_seconds,
        )
        self._logr.info("Uploading error logs")
        # Nothing to do prior to updating state again
        await self._update_job_state_loop(job, models.JobState.ERROR_PROCESSING_SUBMITTED)
        stdout = self._workdir / (self._get_log_prefix(job) + ".out")
        stderr = self._workdir / (self._get_log_prefix(job) + ".err")
        outcrc = crc64nvme_b64(stdout)
        errcrc = crc64nvme_b64(stderr)
        s3outpath, s3errpath = get_filenames_for_container(self._cfg.container_number)
        if self._error_upload_delay_sec:
            await asyncio.sleep(self._error_upload_delay_sec)
        # TODO PERF config / set concurrency
        await self._s3cli.upload_objects_from_file(
            S3Paths([
                f"{self._cfg.s3_error_log_path.strip('/')}/{s3outpath}",
                f"{self._cfg.s3_error_log_path.strip('/')}/{s3errpath}",
            ]),
            [stdout, stderr],
            [outcrc, errcrc]
        )
        await self._update_job_state_loop(
            job, models.JobState.ERROR, admin_error=f"Container exit code: {result.exit_code}"
        )

    async def _process_complete_state(
        self, job: models.AdminJobDetails, result: ContainerResult
    ):
        await self._update_job_state_loop(
            job, models.JobState.UPLOAD_SUBMITTING,
            exit_code=0,
            cpu_hours=result.cpu_hours,
            max_memory_bytes=result.max_memory_bytes,
            runtime_seconds=result.runtime_seconds,
        )
        # Nothing to do prior to updating state again
        await self._update_job_state_loop(job, models.JobState.UPLOAD_SUBMITTED)
        outdir = self._workdir / _OUTPUT
        outfiles = [file.relative_to(outdir) for file in outdir.rglob('*') if file.is_file()]
        crcs = [crc64nvme_b64(outdir / o) for o in outfiles]
        s3paths = [f"{job.job_input.output_dir.strip('/')}/{p}" for p in outfiles]
        filerecs = []
        for i, (file, crc, s3path) in enumerate(zip(outfiles, crcs, s3paths), start=1):
            filerecs.append(f"""
File #{i} CRC64NVME: {crc}
S3 Path: {s3path}
Local relative path: {file}
"""
            )
        self._logr.info("Uploading files:" + "===".join(filerecs))
        if outfiles:
            if self._upload_delay_sec:
                await asyncio.sleep(self._upload_delay_sec)
            await self._s3cli.upload_objects_from_file(
                S3Paths(s3paths), [outdir / o for o in outfiles], crcs
            )
        await self._update_job_state_loop(
            job, models.JobState.COMPLETE, outputs=[
                models.S3File(file=f, crc64nvme=c) for f, c in zip(s3paths, crcs)
            ]
        )


async def run_executor(
    stderr: TextIO,
    *,
    _download_delay_sec: float = 0,
    _upload_delay_sec: float = 0,
    _error_upload_delay_sec: float = 0,
) -> int:
    """
    Run the job executor.

    stderr - a stderr stream.

    Returns 0 for success, a positive integer for the container exit code, or -1 if an error
    occurred other than a container error.
    """
    stderr.write(f"Executor version: {VERSION} githash: {GIT_COMMIT}\n")
    cfg = Config()
    stderr.write("Executor config:\n")
    for k, v in cfg.safe_dump().items():
        stderr.write(f"{k}: {v}\n")
    stderr.write("\n")
    async with Executor(
        cfg,
        _download_delay_sec=_download_delay_sec,
        _upload_delay_sec=_upload_delay_sec,
        _error_upload_delay_sec=_error_upload_delay_sec,
    ) as exe:
        return await exe.execute()


class RetryableExecutorError(Exception):
    """ An error thrown when the executor fails but the error is potentially retryable. """


class FatalExecutorError(Exception):
    """ An error thrown when the executor fails fatally. """
