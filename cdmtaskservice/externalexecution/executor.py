"""
The main CTS external executor class.
"""

import aiohttp
import asyncio
import json
import logging
from pathlib import Path
import sys
import time
import traceback
from typing import TextIO, Any

from cdmtaskservice.argument_generator import ArgumentGenerator
from cdmtaskservice.exceptions import ChecksumMismatchError
from cdmtaskservice.externalexecution.config import Config
from cdmtaskservice.externalexecution import container_runner
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


class Executor:
    """ The executor. Note that the executor is not concurrency safe. """
    
    def __init__(self, cfg: Config):
        """ Create the executor from the configuration. """
        self._cfg = cfg
        self._url = self._cfg.cts_url.rstrip("/")
        self._sess = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {cfg.get_cts_token()}"}
        )
        self._logr = logging.getLogger(__name__)
        self._args = None
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def close(self):
        """ Close any resources associated with the executor. """
        await self._sess.close()
    
    async def execute(self):
        """ Run the executor. Returns True for success, False for fail. """
        await self._log_service_ver()
        # If we can't get the job, we presumably can't update the job either, so we just throw any
        # exceptions.
        job = await self._get_job()
        # we assume here that the service has already error checked the job input
        try:
            self._args = ArgumentGenerator(job).get_container_arguments(self._cfg.container_number)
            await self._download_files(job)
            await self._update_job_state_loop(job, models.JobState.JOB_SUBMITTING)
            exit_code = await self._run_container(job)  # updates to JOB_SUBMITTED
            if exit_code > 0:
                await self._process_error_state(job, exit_code)
            else:
                await self._process_complete_state(job)
        except Exception as e:
            self._logr.exception(f"Job failed: {e}")
            await self._update_job_state_loop(job, models.JobState.ERROR, exception=e)
            return False
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
                    f"Failed updating job state to {state.value} at {utcdatetime().isoformat()}, "
                    + f"trying again in {backoff} seconds: {e}"
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
        admin_error: str = None,
        exception: Exception = None
    ):
        url = (
            f"{self._url}/external_exec/jobs/{job.id}/"
            + f"container/{self._cfg.container_number}/update/{state.value}"
        )
        # TODO TEST need to mockout utcdatetime
        data = {"time": utcdatetime().isoformat()}
        if exception:
            data["admin_error"] = str(exception)
            data["traceback"] = traceback.format_exc()
        elif admin_error:
            data["admin_error"] = admin_error
        if exit_code is not None:
            data['exit_code'] = exit_code
        if outputs:
            data["outputs"] = [o.model_dump() for o in outputs]
        async with self._sess.put(url, json=data) as resp:
            await self._check_resp(resp, "Failed to update job state in the CDM Task Service")

    async def _download_files(self, job: models.AdminJobDetails) -> dict[models.S3File, str]:
        s3paths = []
        local_paths = []
        root = Path(".") / _INPUT
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
        self._logr.info("Processing files:" + "===".join(filerecs))
        self._s3cli = await S3Client.create(
            self._cfg.s3_url,
            self._cfg.s3_access_key,
            self._cfg.get_s3_access_secret(),
            insecure_ssl=self._cfg.s3_insecure
        )
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

    def _get_log_prefix(self, job: models.AdminJobDetails):
        return f"cts-{job.id}-{self._cfg.container_number}-container"

    async def _run_container(self, job: models.AdminJobDetails) -> int:
        mount = Path.cwd().expanduser().absolute()
        input_ = mount / _INPUT
        output = mount / _OUTPUT
        if job.job_input.params.declobber:
            output = output / str(self._cfg.container_number)
        # Needs to be global write since the job container user is unknown
        output.mkdir(0x777, parents=True, exist_ok=True)
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
        self._logr.info(
            f"Starting image {job.image.name_with_digest} with command:\n{self._args.args}"
        )
        exit_code = await container_runner.run_container(
            job.image.name_with_digest,
            Path(".") / (self._get_log_prefix(job) + ".out"),
            Path(".") / (self._get_log_prefix(job) + ".err"),
            mounts=mounts,
            command=self._args.args,
            env=self._args.env,
            post_start_callback=self._update_job_state_loop(job, models.JobState.JOB_SUBMITTED),
            sigterm_callback=lambda signum: sys.exit(128 + signum)
        )
        self._logr.info(f"Container exited with code {exit_code}")
        return exit_code

    async def _process_error_state(self, job: models.AdminJobDetails, exit_code: int):
        await self._update_job_state_loop(
            job, models.JobState.ERROR_PROCESSING_SUBMITTING, exit_code=exit_code
        )
        # Nothing to do prior to updating state again
        await self._update_job_state_loop(job, models.JobState.ERROR_PROCESSING_SUBMITTED)
        stdout = Path(".") / (self._get_log_prefix(job) + ".out")
        stderr = Path(".") / (self._get_log_prefix(job) + ".err")
        outcrc = crc64nvme_b64(stdout)
        errcrc = crc64nvme_b64(stderr)
        s3outpath, s3errpath = get_filenames_for_container(self._cfg.container_number)
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
            job, models.JobState.ERROR, admin_error=f"Container exit code: {exit_code}"
        )
        
    async def _process_complete_state(self, job: models.AdminJobDetails):
        await self._update_job_state_loop(
            job, models.JobState.UPLOAD_SUBMITTING, exit_code=0
        )
        # Nothing to do prior to updating state again
        await self._update_job_state_loop(job, models.JobState.UPLOAD_SUBMITTED)
        outdir = Path(_OUTPUT)
        outfiles = [file.relative_to(outdir) for file in outdir.rglob('*') if file.is_file()]
        crcs = [crc64nvme_b64(outdir / o) for o in outfiles]
        s3paths = [f"{job.job_input.output_dir.strip('/')}/{p}" for p in outfiles]
        if outfiles:
            await self._s3cli.upload_objects_from_file(
                S3Paths(s3paths), [outdir / o for o in outfiles], crcs
            )
        await self._update_job_state_loop(
            job, models.JobState.COMPLETE, outputs=[
                models.S3File(file=f, crc64nvme=c) for f, c in zip(s3paths, crcs)
            ]
        )


async def run_executor(stderr: TextIO):
    """
    Run the job executor. 
    
    stderr - a stderr stream.
    
    Returns True for success, False for failure.
    """
    stderr.write(f"Executor version: {VERSION} githash: {GIT_COMMIT}\n")
    cfg = Config()
    stderr.write("Executor config:\n")
    for k, v in cfg.safe_dump().items():
        stderr.write(f"{k}: {v}\n")
    stderr.write("\n")
    async with Executor(cfg) as exe:
        return await exe.execute();


class RetryableExecutorError(Exception):
    """ An error thrown when the executor fails but the error is potentially retryable. """


class FatalExecutorError(Exception):
    """ An error thrown when the executor fails fatally. """
