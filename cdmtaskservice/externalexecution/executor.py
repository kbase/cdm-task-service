"""
The main CTS external executor class.
"""

import aiohttp
import json
import logging
from pathlib import Path
from typing import TextIO, Any

from cdmtaskservice.exceptions import ChecksumMismatchError
from cdmtaskservice.externalexecution.config import Config
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice.input_file_locations import determine_file_locations
from cdmtaskservice import models
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.s3.remote import crc64nvme_b64
from cdmtaskservice.version import VERSION


logging.basicConfig(level=logging.INFO)


class Executor:
    """ The executor. """
    
    def __init__(self, cfg: Config):
        """ Create the executor from the configuration. """
        self._cfg = cfg
        self._url = self._cfg.cts_url.rstrip("/")
        self._sess = aiohttp.ClientSession(headers={"Authorization": f"Bearer {cfg.cts_token}"})
        self._logr = logging.getLogger(__name__)
    
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
            await self._download_files(job)
        except Exception as e:
            self._logr.exception(f"Job failed: {e}")
            # TODO EXECUTOR try to drain job state queue and update job state to error
            return False
        return True
    
    async def _check_resp(self, resp: aiohttp.ClientResponse, action: str) -> dict[str, Any]:
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
            root = await self._check_resp(resp, "Failed to get job from the CDM Task Service")
        self._logr.info(f"CTS version: {root['version']} githash: {root['git_hash']}")
    
    async def _get_job(self) -> models.AdminJobDetails:
        # TODO RELIABILITY retries. Tenatcity might be useful
        url = f"{self._url}/admin/jobs/{self._cfg.job_id}"
        async with self._sess.get(url) as resp:
            jobjson = await self._check_resp(resp, "Failed to get job from the CDM Task Service")
        return models.AdminJobDetails.model_validate(jobjson)

    async def _download_files(self, job: models.AdminJobDetails) -> dict[models.S3File, str]:
        s3objs = set(job.job_input.get_files_per_container()[self._cfg.container_number - 1])
        filelocs = determine_file_locations(job.job_input)
        filelocs = {k: v for k, v in filelocs.items() if k in s3objs}
        s3paths = []
        local_paths = []
        root = Path(".") / "__INPUT__"
        filerecs = []
        for i, (obj, loc) in enumerate(filelocs.items(), start=1):
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
            self._cfg.s3_access_secret,
            insecure_ssl=self._cfg.s3_insecure
        )
        # TODO PERFORMACE configure concurrency
        await self._s3cli.download_objects_to_file(S3Paths(s3paths), local_paths)
        for obj, loc in filelocs.items():
            # Could parallelize. Probably not worth it
            crc = crc64nvme_b64(root / loc)
            if crc != obj.crc64nvme:
                raise ChecksumMismatchError(
                    f"The expected CRC64/NMVE checksum '{obj.crc64nvme}' for the path "
                    + f"'{obj.file}' does not match the actual checksum '{crc}'"
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
