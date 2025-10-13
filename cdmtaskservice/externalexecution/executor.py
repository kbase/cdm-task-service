"""
The main CTS external executor class.
"""

import aiohttp
import asyncio
import json
import logging
import sys
from typing import TextIO, Any

from cdmtaskservice.externalexecution.config import Config
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice import models
from cdmtaskservice.version import VERSION


logging.basicConfig()


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
        """ Run the executor. """
        job = await self._get_job()
        print(job.model_dump_json(indent=2))
    
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
    
    async def _get_job(self) -> models.AdminJobDetails:
        # TODO RELIABILITY retries. Tenatcity might be useful
        url = f"{self._url}/admin/jobs/{self._cfg.job_id}"
        # If we can't get the job, we presumably can't update the job either, so we just throw any
        # exceptions.
        async with self._sess.get(url) as resp:
            jobjson = await self._check_resp(resp, "Failed to get job from the CDM Task Service")
        return models.AdminJobDetails.model_validate(jobjson)


async def run_executor(stdout: TextIO, stderr: TextIO):
    stdout.write(f"Executor version: {VERSION} githash: {GIT_COMMIT}\n")
    cfg = Config()
    stdout.write("Executor config:\n")
    for k, v in cfg.safe_dump().items():
        stdout.write(f"{k}: {v}\n")
    async with Executor(cfg) as exe:
        await exe.execute();


class RetryableExecutorError(Exception):
    """ An error thrown when the executor fails but the error is potentially retryable. """


class FatalExecutorError(Exception):
    """ An error thrown when the executor fails fatally. """
