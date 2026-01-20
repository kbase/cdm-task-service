"""
An minimal async client for the JAWS central server.
"""

# API docs: https://jaws-api.jgi.doe.gov/api/v2/ui/#/


import aiohttp
import aiohttp_socks
import datetime
from enum import Enum
import logging
import os
from typing import Any

from cdmtaskservice.arg_checkers import require_string as _require_string, not_falsy as _not_falsy
from cdmtaskservice.jaws.config import JAWSConfig


class JAWSResult(Enum):
    """
    An enum of the possible JAWS result states.
    """
    SUCCESS = 1
    """
    The job succeeded.
    """
    FAILED = 2
    """
    The job ran but failed.
    """
    CANCELED = 3
    """
    The job was canceled.
    """
    SYSTEM_ERROR = 10
    """
    A JAWS system error prevented the job from running. Likely no output files are available.
    """


class JAWSClient:
    """ The JAWS client. """
    
    @classmethod
    async def create(self, config: JAWSConfig):
        """
        Initialize the client from a configuration.
        """
        cli = JAWSClient(_not_falsy(config, "config").url, config.token)
        try:
            user = await cli._user()  # test connection & token
        except Exception:
            await cli.close()
            raise
        if user != config.user:
            await cli.close()
            raise ValueError(
                f"JAWS client expected user {config.user} for token, but got {user}"
            )
        cli._logr.info(f"Initialized JAWS client with user {user}")
        return cli
    
    def __init__(self, url: str, token: str):
        url = _require_string(url, "url")
        if not url.endswith("/"):
            url += "/"
        url += "api/v2/"
        headers = {"Authorization": f"Bearer {_require_string(token, 'token')}"}
        self._logr = logging.getLogger(__name__)
        pxurl = os.environ.get("JAWS_HTTPS_PROXY")
        if pxurl:
            # just used for testing so not advertised for now
            self._logr.info(f"Using proxy {pxurl}")
            proxy = aiohttp_socks.ProxyConnector.from_url(pxurl)
            self._sess = aiohttp.ClientSession(base_url=url, headers=headers, connector=proxy)
        else:
            self._sess = aiohttp.ClientSession(base_url=url, headers=headers)

    async def _get(self, url, params=None) -> dict[str, Any]:
        return await self._req("GET", url, params=params)
    
    async def _put(self, url, params=None) -> dict[str, Any]:
        return await self._req("PUT", url, params=params)
        
    async def _req(self, method, url, params=None) -> dict[str, Any]:
        async with self._sess.request(method=method, url=url, params=params) as res:
            # Any jaws errors would be 500 errors since should just be querying known jobs, so
            # don't worry too much about exceptions. Expand later if needed
            # May need to add retries
            # May need to to add some sort of down notification or detection
            if res.ok:  # assume here that if we get a 2XX we get JSON. Fix if necessary
                return await res.json()
            try:
                json = await res.json()
            except Exception as e:
                self._logr.exception(f"JAWS error response:\n{await res.text()}")
                raise JAWSClientError("Got non-JSON error response from JAWS") from e
            # Assume here we're talking to jaws
            err = json["detail"]["error"]
            if "too late to cancel" in err:
                raise JAWSJobCanceledError("Job is canceled")
            res.raise_for_status()

    async def _user(self) -> str:
        res = await self._get("user")
        return res["uid"]
    
    async def status(self, run_id: str) -> dict[str, Any]:
        """
        Get the status of a JAWS run.
        """
        try:
            res = await self._get(
                f"run/{_require_string(run_id, 'run_id')}",
                params={"verbose": "true", "local_tz": "UTC"}
            )
        except aiohttp.client_exceptions.ClientResponseError as e:
            if e.status == 404:
                raise NoSuchJAWSJobError(run_id) from e
            raise
        res["submitted"] = _add_tz(res["submitted"])
        res["updated"] = _add_tz(res["updated"])
        return res
    
    async def logs(self, run_id: str) -> list[tuple[str, str, datetime.datetime, str]]:
        """ Get the logs from a JAWS run. """
        try:
            res = await self._get(
                f"run/{_require_string(run_id, 'run_id')}/run_log",
                params={"local_tz": "UTC"}
            )
        except aiohttp.client_exceptions.ClientResponseError as e:
            if e.status == 404:
                raise NoSuchJAWSJobError(run_id) from e
            raise
        data = res["data"]
        for d in data:
            d[2] = _add_tz(d[2])
        return data
    
    async def is_site_up(self, site: str) -> bool:
        """ Check whether a JAWS site is up. """
        res = await self._get(f"status/{_require_string(site, 'site')}")
        if res[f"{site}-Site"] == "UNKNOWN SITE":
            raise ValueError(f"No such JAWS site: {site}") 
        return all(v == "UP" for v in res.values())
    
    async def cancel(self, run_id: str) -> dict[str, Any]:
        """ Cancel a job. """
        try:
            return await self._put(f"run/{_require_string(run_id, 'run_id')}/cancel")
        except JAWSJobCanceledError:
            self._logr.info(f"JAWS job {run_id} is already canceled - noop")
            # do nothing
        except aiohttp.client_exceptions.ClientResponseError as e:
            if e.status == 404:
                raise NoSuchJAWSJobError(run_id) from e
            raise
    
    async def close(self):
        await self._sess.close()


def _add_tz(timestr: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(timestr).replace(tzinfo=datetime.timezone.utc)


# In lieu of making a pydantic model for the jaws status output, for now we just add little
# helper methods. If this gets too gross make the model.
def is_done(job: dict[str, Any]) -> bool:
    """
    Given a JAWS status dictionary, determine if the job is done.
    """
    return job["status"] == "done"


_JAWS_RES_TO_ENUM = {
    "succeeded": JAWSResult.SUCCESS,
    "failed": JAWSResult.FAILED,
    "cancelled": JAWSResult.CANCELED,
    None: JAWSResult.SYSTEM_ERROR,
}
# TODO RELIABILITY cancelled is a possible result, but can be cancelled and still be null.
#                  if null, check the jaws logs for a cancelled state


def result(job: dict[str, Any]) -> JAWSResult:
    """
    Given a JAWS status dictionary, determine the result of the job.
    """
    return _JAWS_RES_TO_ENUM[job["result"]]


class JAWSClientError(Exception):
    """ General class for JAWS client errors. """


class NoSuchJAWSJobError(JAWSClientError):
    """ Thrown when a a jaws run ID does not exist. """


class JAWSJobCanceledError(JAWSClientError):
    """ Raised when a job is in the canceled state and the operatoin is therefore illegal. """
