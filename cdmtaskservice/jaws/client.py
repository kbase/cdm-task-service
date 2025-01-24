"""
An minimal async client for the JAWS central server.
"""

import aiohttp
import datetime
from enum import Enum
import logging
from typing import Any

from cdmtaskservice.arg_checkers import require_string as _require_string


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
    SYSTEM_ERROR = 3
    """
    A JAWS system error prevented the job from running. Likely no output files are available.
    """


class JAWSClient:
    """ The JAWS client. """
    
    @classmethod
    async def create(self, url: str, token: str, expected_user: str):
        """
        Initialize the client.
        
        url - the JAWS Central URL.
        token - the JAWS token.
        expected_user - the user for the token.
        """
        cli = JAWSClient(url, token)
        try:
            user = await cli._user()  # test connection & token
        except Exception:
            await cli.close()
            raise
        if user != expected_user:
            await cli.close()
            raise ValueError(
                f"JAWS client expected user {expected_user} for token, but got {user}"
            )
        logging.getLogger(__name__).info(f"Initialized JAWS client with user {user}")
        return cli
    
    def __init__(self, url: str, token: str):
        url = _require_string(url, "url")
        if not url.endswith("/"):
            url += "/"
        self._sess = aiohttp.ClientSession(
            base_url=url,
            headers={"Authorization": f"Bearer {_require_string(token, 'token')}"}
        )

    async def _get(self, url, params=None) -> dict[str, Any]:
        async with self._sess.get(url, params=params) as res:
            # Any jaws errors would be 500 errors since should just be querying known jobs, so
            # don't worry too much about exceptions. Expand later if needed
            # May need to add retries
            # May need to to add some sort of down notification or detection
            res.raise_for_status()
            if res.ok:  # assume here that if we get a 2XX we get JSON. Fix if necessary
                return await res.json()
            else:
                raise ValueError("not sure how this is possible, how exciting")

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
    None: JAWSResult.SYSTEM_ERROR,
}
# TODO RELIABILITY cancelled is a possible result, but can be cancelled and still be null.
#                  if null, check the jaws logs for a cancelled state


def result(job: dict[str, Any]) -> JAWSResult:
    """
    Given a JAWS status dictionary, determine the result of the job.
    """
    return _JAWS_RES_TO_ENUM[job["result"]]


class NoSuchJAWSJobError(Exception):
    """ Thrown when a a jaws run ID does not exist. """
