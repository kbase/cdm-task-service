"""
An minimal async client for the JAWS central server.
"""

import aiohttp
import datetime
import logging
from typing import Any

from cdmtaskservice.arg_checkers import require_string as _require_string


class JAWSClient:
    """ The JAWS client. """
    
    @classmethod
    async def create(self, url: str, token: str):
        """
        Initialize the client.
        
        url - the JAWS Central URL.
        token - the JAWS token.
        """
        cli = JAWSClient(url, token)
        user = await cli._user()  # test connection & token
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

    async def _user(self) -> str:
        res = await self._get("user")
        return res["uid"]
    
    async def status(self, run_id: str) -> dict[str, Any]:
        """
        Get the status of a JAWS run.
        """
        res = await self._get(
            f"run/{_require_string(run_id, 'run_id')}",
            params={"verbose": "true", "local_tz": "UTC"}
        )
        res["submitted"] = _add_tz(res["submitted"])
        res["updated"] = _add_tz(res["updated"])
        return res
    
    async def close(self):
        await self._sess.close()


def _add_tz(timestr: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(timestr).replace(tzinfo=datetime.timezone.utc)
