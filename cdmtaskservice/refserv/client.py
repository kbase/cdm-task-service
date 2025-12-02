"""
A very basic client for the refdata service, only covering  functionality needed by the CTS.
"""
# TODO CODE I keep using this pattern over and over since all my services have a similar error
#           structure. Maybe make a client for it?
# TODO CODE This is very similar to the code in executor.py, refactor (see above)
# TODO RELIABILITY add retries

import aiohttp
import json
import logging
from typing import Self, Any

from cdmtaskservice.arg_checkers import require_string as _require_string, not_falsy as _not_falsy
from cdmtaskservice import sites


class RefdataServiceClient:
    """ A client for the CTS refdata service. """
    
    @classmethod
    async def create(cls, url: str, token: str) -> Self:
        """
        Create the client
        
        url - the url of the refdata server.
        token - the token to use to contact the refdata server.
        """
        c = cls(url, token)
        try:
            # TDOO CODE may wan to call a method that checks the token has the right roles.
            #           Don't really want to expose the CTS role to users, though
            await c._check_server()
        except Exception:
            await c.close()
            raise
        return c
        
        
    def __init__(self, url: str, token: str):
        self._url = _require_string(url, "url").rstrip().rstrip("/")
        self._sess = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {_require_string(token, 'token')}"}
        )
        
    async def _check_resp(self, resp: aiohttp.ClientResponse, action: str
    ) -> dict[str, Any] | None:
        logr = logging.getLogger(__name__)
        if resp.status == 204:  # no content
            return None
        try:
            resjson = await resp.json()
        except Exception:
            err = "Non-JSON response from CDM Refdata Service, status code: " + str(resp.status)
            # TODO TEST logging
            logr.exception("%s, response:\n%s", err, await resp.text())
            raise RefdataServerError(err)
        if resp.status != 200:
            # assume we're talking to the Refdata server at this point
            logr.error(f"{action}. Response contents:\n{json.dumps(resjson, indent=2)}")
            msg = f"{action}: {resjson['error']['message']}"
            raise RefdataServerError(msg)
        return resjson
    
    async def _check_server(self):
        async with self._sess.get(self._url,) as resp:
            r = await self._check_resp(resp, "Failed to contact the CDM Refdata Service")
            if r.get("service_name") != "CDM Refdata Service":
                raise RefdataServerError(
                    f"The service at {self._url} is not the refdata servvice"
                )

    async def stage_refdata(self, refdata_id, cluster: sites.Cluster):
        """
        Start the refdata staging process.
        
        refdata_id - the ID of the refdata in the CDM Task Service.
        cluster - the cluter for which the refdata is being deployed.
        """
        _require_string(refdata_id, "refdata_id")
        _not_falsy(cluster, "cluster")
        url = f"{self._url}/refdata/{refdata_id}/{cluster.value}"
        async with self._sess.post(url) as resp:
            await self._check_resp(resp, "Failed staging refdata")

    async def close(self):
        """
        Release resources associated with the client instance.
        """
        await self._sess.close()


class RefdataServerError(Exception):
    """ Thrown when an error occurs contacting the refdata service. """
