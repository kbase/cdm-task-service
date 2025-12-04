""" A very basic client for the CTS for the purposes of managing reference data. """
# TODO CODE Here's this pattern again -  all my services have a similar error
#           structure and so the error response is similar. That being said, they all differ
#           in some ways so making a common function might be worse...
# TODO CODE This is very similar to the code in executor.py, refactor (see above)
# TODO RELIABILITY add retries


import aiohttp
import json
import logging
from typing import Self, Any

from cdmtaskservice.arg_checkers import require_string as _require_string
from cdmtaskservice import models


class CTSRefdataClient:
    """ A client for the CTS focusing on managing refdata downloads. """
    
    @classmethod
    async def create(cls, url: str, token: str) -> Self:
        """
        Create the client
        
        url - the url of the CTS.
        token - the token to use to contact the  CTS.
        """
        c = cls(url, token)
        # We deliberately don't call the CTS here to avoid a deadlock, as it's trying to 
        # do the same thing
        # TDOO CODE at some point will need a way of checking CTS <-> refdata connectivity
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
            err = "Non-JSON response from CDM Task Service, status code: " + str(resp.status)
            # TODO TEST logging
            logr.exception("%s, response:\n%s", err, await resp.text())
            raise CTSRefdataError(err)
        if resp.status != 200:
            # assume we're talking to the Refdata server at this point
            logr.error(f"{action}. Response contents:\n{json.dumps(resjson, indent=2)}")
            msg = f"{action}: {resjson['error']['message']}"
            raise CTSRefdataError(msg)
        return resjson
    
    async def _check_server(self):
        async with self._sess.get(self._url,) as resp:
            r = await self._check_resp(resp, "Failed to contact the CDM Task Service")
            if r.get("service_name") != "CDM Task Servicex":
                raise CTSRefdataError(
                    f"The service at {self._url} is not the task service"
                )

    async def get_refdata(self, refdata_id) -> models.ReferenceData:
        """
        Get refdata information from the CTS based on its ID.
        """
        _require_string(refdata_id, "refdata_id")
        url = f"{self._url}/refdata/id/{refdata_id}"
        async with self._sess.get(url) as resp:
            res = await self._check_resp(resp, "Failed staging refdata")
        return models.ReferenceData.parse_obj(res)

    async def close(self):
        """
        Release resources associated with the client instance.
        """
        await self._sess.close()


class CTSRefdataError(Exception):
    """ Thrown when an error occurs contacting the refdata service. """
