"""
Get the status of the NERSC perlmutter and DTN systems without authentication.
"""

import datetime
from sfapi_client import AsyncClient, StatusValue, OutageStatusValue
from sfapi_client.compute import Machine
from typing import NamedTuple
import asyncio


class Status(NamedTuple):
    """ The status of the NERSC compute systems. """
    
    ok: bool
    """ True if all systems are available. """
    
    perlmutter_up: bool
    """ True if perlmutter is available. """
    
    dtns_up: bool
    """ True if the DTNs are available. """
    
    perlmutter_available: datetime.datetime | None
    """ A date for when perlmutter is expected to be up if available. """
    
    dtns_available: datetime.datetime | None
    """ A date for when the DTNs are expected to be up if available. """
    
    perlmutter_description: str | None
    """ A free text description of the perlmutter issue, if any. """
    
    dtns_description: str | None
    """ A free text description of the DTN issue, if any. """

    @property
    def systems_available(self) -> datetime.datetime | None:
        """ A date for when perlmutter and the DTNs are expected to be up if available. """
        if self.perlmutter_available and self.dtns_available:
            return max(self.perlmutter_available, self.dtns_available)
        return self.dtns_available if self.dtns_available else self.perlmutter_available


class NERSCStatus:
    """ Checks NERSC status. """
    
    def __init__(self):
        """
        Create the status checker. Does not confirm connectivity to NERSC at startup.
        """
        self._cli = AsyncClient()
        
    async def status(self) -> Status:
        async with asyncio.TaskGroup() as tg:
            perl = tg.create_task(self._get_status(Machine.perlmutter))
            dtns = tg.create_task(self._get_status(Machine.dtns))

        perl_ok, perl_up, perl_desc = perl.result()
        dtns_ok, dtns_up, dtns_desc = dtns.result()
        return Status(
            ok=perl_ok and dtns_ok,
            perlmutter_up=perl_ok,
            dtns_up=dtns_ok,
            perlmutter_available=perl_up,
            dtns_available=dtns_up,
            perlmutter_description=perl_desc,
            dtns_description=dtns_desc,
        )
    
    async def _get_status(self, m: Machine) -> tuple[bool, datetime.datetime, str]:
        ac = await self._cli.compute(m)
        if ac.status != StatusValue.active:
            outages = await self._cli.resources.outages(ac.name)
            for o in outages:
                if o.status == OutageStatusValue.Active:
                    return False, o.end_at, o.description
            raise ValueError(f"NERSC resource {m} is inactive but found no outage")
        return True, None, None

    async def close(self):
        """ Close the status client. Further calls will result in unspecified errors. """
        await self._cli.close()
