"""
NERSC Superfacilty API (SFAPI) client manager. Its primary purpose is to hide the client
credentials periodic expiration from the rest of the system.
"""

import asyncio
import datetime
import logging
import os
from pathlib import Path
from queue import PriorityQueue
import time
from typing import Self

from sfapi_client import AsyncClient
from sfapi_client.users import AsyncUser

from cdmtaskservice import logfields
from cdmtaskservice.arg_checkers import require_string, not_falsy


# TODO TEST add manual & automated tests
# TODO NERSCFEATURE this could be made much simpler with
#       https://github.com/NERSC/sfapi_client/issues/91

# Might want to make these configurable
_BACKOFF = (1, 2, 5, 10, 30, 60)
_CHECK_CREDFILE_SEC = 600
_CHECK_FOR_CLIENT_CLOSE_SEC = 60
_CLIENT_CLOSE_DELAY_SEC = 3600


class NERSCSFAPIClientProvider:
    """
    Provides a SFAPI client to a client and manages client credential expiration.
    
    This class is not thread safe.
    """
    # Could probably make it thread safe if needed later

    @classmethod
    async def create(
        cls,
        credential_path: Path,
        expected_user: str,
    ) -> Self:
        """
        Create the provider.
        
        credential_path - a path to an SFAPI credential file. The first line of the file
            must be the client ID, and the rest the client secret in PEM format.
        expected_user - the expected NERSC username for the client. If the username does
            not match an error will be thrown when fetching the client.
        """
        cp = NERSCSFAPIClientProvider(credential_path, expected_user)
        await cp._update_client(False)
        # keep a reference to the task or the coroutine can be GC'd
        cp._cred_watch_task = asyncio.create_task(cp._watch_for_changes())
        cp._client_close_task = asyncio.create_task(cp._close_clients())
        return cp

    def __init__(self, credential_path: Path, expected_user: str):
        self._credpath = not_falsy(credential_path, "credential_path")
        self._user = require_string(expected_user, "expected_user")
        self._credfile_last_mod = os.path.getmtime(credential_path)
        self._client = None
        self._clients_to_close = PriorityQueue()
        self._destroy = False
        
    async def _watch_for_changes(self):
        # tried using watchfiles here but couldn't get it to work
        while not self._destroy:
            await asyncio.sleep(_CHECK_CREDFILE_SEC)
            modtime = os.path.getmtime(self._credpath)
            if self._credfile_last_mod != modtime:
                self._credfile_last_mod = modtime
                logging.getLogger(__name__).info(
                    f"Detected SFAPI credential at {self._credpath} "
                    + f"changed at {time.time()}, reloading")
                await self._update_client(True)

    async def _close_clients(self):
        while not self._destroy:
            await asyncio.sleep(_CHECK_FOR_CLIENT_CLOSE_SEC)
            now = time.time()
            while not self._clients_to_close.empty():
                replacetime, cli, cli_id = self._clients_to_close.queue[0]
                if now - replacetime > _CLIENT_CLOSE_DELAY_SEC:
                    await _close_client(cli, cli_id, replacetime=replacetime)
                    # assume here that a client can't be added to the queue that sorts earlier
                    # than the current client
                    self._clients_to_close.get()
                else:
                    break
    
    async def _update_client(self, infinite_recovery: bool):
        # TODO TEST this is going to need some nasty patching
        index = -1
        while index is not None:
            client = None
            index = _get_next_index(index, infinite_recovery)
            try:
                # Could look into aiofiles here
                with open(self._credpath) as f:
                    client_id = f.readline().strip()
                    secret = f.read().strip()
                if not client_id or not secret:
                    raise ValueError(f"SFAPI credentials file at {self._credpath} is malformed.")
                client = AsyncClient(client_id=client_id, secret=secret)
                cliuser = await client.user()
                if cliuser.name != self._user:
                    raise ValueError(f"Configured NERSC user is {self._user} "
                           + f"but the SFAPI credentials are for {cliuser.name}")
                exp = await _get_expiration(cliuser, client_id)
                if self._destroy:  # Don't add another client to the close list if destroyed
                    await client.close()
                    return
                # Don't await after this point or a client could be added to the destroy list
                # and never be closed
                if self._client:
                    self._clients_to_close.put((time.time(), self._client[0], self._client[1]))
                logging.getLogger(__name__).info(
                    f"Created SFAPI client id {client_id} at {hex(id(client))}")
                self._client = (client, client_id, exp)
                index = None
            except Exception as e:
                if client:
                    await client.close()
                err = f"Failed to update NERSC SFAPI client from credentials file: {e}"
                if index is None:
                    raise ValueError(err) from e
                err += f"\n*** Reloading creds in {_BACKOFF[index]}s ***."
                logging.getLogger(__name__).exception(
                    err, extra={logfields.NEXT_ACTION_SEC: _BACKOFF[index]}
                )
                await asyncio.sleep(_BACKOFF[index])

    async def destroy(self):
        """ Close the client and stop watching for file changes. """
        self._destroy = True
        self._cred_watch_task.cancel()
        self._client_close_task.cancel()
        while not self._clients_to_close.empty():
            replacetime, cli, cli_id = self._clients_to_close.get()
            await _close_client(cli, cli_id, destroy=True, replacetime=replacetime)
        await _close_client(self._client[0], self._client[1], destroy=True)
    
    def get_client(self) -> AsyncClient:
        """
        Get the client.
        
        Do not keep a reference to the client; dispose of it as soon as the call is complete
        and get a new client for the new call.
        """
        self._check_destroy()
        return self._client[0]

    def get_client_id(self) -> str:
        """ Get ID of the client, an opaque string. """
        self._check_destroy()
        return self._client[1]

    def expiration(self) -> datetime.datetime:
        """ Get the expiration time of the current set of credentials. """
        self._check_destroy()
        return self._client[2]

    def _check_destroy(self):
        if self._destroy:
            raise ValueError("client has been destroyed")


async def _close_client(
    client: AsyncClient,
    client_id: str,
    replacetime: float = None,
    destroy: bool = False
):
    msg = f"Closing SFAPI client id {client_id} at {hex(id(client))}"
    if replacetime is not None:
        msg += f", replaced at {replacetime}"
    if destroy:
        msg += " as part of the destroy routine"
    logging.getLogger(__name__).info(msg)
    await client.close()


def _get_next_index(index: int, infinite_recovery: bool) -> int | None:
    if index < len(_BACKOFF) - 1:
        return index + 1
    return index if infinite_recovery else None


async def _get_expiration(user: AsyncUser, client_id: str) -> datetime.datetime:
    clients = await user.clients()
    for cli in clients:
        if cli.clientId == client_id:
            return datetime.datetime.fromisoformat(cli.expiresAt)
    # This should be impossible
    raise ValueError(f"NERSC returned no client matching client ID {client_id}")
