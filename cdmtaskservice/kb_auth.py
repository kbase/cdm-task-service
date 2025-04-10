"""
A client for the KBase Auth2 server.
"""

# Mostly copied from https://github.com/kbase/collections with a few tweaks.
# TODO make a KBase auth library?

import aiohttp
from cacheout.lru import LRUCache
from enum import IntEnum
import logging
import time
from typing import NamedTuple, Self

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy


class AdminPermission(IntEnum):
    """
    The different levels of admin permissions.
    """
    NONE = 1
    # leave some space for potential future levels
    FULL = 10


class KBaseUser(NamedTuple):
    user: str
    admin_perm: AdminPermission


async def _get(url, headers):
    # TODO PERF keep a single session and add a close method
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as r:
            await _check_error(r)
            return await r.json()


async def _check_error(r):
    if r.status != 200:
        try:
            j = await r.json()
        except Exception:
            err = "Non-JSON response from KBase auth server, status code: " + str(r.status)
            logging.getLogger(__name__).info("%s, response:\n%s", err, r.text)
            raise IOError(err)
        # assume that if we get json then at least this is the auth server and we can
        # rely on the error structure.
        if j["error"].get("appcode") == 10020:  # Invalid token
            raise InvalidTokenError("KBase auth server reported token is invalid.")
        # don't really see any other error codes we need to worry about - maybe disabled?
        # worry about it later.
        raise IOError("Error from KBase auth server: " + j["error"]["message"])


class KBaseAuth:
    """ A client for contacting the KBase authentication server. """

    @classmethod
    async def create(
        cls,
        auth_url: str,
        required_roles: list[str] = None,
        full_admin_roles: list[str] = None,
        cache_max_size: int = 10000,
        cache_expiration: int = 300,
    ) -> Self:
        """
        Create the client.
        auth_url - The root url of the authentication service.
        required_roles - The KBase Auth2 roles that the user must possess in order to be allowed
            to use the service.
        full_admin_roles -  The KBase Auth2 roles that determine that user is an administrator.
        cache_max_size -  the maximum size of the token cache.
        cache_expiration -  the expiration time for the token cache in
            seconds.
        """
        if not _not_falsy(auth_url, "auth_url").endswith("/"):
            auth_url += "/"
        j = await _get(auth_url, {"Accept": "application/json"})
        return KBaseAuth(
            auth_url,
            required_roles,
            full_admin_roles,
            cache_max_size,
            cache_expiration,
            j.get("servicename"),
        )

    def __init__(
        self,
        auth_url: str,
        required_roles: list[str],
        full_admin_roles: list[str],
        cache_max_size: int,
        cache_expiration: int,
        service_name: str,
    ):
        self._url = auth_url
        self._me_url = self._url + "api/V2/me"
        self._req_roles = set(required_roles) if required_roles else None
        self._full_roles = set(full_admin_roles) if full_admin_roles else set()
        self._cache_timer = time.time  # TODO TEST figure out how to replace the timer to test
        self._cache = LRUCache(
            timer=self._cache_timer, maxsize=cache_max_size, ttl=cache_expiration
        )

        if service_name != "Authentication Service":
            raise IOError(f"The service at {self._url} does not appear to be the KBase "
                          + "Authentication Service"
            )

        # could use the server time to adjust for clock skew, probably not worth the trouble

    async def get_user(self, token: str) -> KBaseUser:
        """
        Get a username from a token as well as the user's administration status.
        Verifies the user has all the required roles set in the create() method.
        
        token - The user's token.
        
        Returns the user.
        """
        # TODO CODE should check the token for \n etc.
        _not_falsy(token, "token")

        admin_cache = self._cache.get(token, default=False)
        if admin_cache:
            return KBaseUser(admin_cache[0], admin_cache[1]) 
        j = await _get(self._me_url, {"Authorization": token})
        croles = set(j["customroles"])
        if self._req_roles and not self._req_roles <= croles:
            raise MissingRoleError(
                "The user is missing a required authentication role to use the service."
            )
        v = (j["user"], self._get_admin_role(croles))
        self._cache.set(token, v)
        return KBaseUser(v[0], v[1])

    def _get_admin_role(self, roles: set[str]):
        if roles & self._full_roles:
            return AdminPermission.FULL
        return AdminPermission.NONE


class AuthenticationError(Exception):
    """ An error thrown from the authentication service. """


class InvalidTokenError(AuthenticationError):
    """ An error thrown when a token is invalid. """


class MissingRoleError(AuthenticationError):
    """ An error thrown when a user does not have the required roles to use the service """
