"""
Classes for dealing with CTS users.
"""

from dataclasses import dataclass, field
from enum import Enum
from kbase.auth import AsyncKBaseAuthClient

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string
from cdmtaskservice.exceptions import UnauthorizedError


class CTSRole(str, Enum):
    """ A role for the service a user may possess. """
    
    EXTERNAL_EXECUTOR = "external_executor"
    
    FULL_ADMIN = "full_admin"
    # add roles as needed


@dataclass(frozen=True)
class CTSUser:
    """
    Represents a user of the CTS system.

    Attributes:
        user - the name of the user.
        roles - the set of roles assigned to the user.
        is_kbase_staff - whether the user is a KBase staff member.
        has_nersc_account - whether the user has a NERSC account.
    """
    user: str
    roles: frozenset[CTSRole] = field(default_factory=frozenset)
    is_kbase_staff: bool = False
    has_nersc_account: bool = False

    def __post_init__(self):
        # Convert roles to frozenset if it isn't one already
        if not isinstance(self.roles, frozenset):
            object.__setattr__(self, 'roles', frozenset(self.roles))
        # TODO CODE check args aren't None. Creating class here so YAGNI for now

    def is_full_admin(self):
        """ Returns true if the user is a service admin with full rights to everything. """
        return CTSRole.FULL_ADMIN in self.roles

    def is_external_executor(self):
        """ Returns true if the user is an external executor. """
        return CTSRole.EXTERNAL_EXECUTOR in self.roles


# Illegal user name in kbase and hopefully everywhere else
SERVICE_USER = CTSUser(user="**** SERVICE ****")


class CTSAuth:
    """ An authentication class for the CTS. """
    
    def __init__(
            self,
            kbaseauth: AsyncKBaseAuthClient,
            service_admin_roles: set[str],
            is_kbase_staff_role: str,
            has_nersc_account_role: str,
            external_executor_role: str,
    ):
        """
        Create the auth client.
        
        kbaseauth - a KBase authentication client.
        service_admin_roles - KBase auth roles that designates that a user is a service admin
            with full rights to everything.
        is_kbase_staff_role - a KBase auth role that designates that a user is a KBase staff
            member.
        has_nersc_account_role - a KBase auth role that designates that a user has a NERSC
            account.
        external_executor_roel -a KBase auth role that designates the user is a external
            job executor.
        """
        # In the future this mey need changes to support other auth sources...?
        self._kbauth = _not_falsy(kbaseauth, "kbaseauth")
        # TODO CODE check contents are non-whitespace only strings
        self._admin_roles = _not_falsy(service_admin_roles, "service_admin_roles")
        self._kbstaff_role = require_string(is_kbase_staff_role, "is_kbase_staff_role")
        self._nersc_role = require_string(has_nersc_account_role, "has_nersc_account_role")
        self._external_executor_role = require_string(
            external_executor_role, "external_executor_role"
        )


    async def is_valid_kbase_user(self, user: str, token: str) -> bool:
        """
        Check if a user name is valid in the KBase auth service.
        
        user - the user name to check.
        token - a token to provide to the auth service to allow accessing the lookup endpoint.
        
        Throws an exception if the user name is illegally formatted.
        """
        # passthrough method
        return (await self._kbauth.validate_usernames(token, user))[user]

    async def get_kbase_user(self, token: str) -> CTSUser:
        """ Get a CTS user given a KBase token. """
        # this will def need rethinking if we ever support more auth sources
        user = await self._kbauth.get_user(token)
        roles = set()
        has_roles = set(user.customroles)
        if has_roles & self._admin_roles:
            roles.add(CTSRole.FULL_ADMIN)
        if self._external_executor_role in has_roles:
            roles.add(CTSRole.EXTERNAL_EXECUTOR)
        ctsuser = CTSUser(
            user=user.user,
            roles=roles,
            is_kbase_staff=self._kbstaff_role in has_roles,
            has_nersc_account=self._nersc_role in has_roles,
        )
        # ensure admins have all roles necessary to use any part of the system
        if (
            ctsuser.is_full_admin()
            and (not ctsuser.is_kbase_staff or not ctsuser.has_nersc_account)
        ):
            raise UnauthorizedError("Service admins must be KBase staff and have NERSC accounts")
        return ctsuser
