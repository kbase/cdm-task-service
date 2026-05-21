"""
Classes for dealing with CTS users.
"""

from dataclasses import dataclass, field
from enum import Enum
from kbase.auth import AsyncKBaseAuthClient

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.exceptions import UnauthorizedError


class CTSRole(str, Enum):
    """ A role for the service a user may possess. """

    FULL_ADMIN = "full_admin"
    CTS_USER = "cts_user"
    # add roles as needed


@dataclass(frozen=True)
class CTSUser:
    """
    Represents a user of the CTS system.

    Attributes:
        user - the name of the user.
        roles - the set of roles assigned to the user.
        has_nersc_account - whether the user has a NERSC account.
        is_external_excutor - whether ther user is an external executor running a CTS job.
        is_cdm_task_service - whether the user is the CDM task service.
            Used by the refdata service.
        is_refdata_service - whether the user is the refdata service.
    """
    user: str
    # TODO ROLES Need to rethink how roles are presented here - bools vs CTSRoles
    # The difference is that roles are visible in the whoami endpoint while bools are not.
    # Roles should allow the token to perform privileged tasks (like admin)
    # but I'm not sure if we should expose roles that no regular user should have (like
    # executor) in the api docs
    # Maybe a special endpoint for executor / cts / refserver roles?
    # Should the NERSC role be exposed? It's a basic requirement for running NERSC jobs
    # and so isn't really useful to expose
    # Maybe make CTSRoles for operations and add those operations as appropriate
    # for each service / executor
    roles: frozenset[CTSRole] = field(default_factory=frozenset)
    has_nersc_account: bool = False
    is_cdm_task_service: bool = False
    is_refdata_service: bool = False
    is_external_executor: bool = False

    def __post_init__(self):
        # Convert roles to frozenset if it isn't one already
        if not isinstance(self.roles, frozenset):
            object.__setattr__(self, 'roles', frozenset(self.roles))
        # TODO CODE check args aren't None. Creating class here so YAGNI for now

    def is_full_admin(self):
        """ Returns true if the user is a service admin with full rights to everything. """
        return CTSRole.FULL_ADMIN in self.roles

    def is_cts_user(self):
        """ Returns true if the user is authorized to use the CTS. """
        return CTSRole.CTS_USER in self.roles


# Illegal user name in kbase and hopefully everywhere else
SERVICE_USER = CTSUser(user="**** SERVICE ****")


class CTSAuth:
    """ An authentication class for the CTS. """
    
    def __init__(
            self,
            kbaseauth: AsyncKBaseAuthClient,
            service_admin_roles: set[str],
            *,
            cts_user_roles: set[str] | None = None,
            has_nersc_account_role: str | None = None,
            external_executor_role: str | None = None,
            cts_role: str | None = None,
            refdata_service_role: str | None = None,
            require_cts_user_and_nersc_accounts_for_admin: bool = True,
    ):
        """
        Create the auth client.

        kbaseauth - a KBase authentication client.
        service_admin_roles - KBase auth roles that designates that a user is a service admin
            with full rights to everything.
        cts_user_roles - KBase auth roles that designate that a user is authorized to use the
            CTS. Any user possessing at least one of these roles receives CTSRole.CTS_USER.
        has_nersc_account_role - a KBase auth role that designates that a user has a NERSC
            account.
        external_executor_role - a KBase auth role that designates the user is an external
            job executor.
        cts_role - a KBase auth role that designates that the user represents the CDM Task
            Service.
        refdata_service_role - a KBase auth role that designates the user is the refdata
            service.
        require_cts_user_and_nersc_accounts_for_admin - if false, the CTS user role and NERSC
            account role are not required for full admin status. This is typically only used
            for the reference data service.
        """
        # In the future this mey need changes to support other auth sources...?
        self._kbauth = _not_falsy(kbaseauth, "kbaseauth")
        # TODO CODE check contents are non-whitespace only strings
        self._admin_roles = service_admin_roles or set()
        self._cts_user_roles = cts_user_roles or set()
        self._nersc_role = has_nersc_account_role
        self._external_executor_role = external_executor_role
        self._cts_role = cts_role
        self._refserv_role = refdata_service_role
        self._require_admin_roles = require_cts_user_and_nersc_accounts_for_admin


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
        if has_roles & self._cts_user_roles:
            roles.add(CTSRole.CTS_USER)
        ctsuser = CTSUser(
            user=user.user,
            roles=roles,
            has_nersc_account=self._nersc_role in has_roles,
            is_external_executor=self._external_executor_role in has_roles,
            is_cdm_task_service=self._cts_role in has_roles,
            is_refdata_service=self._refserv_role in has_roles,
        )
        # ensure admins have all roles necessary to use any part of the system
        if (
            ctsuser.is_full_admin()
            and self._require_admin_roles
            and (not ctsuser.is_cts_user() or not ctsuser.has_nersc_account)
        ):
            raise UnauthorizedError("Service admins must be CTS users and have NERSC accounts")
        return ctsuser
