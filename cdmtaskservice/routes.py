"""
CDM task service endpoints.
"""

import datetime
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from typing import Annotated
from cdmtaskservice import app_state
from cdmtaskservice import kb_auth
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice.http_bearer import KBaseHTTPBearer
from cdmtaskservice.version import VERSION
from cdmtaskservice.timestamp import timestamp

SERVICE_NAME = "CDM Task Service Prototype"

ROUTER_GENERAL = APIRouter(tags=["General"])
ROUTER_ADMIN = APIRouter(tags=["Admin"], prefix="/admin")

_AUTH = KBaseHTTPBearer()


def _ensure_admin(user: kb_auth.KBaseUser, err_msg: str):
    if user.admin_perm != kb_auth.AdminPermission.FULL:
        raise UnauthorizedError(err_msg)


class Root(BaseModel):
    """ General information about the service """
    service_name: Annotated[
        str, Field(example=SERVICE_NAME, description="The name of the service."
    )]
    version: Annotated[
        str, Field(example=VERSION, description="The semantic version of the service."
    )]
    git_hash: Annotated[str, Field(
        example="b78f6e15e85381a7df71d6005d99e866f3f868dc",
        description="The git commit of the service code."
    )]
    server_time: Annotated[str, Field(
        example="2022-10-07T17:58:53.188698+00:00",
        description="The server's time as an ISO8601 string."
    )]


@ROUTER_GENERAL.get(
    "/",
    response_model=Root,
    summary="General service info",
    description="General information about the service.")
async def root():
    return {
        "service_name": SERVICE_NAME,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": timestamp()
    }


class WhoAmI(BaseModel):
    """ The username associated with the provided user token and the users's admin state. """
    user: Annotated[str, Field(example="kbasehelp", description="The users's username.")]
    is_service_admin: Annotated[bool, Field(
        example=False, description="Whether the user is a service administrator."
    )]
    

@ROUTER_GENERAL.get(
    "/whoami/",
    response_model=WhoAmI,
    summary="Who am I? What does it all mean?",
    description="Information about the current user."
)
async def whoami(user: kb_auth.KBaseUser=Depends(_AUTH)):
    return {
        "user": user.user,
        "is_service_admin": kb_auth.AdminPermission.FULL == user.admin_perm
    }


class NERSCClientInfo(BaseModel):
    """ Provides information about the NERSC SFAPI client. """
    id: Annotated[str, Field(description="The opaque ID of the client.")]
    expires_at: Annotated[datetime.datetime, Field(
        example="2024-10-24T22:35:40Z",
        description="The time the client expires in ISO8601 format."
    )]
    expires_in: Annotated[datetime.timedelta, Field(
        example="PT12H30M5S",
        description="The time duration until the client expires as an ISO8601 duration string."
    )]

@ROUTER_ADMIN.get(
    "/clients/nersc",
    response_model=NERSCClientInfo,
    summary="NERSC SFAPI client info",
    description="Get information about the NERSC Superfacility API client "
        + "including the expiration time,\n"
        + "Administrator credentials are required."
)
async def get_nersc_client_info(r: Request, user: kb_auth.KBaseUser=Depends(_AUTH)):
    # TODO CLIENTEXP add query param to throw error if expiration time is < param time in the future
    _ensure_admin(user, "Only service administrators may view NERSC client information.")
    nersc_cli = app_state.get_app_state(r).sfapi_client
    expires = nersc_cli.expiration()
    return NERSCClientInfo(
        id=nersc_cli.get_client_id(),
        expires_at=expires,
        expires_in=expires - datetime.datetime.now(datetime.timezone.utc)
    )


class UnauthorizedError(Exception):
    """ An error thrown when a user is not authorized to perform an action."""
