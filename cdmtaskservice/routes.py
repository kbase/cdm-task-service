"""
CDM task service endpoints.
"""

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from cdmtaskservice import kb_auth
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice.http_bearer import KBaseHTTPBearer
from cdmtaskservice.version import VERSION
from cdmtaskservice.timestamp import timestamp

SERVICE_NAME = "CDM Task Service Prototype"

ROUTER_GENERAL = APIRouter(tags=["General"])

_AUTH = KBaseHTTPBearer()


class Root(BaseModel):
    """ General information about the service """
    service_name: str = Field(example=SERVICE_NAME, description="The name of the service.")
    version: str = Field(example=VERSION, description="The semantic version of the service.")
    git_hash: str = Field(
        example="b78f6e15e85381a7df71d6005d99e866f3f868dc",
        description="The git commit of the service code."
    )
    server_time: str = Field(
        example="2022-10-07T17:58:53.188698+00:00",
        description="The server's time as an ISO8601 string."
    )


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
    user: str = Field(example="kbasehelp", description="The users's username.")
    is_service_admin: bool = Field(
        example=False, description="Whether the user is a service administrator."
    )
    

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
