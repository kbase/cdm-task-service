"""
CDM task service endpoints.
"""

from fastapi import APIRouter
from pydantic import BaseModel, Field
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice.version import VERSION
from cdmtaskservice.timestamp import timestamp

SERVICE_NAME = "CDM Task Service Prototype"

ROUTER_GENERAL = APIRouter(tags=["General"])


class Root(BaseModel):
    service_name: str = Field(example=SERVICE_NAME)
    version: str = Field(example=VERSION)
    git_hash: str = Field(example="b78f6e15e85381a7df71d6005d99e866f3f868dc")
    server_time: str = Field(example="2022-10-07T17:58:53.188698+00:00")


@ROUTER_GENERAL.get("/", response_model=Root)
async def root():
    return {
        "service_name": SERVICE_NAME,
        "version": VERSION,
        "git_hash": GIT_COMMIT,
        "server_time": timestamp()
    }
