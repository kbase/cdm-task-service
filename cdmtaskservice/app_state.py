"""
Functions for creating and handling application state.

All functions assume that the application state has been appropriately initialized via
calling the build_app() method
"""

import asyncio
from typing import NamedTuple

from fastapi import FastAPI, Request
from cdmtaskservice.config import CDMTaskServiceConfig
from cdmtaskservice.kb_auth import KBaseAuth

# The main point of this module is to handle all the application state in one place
# to keep it consistent and allow for refactoring without breaking other code


class AppState(NamedTuple):
    """ Holds application state. """
    auth: KBaseAuth


async def build_app(
    app: FastAPI,
    cfg: CDMTaskServiceConfig,
) -> None:
    """
    Build the application state.

    app - the FastAPI app.
    cfg - the CDM task service config.
    """
    print("Connecting to KBase auth service... ", end="", flush=True)
    # TODO AUTH enforce kbase staff and Nersc account roles
    auth = await KBaseAuth.create(cfg.auth_url, full_admin_roles=cfg.auth_full_admin_roles)
    print("Done")
    app.state._cdmstate = AppState(auth)


def get_app_state(r: Request) -> AppState:
    """
    Get the application state from a request.
    """
    return _get_app_state_from_app(r.app)


async def destroy_app_state(app: FastAPI):
    """
    Destroy the application state, shutting down services and releasing resources.
    """
    appstate = _get_app_state_from_app(app)  # first to check state was set up
    # TODO APPSTATE handle shutdown routines, primarily mongo client
    appstate = appstate  # get rid of unused var warning for now
    # https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
    await asyncio.sleep(0.250)


def _get_app_state_from_app(app: FastAPI) -> AppState:
    if not app.state._cdmstate:
        raise ValueError("App state has not been initialized")
    return app.state._cdmstate
