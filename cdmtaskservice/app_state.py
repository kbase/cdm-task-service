"""
Functions for creating and handling application state.

All functions assume that the application state has been appropriately initialized via
calling the build_app() method
"""

import asyncio
from pathlib import Path
from typing import NamedTuple

from fastapi import FastAPI, Request
from cdmtaskservice.config import CDMTaskServiceConfig
from cdmtaskservice.kb_auth import KBaseAuth
from cdmtaskservice.nersc.client import NERSCSFAPIClientProvider

# The main point of this module is to handle all the application state in one place
# to keep it consistent and allow for refactoring without breaking other code


class AppState(NamedTuple):
    """ Holds application state. """
    auth: KBaseAuth
    sfapi_client: NERSCSFAPIClientProvider


async def build_app(
    app: FastAPI,
    cfg: CDMTaskServiceConfig,
) -> None:
    """
    Build the application state.

    app - the FastAPI app.
    cfg - the CDM task service config.
    """
    # May want to parallelize some of this for faster startups. would need to rework prints
    print("Connecting to KBase auth service... ", end="", flush=True)
    auth = await KBaseAuth.create(
        cfg.auth_url,
        required_roles=[cfg.kbase_staff_role, cfg.has_nersc_account_role],
        full_admin_roles=cfg.auth_full_admin_roles
    )
    print("Done")
    print("Initializing NERSC SFAPI client... ", end="", flush=True)
    nersc = await NERSCSFAPIClientProvider.create(Path(cfg.sfapi_cred_path), cfg.sfapi_user)
    print("Done")
    app.state._cdmstate = AppState(auth, nersc)


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
