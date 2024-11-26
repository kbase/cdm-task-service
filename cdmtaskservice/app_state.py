"""
Functions for creating and handling application state.

All functions assume that the application state has been appropriately initialized via
calling the build_app() method
"""

import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from pathlib import Path
from typing import NamedTuple

from fastapi import FastAPI, Request
from cdmtaskservice.config import CDMTaskServiceConfig
from cdmtaskservice.image_remote_lookup import DockerImageInfo
from cdmtaskservice.images import Images
from cdmtaskservice.job_state import JobState
from cdmtaskservice.kb_auth import KBaseAuth
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.nersc.client import NERSCSFAPIClientProvider
from cdmtaskservice.s3.client import S3Client

# The main point of this module is to handle all the application state in one place
# to keep it consistent and allow for refactoring without breaking other code


class AppState(NamedTuple):
    """ Holds application state. """
    auth: KBaseAuth
    sfapi_client: NERSCSFAPIClientProvider
    s3_client: S3Client
    job_state: JobState
    images: Images


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
    logr = logging.getLogger(__name__)
    logr.info("Connecting to KBase auth service... ")
    auth = await KBaseAuth.create(
        cfg.auth_url,
        required_roles=[cfg.kbase_staff_role, cfg.has_nersc_account_role],
        full_admin_roles=cfg.auth_full_admin_roles
    )
    logr.info("Done")
    logr.info("Initializing NERSC SFAPI client... ")
    nersc = await NERSCSFAPIClientProvider.create(Path(cfg.sfapi_cred_path), cfg.sfapi_user)
    logr.info("Done")
    logr.info("Initializing S3 client... ")
    s3 = await S3Client.create(
        cfg.s3_url, cfg.s3_access_key, cfg.s3_access_secret, insecure_ssl=cfg.s3_allow_insecure
    )
    logr.info("Done")
    logr.info("Initializing MongoDB client...")
    mongocli = await get_mongo_client(cfg)
    logr.info("Done")
    try:
        mongodao = await MongoDAO.create(mongocli[cfg.mongo_db])
        job_state = JobState(mongodao, s3)
        imginfo = await DockerImageInfo.create(Path(cfg.crane_path).expanduser().absolute())
        images = Images(mongodao, imginfo)
        app.state._mongo = mongocli
        app.state._cdmstate = AppState(auth, nersc, s3, job_state, images)
    except:
        mongocli.close()
        raise


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
    app.state._mongo.close()
    await appstate.sfapi_client.destroy()
    # https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
    await asyncio.sleep(0.250)


def _get_app_state_from_app(app: FastAPI) -> AppState:
    if not app.state._cdmstate:
        raise ValueError("App state has not been initialized")
    return app.state._cdmstate


async def get_mongo_client(cfg: CDMTaskServiceConfig) -> AsyncIOMotorClient:
    client = AsyncIOMotorClient(
        cfg.mongo_host,
        # Note auth is only currently tested manually
        authSource=cfg.mongo_db,
        username=cfg.mongo_user,
        password=cfg.mongo_pwd,
        retryWrites=cfg.mongo_retrywrites,
    )
    # Test connnection cheaply, doesn't need auth.
    # Just throw the exception as is
    try:
        await client.admin.command("ismaster")
        return client
    except:
        client.close()
        raise
