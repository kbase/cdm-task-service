"""
Functions for creating and handling application state.

All functions assume that the application state has been appropriately initialized via
calling the build_app() method
"""

import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
import os
from pathlib import Path
from typing import NamedTuple

from fastapi import FastAPI, Request
from cdmtaskservice import models
from cdmtaskservice.config import CDMTaskServiceConfig
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.image_remote_lookup import DockerImageInfo
from cdmtaskservice.images import Images
from cdmtaskservice.jaws.client import JAWSClient
from cdmtaskservice.jobflows.flowmanager import JobFlowManager
from cdmtaskservice.jobflows.nersc_jaws import NERSCJAWSRunner
from cdmtaskservice.job_state import JobState
from cdmtaskservice.kb_auth import KBaseAuth
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.nersc.client import NERSCSFAPIClientProvider
from cdmtaskservice.nersc.status import NERSCStatus
from cdmtaskservice.nersc.manager import NERSCManager
from cdmtaskservice.refdata import Refdata
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import validate_path
from cdmtaskservice.version import VERSION

# The main point of this module is to handle all the application state in one place
# to keep it consistent and allow for refactoring without breaking other code


class AppState(NamedTuple):
    """ Holds application state. """
    auth: KBaseAuth
    sfapi_client: NERSCSFAPIClientProvider
    s3_client: S3Client  # may be None if NERSC is unavailable at startup
    job_state: JobState
    refdata: Refdata
    images: Images
    jobflow_manager: JobFlowManager


async def build_app(
    app: FastAPI,
    cfg: CDMTaskServiceConfig,
) -> None:
    """
    Build the application state.

    app - the FastAPI app.
    cfg - the CDM task service config.
    """
    # This method is getting pretty long but it's stupid simple so...
    # May want to parallelize some of this for faster startups. would need to rework prints
    logr = logging.getLogger(__name__)
    # check that the path is a valid path
    logbuk = validate_path(cfg.container_s3_log_dir).split("/", 1)[0]
    flowman = JobFlowManager()
    coman = CoroutineWrangler()
    logr.info("Connecting to KBase auth service... ")
    auth = await KBaseAuth.create(
        cfg.auth_url,
        required_roles=[cfg.kbase_staff_role, cfg.has_nersc_account_role],
        full_admin_roles=cfg.auth_full_admin_roles
    )
    logr.info("Done")
    jaws_client = None
    sfapi_client = None
    mongocli = None
    try:
        sfapi_client, jaws_client, nerscman, failreason = await _build_NERSC_flow_deps(logr, cfg)
        logr.info("Initializing S3 client... ")
        s3 = await S3Client.create(
            cfg.s3_url, cfg.s3_access_key, cfg.s3_access_secret, insecure_ssl=cfg.s3_allow_insecure
        )
        await s3.is_bucket_writeable(logbuk)
        s3_external = await S3Client.create(
            cfg.s3_external_url,
            cfg.s3_access_key,
            cfg.s3_access_secret,
            insecure_ssl=cfg.s3_allow_insecure,
            skip_connection_check=not cfg.s3_verify_external_url
        )
        logr.info("Done")
        logr.info("Initializing MongoDB client...")
        mongocli = await get_mongo_client(cfg)
        logr.info("Done")
        mongodao = await MongoDAO.create(mongocli[cfg.mongo_db])
        if failreason:
            flowman.mark_flow_inactive(models.Cluster.PERLMUTTER_JAWS, failreason)
        else:
            nerscjawsflow = NERSCJAWSRunner(  # this has a lot of required args, yech
                models.Cluster.PERLMUTTER_JAWS,
                nerscman,
                jaws_client,
                mongodao,
                s3,
                s3_external,
                cfg.container_s3_log_dir,
                coman,
                cfg.service_root_url,
                s3_insecure_ssl=cfg.s3_allow_insecure,
            )
            flowman.register_flow(models.Cluster.PERLMUTTER_JAWS, nerscjawsflow)
        imginfo = await DockerImageInfo.create(Path(cfg.crane_path).expanduser().absolute())
        images = Images(mongodao, imginfo)
        job_state = JobState(mongodao, s3, images, coman, flowman, logbuk, cfg.job_max_cpu_hours)
        refdata = Refdata(mongodao, s3, coman, flowman)
        app.state._mongo = mongocli
        app.state._coroman = coman
        app.state._jaws_cli = jaws_client
        app.state._cdmstate = AppState(auth, sfapi_client, s3, job_state, refdata, images, flowman)
    except:
        if mongocli:
            mongocli.close()
        if jaws_client:
            await jaws_client.close()
        if sfapi_client:
            await sfapi_client.destroy()
        raise


async def _build_NERSC_flow_deps(
    logr: logging.Logger, cfg: CDMTaskServiceConfig
) -> tuple[NERSCSFAPIClientProvider, NERSCManager, JAWSClient, str]:
    # this is not helpful other than for testing, so it's undocumented and 
    # will need changes later. Managing a server running with jobflows turning on and off needs
    # more thought.
    start_wo_nersc = os.environ.get("KBCTS_START_WITHOUT_NERSC") == "true"
    sfapi_client = None
    jaws_client = None
    try:
        nscli = NERSCStatus()
        ns = await nscli.status()
        await nscli.close()
        if not ns.ok:
            desc = ns.perlmutter_description if ns.perlmutter_description else ns.dtns_description
            if start_wo_nersc:
                logr.info(f"NERSC is down, starting without job flow:\n{ns}")
                return None, None, None, desc
            else:
                raise ValueError(f"NERSC is down: {desc}")
        logr.info("Initializing NERSC SFAPI client... ")
        sfapi_client = await NERSCSFAPIClientProvider.create(
            Path(cfg.sfapi_cred_path), cfg.nersc_jaws_user
        )
        logr.info("Done")
        logr.info("Setting up NERSC manager and installing code at NERSC...")
        # TODO MULTICLUSTER service won't start if perlmutter is down, need to make it more dynamic
        nerscman = await NERSCManager.create(
            sfapi_client.get_client,
            Path(cfg.nersc_remote_code_dir) / VERSION,
            cfg.jaws_token,
            cfg.jaws_group,
            Path(cfg.jaws_refdata_root_dir),
            service_group=cfg.service_group,
        )
        logr.info("Done")
        logr.info("Initializing JAWS Central client... ")
        jaws_client = await JAWSClient.create(cfg.jaws_url, cfg.jaws_token, cfg.nersc_jaws_user)
        logr.info("Done")
        return sfapi_client, jaws_client, nerscman, None
    except Exception:
        # May want to be smarter about this and return the sfapi client if it's available
        # so the expiration time for the creds can be checked.
        # Since this would only happen if the service is running for tests don't worry about it
        # for now. Currently routes.py will throw an error if sfapi_client is null 
        if jaws_client:
            await jaws_client.close()
        if sfapi_client:
            await sfapi_client.destroy()
        if start_wo_nersc:
            logr.exception(
                "Failed to create NERSC job flow dependencies. Starting without job flow."
            )
            return None, None, None, "NERSC or JAWS is unavailable"
        else:
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
    app.state._coroman.destroy()
    if appstate.sfapi_client:
        await appstate.sfapi_client.destroy()
    if app.state._jaws_cli:
        await app.state._jaws_cli.close()
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
        tz_aware=True,
    )
    # Test connnection cheaply, doesn't need auth.
    # Just throw the exception as is
    try:
        await client.admin.command("ismaster")
        return client
    except:
        client.close()
        raise
