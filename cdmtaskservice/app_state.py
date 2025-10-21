"""
Functions for creating and handling application state.

All functions assume that the application state has been appropriately initialized via
calling the build_app() method
"""

import asyncio
import datetime
from fastapi import FastAPI, Request
from kbase.auth import AsyncKBaseAuthClient
import logging
from motor.motor_asyncio import AsyncIOMotorClient
import os
from pathlib import Path
from typing import NamedTuple, Callable

from cdmtaskservice.config_s3 import S3Config
from cdmtaskservice.config import CDMTaskServiceConfig
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import UnavailableResourceError
from cdmtaskservice.image_remote_lookup import DockerImageInfo
from cdmtaskservice.images import Images
from cdmtaskservice.jobflows.flowmanager import JobFlowManager
from cdmtaskservice.jobflows.jaws_flows_provider import JAWSFlowProvider
from cdmtaskservice.jobflows.kbase import KBaseFlowProvider, KBaseRunner
from cdmtaskservice.jobflows.lawrencium_jaws import LawrenciumJAWSRunner
from cdmtaskservice.jobflows.nersc_jaws import NERSCJAWSRunner
from cdmtaskservice.job_state import JobState
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.nersc.client import NERSCSFAPIClientProvider
from cdmtaskservice.notifications.kafka_checker import KafkaChecker
from cdmtaskservice.refdata import Refdata
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice import sites
from cdmtaskservice.timestamp import utcdatetime
from cdmtaskservice.user import CTSAuth, CTSUser

# The main point of this module is to handle all the application state in one place
# to keep it consistent and allow for refactoring without breaking other code


class AppState(NamedTuple):
    """ Holds application state. """
    auth: CTSAuth
    """ The authentication client for the service. """
    sfapi_client_provider: Callable[[], NERSCSFAPIClientProvider]
    """ A callable that returns a provider for a NERSC SFAPI client. """
    job_state: JobState
    """ The job state manager class. """
    refdata: Refdata
    """ The refdata manager class. """
    images: Images
    """ The Docker images manager class. """
    jobflow_manager: JobFlowManager
    """ The job flow manager class. """
    kafka_checker: KafkaChecker
    """ The Kafka state checker class. """
    allowed_paths: list[str]
    """
    What paths are allowed for reading and writing files in S3.
    If empty, the user can read and write to anywhere the service can read and write,
    other than where job logs are stored.
    """
    condor_exe_path: Path
    """ The local path to the executable file for use when running jobs with HTCondor. """ 
    code_archive_path: Path
    """ The local path to the *.tgz archive file for use with jobs in an external runner. """


class RequestState(NamedTuple):
    """ Holds request specific state. """
    user: CTSUser | None
    token: str | None


async def _check_paths(s3: S3Client, logr: logging.Logger, cfg: CDMTaskServiceConfig):
    # this is ugly, but since it only happens at startup with admin supplied data just leave it
    # for now
    logr.info("Checking allowed and log path writeability")
    paths = [cfg.container_s3_log_dir]
    buckets = []
    if cfg.allowed_s3_paths:
        for p in cfg.allowed_s3_paths:
            if "/" in p[:-1]:  # always end with /
                paths.append(p)
            else:
                buckets.append(p[:-1])
    s3paths = S3Paths(paths, no_index_in_errors=True)
    async with asyncio.TaskGroup() as tg:
        # We'll assume there aren't too many buckets/paths here
        tg.create_task(s3.is_paths_writeable(s3paths))
        for b in buckets:
            tg.create_task(s3.is_bucket_writeable(b))
    logr.info("Done")


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
    # But NERSC startup takes 95% of the time, so YAGNI
    logr = logging.getLogger(__name__)
    # currently test mode means that job flow readiness checks are ignored and the job is
    # created in the DB but is never submitted. Only useful for testing purposes
    test_mode = os.environ.get("KBCTS_TEST_MODE") == "true"
    if test_mode:
        logr.info("KBCTS_TEST_MODE env var is 'true', will not submit jobs for processing")
    # check that the path is a valid path
    coman = CoroutineWrangler()
    logr.info("Connecting to KBase auth service... ")
    kbauth = await AsyncKBaseAuthClient.create(cfg.auth_url)
    auth = CTSAuth(
        kbauth,
        set(cfg.auth_full_admin_roles),
        cfg.kbase_staff_role,
        cfg.has_nersc_account_role,
        cfg.external_executor_role,
    )
    logr.info("Done")
    jaws_job_flows = None
    mongocli = None
    kafka_notifier = None
    try:
        logr.info("Initializing S3 client... ")
        s3cfg = cfg.get_s3_config()
        # ensure clients are working before we proceed
        await s3cfg.initialize_clients()
        logr.info("Done")
        await _check_paths(s3cfg.get_internal_client(), logr, cfg)
        logr.info("Initializing MongoDB client...")
        mongocli = await get_mongo_client(cfg)
        logr.info("Done")
        mongodao = await MongoDAO.create(mongocli[cfg.mongo_db])
        await mongodao.initialize_sites(list(sites.Cluster))
        logr.info("Initializing Kafka client...")
        kafka_notifier = await KafkaNotifier.create(
            cfg.kafka_boostrap_servers, cfg.kafka_topic_jobs
        )
        logr.info("Done")
        flowman = JobFlowManager(mongodao)
        jaws_job_flows = await _register_nersc_job_flows(
            logr, cfg, flowman, mongodao, s3cfg, kafka_notifier, coman
        )
        _register_kbase_job_flow(cfg, flowman, mongodao, s3cfg, kafka_notifier, coman)
        imginfo = await DockerImageInfo.create(Path(cfg.crane_path).expanduser().absolute())
        refdata = Refdata(mongodao, s3cfg.get_internal_client(), coman, flowman)
        images = Images(mongodao, imginfo, refdata)
        job_state = JobState(  # this also has a lot of required args, yech
            mongodao,
            s3cfg.get_internal_client(),
            images,
            kafka_notifier,
            refdata,
            coman,
            flowman,
            cfg.allowed_s3_paths,
            cfg.container_s3_log_dir,
            cfg.job_max_cpu_hours,
            test_mode=test_mode,
        )
        app.state._mongo = mongocli
        app.state._coroman = coman
        app.state._jaws_provider = jaws_job_flows
        app.state._kafka = kafka_notifier
        app.state._kbauth = kbauth
        kc = KafkaChecker(mongodao, kafka_notifier)
        sfcliprov = _get_sfapi_client_provider(jaws_job_flows)
        app.state._cdmstate = AppState(
            auth,
            sfcliprov,
            job_state,
            refdata,
            images,
            flowman,
            kc,
            cfg.allowed_s3_paths,
            _get_local_path(cfg.condor_exe_path),
            _get_local_path(cfg.code_archive_path),
        )
        await _check_unsent_kafka_messages(logr, cfg, kc)
    except:
        await kbauth.close()
        if mongocli:
            mongocli.close()
        if jaws_job_flows:
            await jaws_job_flows.close()
        if kafka_notifier:
            # TODO KAFKA see https://github.com/aio-libs/aiokafka/issues/1101
            await asyncio.wait_for(kafka_notifier.close(), 10)
        raise


def _get_local_path(path: str) -> Path:
    p = Path(path)
    if not p.is_file():
        raise ValueError(f"Path {path} does not exist or is not a file")
    return p


def _get_sfapi_client_provider(jaws_flows: JAWSFlowProvider
    ) -> Callable[[], NERSCSFAPIClientProvider]:
    if jaws_flows:
        return jaws_flows.get_sfapi_client
    def _get_client_fail():
        raise UnavailableResourceError("The service was started without NERSC job flows")
    return _get_client_fail


async def _check_unsent_kafka_messages(
    logr: logging.Logger, cfg: CDMTaskServiceConfig, kc: KafkaChecker
):
    if cfg.kafka_startup_unsent_delay_min:
        older_than = utcdatetime() - datetime.timedelta(minutes=cfg.kafka_startup_unsent_delay_min)
        logr.info(f"Checking for unsent kafka messages older than {older_than.isoformat()}...")
        jobs, notifs = await kc.check(older_than)
        logr.info(f"Sending {notifs} kafka updates for {jobs} jobs")


async def _register_nersc_job_flows(
    logr: logging.Logger,
    cfg: CDMTaskServiceConfig,
    flowman: JobFlowManager,
    mongodao: MongoDAO,
    s3config: S3Config,
    kafka_notifier: KafkaNotifier,
    coman: CoroutineWrangler
) -> JAWSFlowProvider:
    # This is only useful for testing with other processes that just want to pull job records
    # but not start or run jobs or only run KBase jobs. As such it's undocumented.
    if os.environ.get("KBCTS_SKIP_NERSC") == "true":
        logr.info("KBCTS_SKIP_NERSC env var is 'true', skipping NERSC and JAWS startup")
        return None

    jaws_job_flows = await JAWSFlowProvider.create(
        Path(cfg.sfapi_cred_path),
        cfg.get_nersc_paths(),
        cfg.get_jaws_config(),
        mongodao,
        s3config,
        kafka_notifier,
        coman,
        cfg.service_group,
        cfg.service_root_url
    )
    flowman.register_flow(NERSCJAWSRunner.CLUSTER, jaws_job_flows.get_nersc_job_flow)
    flowman.register_flow(LawrenciumJAWSRunner.CLUSTER, jaws_job_flows.get_lrc_job_flow)
    return jaws_job_flows


def _register_kbase_job_flow(
    cfg: CDMTaskServiceConfig,
    flowman: JobFlowManager,
    mongodao: MongoDAO,
    s3config: S3Config,
    kafka_notifier: KafkaNotifier,
    coman: CoroutineWrangler
):
    kbase_provider = KBaseFlowProvider.create(
        cfg.get_condor_client_config(),
        mongodao,
        s3config,
        kafka_notifier,
        coman,
    )
    flowman.register_flow(KBaseRunner.CLUSTER, kbase_provider.get_kbase_job_flow)


def get_app_state(r: Request) -> AppState:
    """
    Get the application state from a request.
    """
    return _get_app_state_from_app(r.app)


async def destroy_app_state(app: FastAPI):
    """
    Destroy the application state, shutting down services and releasing resources.
    """
    await app.state._kbauth.close()
    app.state._mongo.close()
    app.state._coroman.destroy()
    if app.state._jaws_provider:
        await app.state._jaws_provider.close()
    if app.state._kafka:
        # TODO KAFKA see https://github.com/aio-libs/aiokafka/issues/1101
        await asyncio.wait_for(app.state._kafka.close(), 10)
    # https://docs.aiohttp.org/en/stable/client_advanced.html#graceful-shutdown
    await asyncio.sleep(0.250)


def _get_app_state_from_app(app: FastAPI) -> AppState:
    if not app.state._cdmstate:
        raise ValueError("App state has not been initialized")
    return app.state._cdmstate


def set_request_user(r: Request, user: CTSUser | None, token: str | None):
    """ Set the user for the current request. """
    # if we add more stuff in the request state we'll need to not blow away the old state
    r.state._cdmstate = RequestState(user=user, token=token)


def _get_request_state(r: Request, field: str) -> RequestState:
    if not getattr(r.state, "_cdmstate", None) or not r.state._cdmstate:
        return None
    return getattr(r.state._cdmstate, field)


def get_request_user(r: Request) -> CTSUser:
    """ Get the user for a request. """
    return _get_request_state(r, "user")


def get_request_token(r: Request) -> str:
    """ Get the token for a request. """
    return _get_request_state(r, "token")


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
