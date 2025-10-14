"""
Manages initialization of JAWS based job flows 
"""
import asyncio
from async_lru import alru_cache
from dataclasses import dataclass
import logging
from pathlib import Path
import time

from cdmtaskservice.arg_checkers import require_string as _require_string, not_falsy as _not_falsy
from cdmtaskservice.config_s3 import S3Config
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import UnavailableResourceError
from cdmtaskservice.jaws.client import JAWSClient
from cdmtaskservice.jaws.config import JAWSConfig
from cdmtaskservice.jaws.sitemapper import get_jaws_site
from cdmtaskservice.jobflows.flowmanager import JobFlowOrError
from cdmtaskservice.jobflows.lawrencium_jaws import LawrenciumJAWSRunner
from cdmtaskservice.jobflows.nersc_jaws import NERSCJAWSRunner
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.nersc.client import NERSCSFAPIClientProvider
from cdmtaskservice.nersc.manager import NERSCManager
from cdmtaskservice.nersc.paths import NERSCPaths
from cdmtaskservice.nersc.status import NERSCStatus
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice import sites


# This is going to be a nightmare to write tests for


_RETRY_DELAY_SEC = 5 * 60  # cnfigurable?


@dataclass(frozen=True)
class _Dependencies:
    jaws_client: JAWSClient | None = None
    lrc_flow: LawrenciumJAWSRunner | None = None
    nersc_flow: NERSCJAWSRunner | None = None


@dataclass(frozen=True)
class _BuildResult(_Dependencies):
    sfapi_client: NERSCSFAPIClientProvider | None = None
    error: str | None = None
    
    def _to_deps(self):
        return _Dependencies(
            jaws_client=self.jaws_client,
            lrc_flow=self.lrc_flow,
            nersc_flow=self.nersc_flow,
        )


class JAWSFlowProvider:
    """
    Class for managing the Perlmutter and LRC job flows' initialization.
    """
    
    # This all seems a bit overly complex, but it works. Maybe looking at it later with fresh
    # eyes will reveal simplifications
    
    # If there are upgrades to JAWs and this service in the future that make the LRC site
    # no longer dependent on NERSC this will need a lot of refactoring
    
    @classmethod
    async def create(  # lot of arguments, but I'm not seeing a way to simplify that makes sense
        cls,
        superfacility_api_credential_path: Path,
        nersc_paths: NERSCPaths,
        jaws_config: JAWSConfig,
        mongodao: MongoDAO,
        s3config: S3Config,
        kafka_notifier: KafkaNotifier,
        coman: CoroutineWrangler,
        service_group: str,
        service_root_url: str,
    ):
        """
        WARNING: this class is not thread safe.
        
        Create the JAWS based job flows provider.
        
        superfacility_api_credential_path - a path to an SFAPI credential file.
            The first line of the file must be the client ID, and the rest the client
            secret in PEM format.
        nersc_paths - the set of paths for NERSC manager use.
        jaws_config - configuration for communicating with the JAWS job runner at NERSC. The JAWS
            username is expected to be a NERSC user and the same user as for the SFAPI creds.
            It is typically a collaboration account.
        mongodao - the Mongo DAO.
        s3config - the S3 configuration.
        kafka_notifier - a kafka notifier.
        coman - a coroutine manager.
        service_group - The service group to which this instance of the flow provider belongs.
            This is used to separate files at NERSC so files from different S3 instances
            (say production and development) don't collide.
        service_root_url - the URL of the service root, used for constructing service callbacks.
        """
        jfp = cls()
        # set up input variables
        jfp._sfapi_cred_path = _not_falsy(
            superfacility_api_credential_path, "superfacility_api_credential_path"
        )
        jfp._nersc_paths = _not_falsy(nersc_paths, "nersc_paths")
        jfp._jaws_config = _not_falsy(jaws_config, "jaws_config")
        jfp._mongodao = _not_falsy(mongodao, "mongodao")
        jfp._s3config = _not_falsy(s3config, "s3config")
        jfp._kafka = _not_falsy(kafka_notifier, "kafka_notifier")
        jfp._coman = _not_falsy(coman, "coman")
        jfp._service_group = _require_string(service_group, "service_group")
        jfp._service_root_url = _require_string(service_root_url, "service_root_url")
        
        # setup other variables
        jfp._logr = logging.getLogger(__name__)
        jfp._nersc_status_cli = NERSCStatus()
        jfp._closed = False
        
        # setup build variables
        jfp._sfapi_client = None
        jfp._build_state = None
        jfp._last_fail_time = 0
        jfp._last_fail_error = "This indicates a programming error, this should have been set"
        
        # attempt to build
        br = await jfp._build_deps()
        jfp._handle_build_result(br)
        return jfp
    
    def __init__(self):
        """
        Don't call this method. Ever. Ancient Assyrians from 973BC cursed it, and you know you
        don't want to mess with those dudes
        """
    
    async def close(self):
        """
        Close all resources managed by this class.
        """
        self._closed = True
        await self._nersc_status_cli.close()
        if self._sfapi_client:
            await self._flow_builds.sfapi_client.destroy()
        if isinstance(self._build_state, _Dependencies):
            await self._build_state.jaws_client.close()

    def _check_closed(self):
        if self._closed:
            raise ValueError("Provider is closed")

    def get_sfapi_client(self) -> NERSCSFAPIClientProvider:
        """ Get the superfacility client provider. """
        self._check_closed()
        # we check the client first, since the build may have failed but the client could still
        # be present since partial builds are possible
        if self._sfapi_client:
            return self._sfapi_client
        # now force a build check
        err = self._check_build()
        if err:
            raise UnavailableResourceError(err)
        return self._sfapi_client

    @alru_cache(maxsize=10, ttl=30)
    async def _is_site_available(self, site: sites.Cluster) -> bool:
        site = get_jaws_site(site)
        return await self._build_state.jaws_client.is_site_up(site)

    async def get_nersc_job_flow(self) -> JobFlowOrError:
        """
        Get the NERSC / Perlmutter job flow manager.
        """
        # These names may need changing if we start supporting the Doubna NERSC system 
        self._check_closed()
        err = self._check_build()
        if err:
            return JobFlowOrError(error=err)
        if not await self._is_site_available(NERSCJAWSRunner.CLUSTER):
            return JobFlowOrError(error="JAWS is not available")
        return JobFlowOrError(jobflow=self._build_state.nersc_flow)
    
    async def get_lrc_job_flow(self) -> JobFlowOrError:
        """
        Get the Lawrencium job flow manager.
        """
        self._check_closed()
        err = self._check_build()
        if err:
            return JobFlowOrError(error=err)
        if not await self._is_site_available(LawrenciumJAWSRunner.CLUSTER):
            return JobFlowOrError(error="JAWS is not available")
        return JobFlowOrError(jobflow=self._build_state.lrc_flow)

    # TODO NERSCSTART could maybe make this automatic vs lazy in the future. Would need to be
    #                 careful around concurrency. Maybe start a thread and only that thread
    #                 runs this method.
    def _check_build(self) -> str:
        """
        NOTE: this method is not async purposefully. Since this class is expected to run in
        a single thread, any operations in this method, given that it's synchronous, cannot be
        interleaved by another coroutine. That means that
        
        * this method needs to run fast
        * We can use a variable as a flag to set the state of build and have that be coroutine
          safe (but not thread safe).
          
        Returns an error string or None if the build is complete.
        """
        state = self._build_state
        if not state:
            remaining_delay = _RETRY_DELAY_SEC - (time.monotonic() - self._last_fail_time)
            if remaining_delay > 0:
                # TODO NERSCSTART try using the restoration time from the NERSC status client
                return (
                    f"NERSC startup failed. Further attempts blocked for {remaining_delay}s. "
                    + f"Last error: {self._last_fail_error}"
                )
            fut = asyncio.get_event_loop().create_future()
            self._build_state = fut
            async def init():
                res = await self._build_deps()
                fut.set_result(res)
            asyncio.create_task(init())
            return "Recovery for NERSC based job flows in process"
        elif isinstance(state, asyncio.Future):
            if state.done():
                err = self._handle_build_result(state.result())
                if err:
                    return (
                        f"NERSC startup failed. Further attempts blocked for {_RETRY_DELAY_SEC}s. "
                        + f"Last error: {self._last_fail_error}"
                    )
            else:
                return "Recovery for NERSC based job flows in process"
        return None
    
    async def _build_deps(self):
        """
        This method should only run as part of _check_build or the constructor to make sure
        it's never run concurrently.
        """
        sfapi_client = None
        jaws_client = None
        try:
            if self._sfapi_client:
                sfapi_client = self._sfapi_client
            else:
                self._logr.info("Initializing NERSC SFAPI client...")
                sfapi_client = await NERSCSFAPIClientProvider.create(
                    self._sfapi_cred_path, self._jaws_config.user
                )
                self._logr.info("Done")
            self._logr.info("Getting NERSC status...")
            ns = await self._nersc_status_cli.status()
            self._logr.info("Done")
            if not ns.ok:
                desc = (
                    ns.perlmutter_description if ns.perlmutter_description
                    else ns.dtns_description
                )
                self._logr.error(f"NERSC is not available: {desc}")
                return _BuildResult(
                    error=f"NERSC is not available: {desc}",
                    sfapi_client=sfapi_client
                )
            self._logr.info("Setting up NERSC manager and installing code at NERSC...")
            # TODO INDEPENDENTLRC if it's ever ok to run jobs on LRC w/o nersc will need changes
            nerscman = await NERSCManager.create(
                sfapi_client.get_client,
                self._nersc_paths,
                self._jaws_config,
                service_group=self._service_group,
            )
            self._logr.info("Done")
            self._logr.info("Initializing JAWS Central client... ")
            jaws_client = await JAWSClient.create(self._jaws_config)
            self._logr.info("Done")
        except Exception as e:
            if jaws_client:
                await jaws_client.close()
            # may want to record this and add a way for admins to get it via the API vs. just logs
            self._logr.exception(f"Failed to initialize NERSC dependencies: {e}")
            return _BuildResult(
                error="NERSC manager startup failed",
                sfapi_client=sfapi_client,
            )
        nerscflow, lrcflow = self._build_flows(jaws_client, nerscman)
        return _BuildResult(
            sfapi_client=sfapi_client,
            jaws_client=jaws_client,
            lrc_flow = lrcflow,
            nersc_flow = nerscflow
         )

    def _handle_build_result(self, br: _BuildResult):
        if not self._sfapi_client and br.sfapi_client:
            self._sfapi_client = br.sfapi_client
        if br.error:
            self._build_state = None
            self._last_fail_error = br.error
            self._last_fail_time = time.monotonic()
            return br.error
        else:
            self._build_state = br._to_deps()
            self._last_fail_error = None
            self._last_fail_time = None
            return None

    def _build_flows(self, jaws_client: JAWSClient, nerscman: NERSCManager
    ) -> (NERSCJAWSRunner, LawrenciumJAWSRunner):
        lrcjawsflow = LawrenciumJAWSRunner(
            nerscman,
            jaws_client,
            self._mongodao,
            self._s3config,
            self._kafka,
            self._coman,
            self._service_root_url,
        )
        nerscjawsflow = NERSCJAWSRunner(
            nerscman,
            jaws_client,
            self._mongodao,
            self._s3config,
            self._kafka,
            self._coman,
            self._service_root_url,
            on_refdata_complete=lrcjawsflow.nersc_refdata_complete,
        )
        return nerscjawsflow, lrcjawsflow
