"""
The KBase job flow implementation and provider. Runs jobs on the HTCondor system at KBase.
"""

import htcondor2
import logging
from pathlib import Path
import time

from cdmtaskservice.arg_checkers import require_string as _require_string, not_falsy as _not_falsy
from cdmtaskservice.condor.client import CondorClient
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import UnauthorizedError
from cdmtaskservice.jobflows.flowmanager import JobFlow, JobFlowOrError
from cdmtaskservice.jobflows.s3config import S3Config
from cdmtaskservice import models
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice import sites
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.user import CTSUser
from cdmtaskservice.timestamp import utcdatetime


_RETRY_DELAY_SEC = 5 * 60  # configurable?


class KBaseRunner(JobFlow):
    
    """
    Runs jobs at KBase using HTCondor.
    """
    
    CLUSTER = sites.Cluster.KBASE
    """ The cluster on which this runner operates. """
    
    def __init__(
        self,
        condor_client: CondorClient,
        mongodao: MongoDAO,
        s3client: S3Client,
        kafka: KafkaNotifier, 
        coro_manager: CoroutineWrangler,
    ):
        """
        Create the runner.
        
        condor_client - a HTCondor client, configured to interact with the remote condor instance.
        mongodao - the Mongo DAO.
        s3client - an S3 client initialized to interact with the S3 storage system.
        kafka - a Kafka notifier.
        coro_manager - a coroutine manager.
        """
        self._condor = _not_falsy(condor_client, "condor_client")
        self._mongo = _not_falsy(mongodao, "mongodao")
        self._s3 = _not_falsy(s3client, "s3client")
        self._kafka = _not_falsy(kafka, "kafka")
        self._coman = _not_falsy(coro_manager, "coro_manager")
        self._logr = logging.getLogger(__name__)
        
        self._logr.info("Initialized kbase job flow")  # TODO REMOVE
        # TODO NOW implement stuff

    async def preflight(self, user: CTSUser, job_id: str, job_input: models.JobInput):
        """
        Check that the inputs to a job are acceptable prior to running a job and set up subjob
        data. Will throw an error if the inputs don't meet requirements.
        
        user - the user running the job.
        job_id - the job's ID.
        job_input - the job input
        """
        _not_falsy(user, "user")
        _require_string(job_id, "job_id")
        _not_falsy(job_input, "job_input")
        if not user.is_kbase_staff:
            raise UnauthorizedError(
                f"To use the {self.CLUSTER.value} site, you must be a KBase staff member."
            )
        update_time = utcdatetime()
        # It's possible but unlikely that we'll save these subjobs and then the main job
        # doesn't save. If so, no big deal, we just have orphaned subjobs. Could make a cleanup
        # routine if necessary 
        await self._mongo.initialize_subjobs([
            models.SubJob(
                id=job_id,
                sub_id=i,
                state=models.JobState.CREATED,
                transition_times=[models.JobStateTransition(
                    state=models.JobState.CREATED,
                    time=update_time
                )]
            ) for i in range(1, job_input.num_containers + 1)
        ])


class KBaseFlowProvider:
    """
    Manages KBase job flow initialization.
    """
    
    # So we don't really need a create method here since this is all sync, but we go ahead to
    # match the other flow provider.
    @classmethod
    def create(
        cls,
        condor_initial_dir: Path,
        condor_executable_url: str,
        condor_code_archive_url: str,
        mongodao: MongoDAO,
        s3config: S3Config,
        kafka_notifier: KafkaNotifier,
        coman: CoroutineWrangler,
        service_root_url: str,
        condor_client_group: str | None = None,
    ):
        """
        WARNING: this class is not thread safe.
        
        Create the flow provider.
        
        condor_initial_dir - where the job logs should be stored on the condor scheduler host.
            The path must exist there and locally.
        condor_executable_url - the url for the executable to run in the condor worker for
            each job. Must end in a file name and have no query or fragment.
        condor_code_archive_url - the url for the *.tgz file code archive to transfer to
            the condor worker for each job. Must end in a file name and have no query or fragment.
        mongodao - the Mongo DAO.
        s3config - the S3 configuration.
        kafka_notifier - a kafka notifier.
        coman - a coroutine manager.
        service_root_url - the URL of the service root, used by the remote job to update job
            state.
        condor_client_group - the client group to submit jobs to, if any. This is a classad on
            a worker with the name CLIENTGROUP.
        """
    
        kb = cls()
        kb._initial_dir = _not_falsy(condor_initial_dir, "condor_initial_dir")
        kb._exe_url = _require_string(condor_executable_url, "condor_executable_url")
        kb._code_archive_url = _require_string(condor_code_archive_url, "condor_code_archive_url")
        kb._mongodao = _not_falsy(mongodao, "mongodao")
        kb._s3config = _not_falsy(s3config, "s3config")
        kb._kafka = _not_falsy(kafka_notifier, "kafka_notifier")
        kb._coman = _not_falsy(coman, "coman")
        kb._service_root_url = _require_string(service_root_url, "service_root_url")
        kb._cligrp = condor_client_group
        
        kb._logr = logging.getLogger(__name__)
        
        kb._build_deps()
        return kb
        
    def __init__(self):
        """
        Don't call this method. Ever. If you do may a curse be upon you such that your
        head falls off at an awkward moment
        """

    async def get_kbase_job_flow(self) -> JobFlowOrError:
        """
        Get the KBase job flow manager or an error message.
        """
        err = self._check_build()
        if err:
            return JobFlowOrError(error=err)
        return JobFlowOrError(jobflow=self._kbase)

    # TODO KBASESTART could maybe make this automatic vs lazy in the future. Would need to be
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
        if not self._kbase:
            remaining_delay = _RETRY_DELAY_SEC - (time.monotonic() - self._last_fail_time)
            if remaining_delay > 0:
                return (
                    "KBase job flow startup failed. Further attempts blocked for "
                    + f"{remaining_delay}s."
                )
            success = self._build_deps()
            if not success:
                return (
                    "KBase job flow startup failed. Further attempts blocked for "
                    + f"{_RETRY_DELAY_SEC}s."
                )
        return None

    def _build_deps(self):
        """
        This method should only run as part of _check_build or the constructor to make sure
        it's never run concurrently.
        
        Returns true if the build was successful.
        """
        # all this is fast enough we shouldn't need to have a complex async system like
        # the JAWS flow manager
        try:
            self._logr.info("Initializing HTCondor Schedd() instance...")
            # For some reason the collector ignores the COLLECTOR_HOST parameter, so we
            # explicitly pass it in
            collector = htcondor2.Collector(htcondor2.param["COLLECTOR_HOST"])
            schedd_ad = collector.locate(htcondor2.DaemonTypes.Schedd)
            schedd = htcondor2.Schedd(schedd_ad)
            self._logr.info("Done")
            condor = CondorClient(
                schedd,
                self._initial_dir,
                self._exe_url,
                self._code_archive_url,
                client_group=self._cligrp,
                # TODO CONDOR add service root and s3 host / insecure/ log path
            )
            kbase = KBaseRunner(
                condor, self._mongodao, self._s3config.client, self._kafka, self._coman
            )
            self._kbase = kbase
            self._last_fail_time = None
            return True
        except Exception as e:
            self._logr.exception(f"Failed to initialize KBase job flow dependencies: {e}")
            self._kbase = None
            self._last_fail_time = time.monotonic()
            return False
