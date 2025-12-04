"""
The KBase job flow implementation and provider. Runs jobs on the HTCondor system at KBase.
"""

import asyncio
from dataclasses import dataclass
import htcondor2
import logging
from pathlib import Path
import time
from typing import Any

from cdmtaskservice.arg_checkers import (
    not_falsy as _not_falsy,
    check_num as _check_num,
    require_string as _require_string,
)
from cdmtaskservice.condor.client import CondorClient, condor_job_stats, condor_jobs_all_held
from cdmtaskservice.condor.config import CondorClientConfig
from cdmtaskservice.config_s3 import S3Config
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import (
    UnauthorizedError,
    InvalidJobStateError,
    InvalidReferenceDataStateError,
    UnsupportedOperationError,
)
from cdmtaskservice.jobflows.flowmanager import JobFlow, JobFlowOrError
from cdmtaskservice.jobflows.state_updates import JobFlowStateUpdates
from cdmtaskservice import models
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice.refserv.client import RefdataServiceClient
from cdmtaskservice.s3.client import S3ObjectMeta
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice import sites
from cdmtaskservice import subjobs
from cdmtaskservice import update_state
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
        s3config: S3Config,
        kafka: KafkaNotifier, 
        coro_manager: CoroutineWrangler,
        refserv_client: RefdataServiceClient
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
        self._s3 = _not_falsy(s3config, "s3config").get_internal_client()
        self._s3logdir = s3config.error_log_path
        self._coman = _not_falsy(coro_manager, "coro_manager")
        self._refcli = _not_falsy(refserv_client, "refserv_client")
        self._logr = logging.getLogger(__name__)
        
        self._updates = JobFlowStateUpdates(self.CLUSTER, self._mongo, _not_falsy(kafka, "kafka"))

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
        param = job_input.params.get_file_parameter()
        if param and param.type is models.ParameterType.MANIFEST_FILE:
            raise UnsupportedOperationError(  # Implement if requested by users
                f"Manifest files are not currently supported for the {self.CLUSTER.value} job flow"
            )
        update_time = utcdatetime()
        # It's possible but unlikely that we'll save these subjobs and then the main job
        # doesn't save. If so, no big deal, we just have orphaned subjobs. Could make a cleanup
        # routine if necessary 
        await self._mongo.initialize_subjobs([
            models.SubJob(
                id=job_id,
                sub_id=i,
                state=models.JobState.DOWNLOAD_SUBMITTED,
                transition_times=[
                    models.JobStateTransition(
                        state=models.JobState.CREATED,
                        time=update_time
                    ),
                    # This is a little weird, but we want to transition the main job doc from
                    # CREATED to DOWNLOAD_SUBMITTED after submitting the job to HTC.
                    # That means the next transition is to JOB_SUMITTING, and so the containers
                    # need to skip the DOWNLOAD_SUBMITTED state.
                    # The alternative would be to add yet another state, and since the container
                    # state won't be available to regular users that seems like overkill.
                    # Also we'd have to figure out how that state fits into the other job flows.
                    models.JobStateTransition(
                        state=models.JobState.DOWNLOAD_SUBMITTED,
                        time=update_time
                    ),
                ]
            ) for i in range(job_input.num_containers)
        ])

    async def get_subjobs(self, job_id: str, container_num: int = None
    ) -> models.SubJob | list[models.SubJob]:
        """
        Get the subjobs for a job.
        
        job_id - the job's ID.
        container_num - if provided, only return the single container / subjob.
            Otherwise return a list of all containers.
        """
        _require_string(job_id, "job_id")
        if container_num is not None:
            _check_num(container_num, "container_num", minimum=0)
            return await self._mongo.get_subjob(job_id, container_num)
        return await self._mongo.get_subjobs(job_id)

    async def get_exit_codes(self, job: models.Job) -> list[int | None]:
        """
        Get the exit codes of the containers / subjobs for a job.
        """
        return await self._mongo.get_exit_codes_for_subjobs(_not_falsy(job, "job").id)

    async def get_job_external_runner_status(
        self,
        job: models.AdminJobDetails,
        container_number: int = 0
    ) -> dict[str, Any]:
        """
        Get details from the external job runner (HTCondor in this case) about the job.
        
        Returns the HTC ClassAd dict as returned from HTC. If the job has not yet been submitted
        to HTC, an empty dict is returned.
        """
        # allow getting details from earlier runs? Seems unnecessary
        if _not_falsy(job, "job").job_input.cluster != self.CLUSTER:
            raise ValueError(f"Job cluster must match {self.CLUSTER}")
        _check_num(container_number, "container_number", minimum=0)
        if container_number >= job.job_input.num_containers:
            raise ValueError(
                f"Provided container number {container_number} is larger than "
                + "the number of containers for the job when indexed from 0: "
                + f"{job.job_input.num_containers}")
        if not job.htcondor_details or not job.htcondor_details.cluster_id:
            return {}  # job not submitted yet
        # if cluster_id exists, there's a cluster ID in it
        cluster_id = job.htcondor_details.cluster_id[-1]
        return await self._condor.get_container_status(cluster_id, container_number)

    async def start_job(self, job: models.Job, objmeta: list[S3ObjectMeta]):
        """
        Start running a job. It is expected that the Job has been persisted to the data
        storage system and is in the created state.
        
        job - the job
        objmeta - the S3 object metadata for the files in the job. CRC64/NVME checksums
            are required for all objects.
        """
        if _not_falsy(job, "job").state != models.JobState.CREATED:
            raise InvalidJobStateError("Job must be in the created state")
        # Could check that the s3 and job paths / etags match... YAGNI
        try:
            cluster_id = await self._condor.run_job(job)
            # It's theoretically possible that all the containers could transition to
            # JOB_SUBMITTING and therefore trigger a main job transition, which will fail, prior
            # to this update being applied to the DB. That seems impossible in practice
            # so we don't worry about it. If it starts occurring, could have the remote job wait
            # for the main job to transition to DOWNLOAD_SUBMITTED before submitting its
            # state transition request.
            await self._updates.update_job_state(
                job.id, update_state.submitted_htcondor_download(cluster_id)
            )
        except Exception as e:
            await self._updates.handle_exception(e, job.id, "starting condor run for")

    async def download_complete(self, job: models.AdminJobDetails):
        """ Throws an exception as this method is not supported. """
        raise UnsupportedOperationError(
            f"This method is not supported for the {self.CLUSTER.value} job flow"
        )
        
    async def job_complete(self, job: models.AdminJobDetails):
        """ Throws an exception as this method is not supported. """
        raise UnsupportedOperationError(
            f"This method is not supported for the {self.CLUSTER.value} job flow"
        )
        
    async def upload_complete(self, job: models.AdminJobDetails):
        """ Throws an exception as this method is not supported. """
        raise UnsupportedOperationError(
            f"This method is not supported for the {self.CLUSTER.value} job flow"
        )
    
    async def error_log_upload_complete(self, job: models.AdminJobDetails):
        """ Throws an exception as this method is not supported. """
        raise UnsupportedOperationError(
            f"This method is not supported for the {self.CLUSTER.value} job flow"
        )

    _COMMON_STATE_TO_UPDATE_FUNC = {
        models.JobState.JOB_SUBMITTING: lambda _: update_state.submitting_job(),
        models.JobState.JOB_SUBMITTED: lambda _: update_state.submitted_job(),
        models.JobState.UPLOAD_SUBMITTED: lambda _: update_state.submitted_upload(),
        models.JobState.ERROR_PROCESSING_SUBMITTED: lambda _:
            update_state.submitted_error_processing(),
    }
    
    _SUBJOB_STATE_TO_UPDATE_FUNC = {
        **_COMMON_STATE_TO_UPDATE_FUNC,
        models.JobState.UPLOAD_SUBMITTING: lambda update:
            update_state.submitting_upload_with_exit_code(update.exit_code),
        models.JobState.COMPLETE: lambda update: update_state.complete(update.outputs),
        models.JobState.ERROR_PROCESSING_SUBMITTING: lambda update:
            update_state.submitting_error_processing_with_exit_code(update.exit_code),
        models.JobState.ERROR: lambda update: update_state.error(
            update.admin_error, traceback=update.traceback
        ),
    }
    
    _JOB_STATE_TO_UPDATE_FUNC = {
        **_COMMON_STATE_TO_UPDATE_FUNC,
        models.JobState.UPLOAD_SUBMITTING: lambda _: update_state.submitting_upload(),
        models.JobState.ERROR_PROCESSING_SUBMITTING: lambda _:
            update_state.submitting_error_processing()
    }
    
    async def update_container_state(
        self,
        job: models.AdminJobDetails,
        container_num: int,
        new_state: models.JobState,
        update: models.ContainerUpdate,
    ):
        """
        Update the state of a container / subjob.
        
        job - the parent job of the container.
        container_num - the container / subjob number.
        update - the new state for the container.
        """
        # TODO UPDATE_SUBJOB add other states.
        _not_falsy(job, "job")
        _check_num(container_num, "conteiner_num", minimum=0)
        _not_falsy(new_state, "new_state")
        _not_falsy(update, "update")
        if new_state not in self._SUBJOB_STATE_TO_UPDATE_FUNC:
            raise UnsupportedOperationError(
                f"Cannot update a container to state {new_state.value}"
        )
        mongo_update = self._SUBJOB_STATE_TO_UPDATE_FUNC[new_state](update)
        # Just throw the error, don't error out the job. If the caller thinks this is an error
        # they can try and set the error state.
        await self._mongo.update_subjob_state(job.id, container_num, mongo_update, update.time)
        # If this fails the job is only stuck if parent_update is not None. It seems really
        # unlikely that the line above would succeed and this line fail, so we don't catch
        # errors here
        parent_update = await subjobs.get_job_update(self._mongo, job, new_state)
        if parent_update:
            if parent_update.state.is_terminal():
                # In order to get the final job info from Condor, need to return so the
                # subjobs can exit
                await self._coman.run_coroutine(self._update_terminal_job(job, parent_update))
                return
            # May not be the same update func as the subjob
            mongo_update = self._JOB_STATE_TO_UPDATE_FUNC[parent_update.state](update)
            # If this fails all the containers have transitioned to an equivalent state and
            # so the job is stuck, so we error out if possible.
            try:
                await self._updates.update_job_state(
                    job.id, mongo_update, update_time=parent_update.time
                )
            except Exception as e:
                await self._updates.handle_exception(e, job.id, "updating job state")
    
    async def _update_terminal_job(
        self, job: models.AdminJobDetails, parent_update: subjobs.JobUpdate
    ):
        try:
            if parent_update.state == models.JobState.ERROR:
                await self._error_job(job)
            else:
                await self._complete_job(job)
        except Exception as e:
            await self._updates.handle_exception(e, job.id, "completing errored")

    async def _error_job(self, job: models.AdminJobDetails):
        cpu_hours, cpu_efficiency, max_mem = await self._get_condor_stats(job)
        exit_codes = await self.get_exit_codes(job)
        # if any exit codes are present and > 0, a container failed. Exit codes can be None
        # if the container never ran
        err_exit = set(exit_codes) - {0, None}
        if err_exit:
            err = (f"At least one container exited with a non-zero "
                   + "error code. Please examine the logs for details.")
        else:
            err = "An unexpected error occurred."
        await self._updates.update_job_state(job.id, update_state.error(
            "Check subjobs / containers for admin errors",  # maybe improve later,
            user_error=err,
            log_files_path=str(Path(self._s3logdir) / job.id) if err_exit else None,
            cpu_hours=cpu_hours,
            cpu_efficiency=cpu_efficiency,
            max_memory=max_mem,
        ))
        
    async def _complete_job(self, job: models.AdminJobDetails):
        cpu_hours, cpu_efficiency, max_mem = await self._get_condor_stats(job)
        subjobs = await self.get_subjobs(job.id)
        filechecksums = {}
        for sj in subjobs:
            for f in sj.outputs:
                filechecksums[f.file] = f.crc64nvme
        if not filechecksums:
            err = "The job produced no output files"
            await self._updates.update_job_state(job.id, update_state.error(err, user_error=err))
            return
        s3objs = await self._s3.get_object_meta(S3Paths(filechecksums.keys()))
        outfiles = []
        for o in s3objs:
            if o.crc64nvme != filechecksums[o.path]:
                raise ValueError(
                    f"Expected CRC64/NVME checksum {filechecksums[o.path]} but got "
                    + f"{o.crc64nvme} for uploaded file {o.path}"
                )
            outfiles.append(models.S3File(file=o.path, crc64nvme=o.crc64nvme))
        await self._updates.update_job_state(job.id, update_state.complete(
            outfiles,
            cpu_hours=cpu_hours,
            cpu_efficiency=cpu_efficiency,
            max_memory=max_mem
        ))

    async def _get_condor_stats(self, job: models.AdminJobDetails) -> tuple[float, float, int]:
        attempts = 0
        # if cluster_id exists, there's a cluster ID in it
        cluster_id = job.htcondor_details.cluster_id[-1]
        while attempts < 12:  # 60 seconds for condor to finish the job, seems ample?
            await asyncio.sleep(5)  # give Condor a few seconds to finish up
            running, complete = await self._condor.get_job_status(cluster_id)
            # Kind of inefficient but I doubt this will happen often
            # If the condor job errors, it's held with the current setup
            # Means the client and this code is coupled, might need to rethink
            if not running or condor_jobs_all_held(running):
                return condor_job_stats(running + complete, job.job_input.cpus)
            attempts += 1
        raise IOError("Condor jobs didn't complete for 60s after all executors sent termination")
    
    async def clean_job(self, job: models.AdminJobDetails, force: bool = False):
        """
        Do nothing. Job cleanup is handled by HTCondor.
        """
        pass # Intentionally do nothing 


    async def stage_refdata(self, refdata: models.ReferenceData, objmeta: S3ObjectMeta):
        """
        Start staging reference data. It is expected that the ReferenceData has been persisted to
        the data storage system and is in the created state.
        
        refdata - the reference data
        objmeta - the S3 object metadata for the reference data file. The CRC64/NVME checksum
            is required.
        """
        refstate = _not_falsy(refdata, "refdata").get_status_for_cluster(self.CLUSTER)
        if refstate.state != models.ReferenceDataState.CREATED:
            raise InvalidReferenceDataStateError("Reference data must be in the created state")
        # Could check that the s3 and refdata path / etag match... YAGNI
        try: 
            await self._refcli.stage_refdata(refdata.id, self.CLUSTER)
        except Exception as e:
            await self._updates.handle_exception(
                e, refdata.id, "starting staging for", refdata=True
            )

    async def refdata_complete(self, refdata_id: str):
        """ Throws an exception as this method is not supported. """
        raise UnsupportedOperationError(
            f"This method is not supported for the {self.CLUSTER.value} job flow"
        )

    _COMMON_STATE_TO_UPDATE_REFDATA_FUNC = {
        models.ReferenceDataState.DOWNLOAD_SUBMITTED: lambda _:
            update_state.submitted_refdata_download(),
        models.ReferenceDataState.COMPLETE: lambda _: update_state.refdata_complete(),
        models.ReferenceDataState.ERROR: lambda update: update_state.refdata_error(
            "An unexpected error occurred", update.admin_error, traceback=update.traceback),
    }

    async def update_refdata_state(
        self,
        refdata_id: str,
        new_state: models.ReferenceDataState,
        update: models.RefdataUpdate | None = None
    ):
        """
        Update refdata state for the KBase site.
        
        refdata_id - the reference data's ID.
        new_state - the new state for the reference data.
        update - the update to apply.
        """
        _require_string(refdata_id, "refdata_id")
        _not_falsy(new_state, "new_state")
        update = update or models.RefdataUpdate()
        if new_state not in self._COMMON_STATE_TO_UPDATE_REFDATA_FUNC:
            raise UnsupportedOperationError(
                f"Cannot update refdatqa to state {new_state.value}"
        )
        mongo_update = self._COMMON_STATE_TO_UPDATE_REFDATA_FUNC[new_state](update)
        # Just throw the error, don't error out the refdata staging.
        # If the caller thinks this is an error they can try and set the error state.
        # Theoretically this could be used to transition a complete job to an error state,
        # but since the endpoint requires a special role don't worry about it for now.
        # Could add a criteria to the mongo query such that
        # if new_state = error current_state != complete
        await self._mongo.update_refdata_state(
            self.CLUSTER, refdata_id, mongo_update, utcdatetime()
        )

    async def clean_refdata(self, refdata: models.ReferenceData, force: bool = False):
        """
        Do nothing. There's nothing to clean up.
        """
        pass # Intentionally do nothing


@dataclass(frozen=True)
class _Dependencies():
    refdata_client: RefdataServiceClient
    kbase_job_flow: KBaseRunner


class KBaseFlowProvider:
    """
    Manages KBase job flow initialization.
    """
    
    @classmethod
    async def create(
        cls,
        condor_config: CondorClientConfig,
        mongodao: MongoDAO,
        s3config: S3Config,
        kafka_notifier: KafkaNotifier,
        coman: CoroutineWrangler,
        refserver_url: str,
        refserver_token: str,
    ):
        """
        WARNING: this class is not thread safe.
        
        Create the flow provider.

        condor_config - the configuration for the condor client.
        mongodao - the Mongo DAO.
        s3config - the S3 configuration.
        kafka_notifier - a kafka notifier.
        coman - a coroutine manager.
        refserver_url - the url for the refdata server.
        refserver_token - the token to use when communicating with the refdata server.
        """
    
        kb = cls()
        kb._condor_config = _not_falsy(condor_config, "condor_config")
        kb._mongodao = _not_falsy(mongodao, "mongodao")
        kb._s3config = _not_falsy(s3config, "s3config")
        kb._kafka = _not_falsy(kafka_notifier, "kafka_notifier")
        kb._coman = _not_falsy(coman, "coman")
        kb._refserv_url = _require_string(refserver_url, "refserver_url")
        kb._refserv_token = _require_string(refserver_token, "refserver_token")
        
        kb._logr = logging.getLogger(__name__)
        kb._closed = False
        
        res = await kb._build_deps()
        kb._handle_build_result(res)
        return kb
        
    def __init__(self):
        """
        Don't call this method. Ever. If you do may a curse be upon you such that your
        head falls off at an awkward moment
        """
        
    async def close(self):
        """
        Close all resources managed by this class.
        """
        self._closed = True
        if isinstance(self._build_state, _Dependencies):
            await self._build_state.refdata_client.close()

    def _check_closed(self):
        if self._closed:
            raise ValueError("Provider is closed")

    async def get_kbase_job_flow(self) -> JobFlowOrError:
        """
        Get the KBase job flow manager or an error message.
        """
        self._check_closed()
        err = self._check_build()
        if err:
            return JobFlowOrError(error=err)
        return JobFlowOrError(jobflow=self._build_state.kbase_job_flow)

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
        state = self._build_state
        if not state:
            remaining_delay = _RETRY_DELAY_SEC - (time.monotonic() - self._last_fail_time)
            if remaining_delay > 0:
                return (
                    "KBase job flow startup failed. Further attempts blocked for "
                    + f"{remaining_delay}s."
                )
            fut = asyncio.get_event_loop().create_future()
            self._build_state = fut
            async def init():
                res = await self._build_deps()
                fut.set_result(res)
            asyncio.create_task(init())
            return "Recovery for KBase job flow in process"
        elif isinstance(self._build_state, asyncio.Future):
            if self._build_state.done():
                err = self._handle_build_result(self._build_state.result())
                if err:
                    return (
                    "KBase job flow startup failed. Further attempts blocked for "
                    + f"{_RETRY_DELAY_SEC}s."
                )
            else:
                return "Recovery for KBase job flow in process"
        return None

    def _handle_build_result(self, deps: _Dependencies | None) -> bool:
        """ Returns True if an error occurred. """
        if deps:
            self._build_state = deps
            self._last_fail_time = None
            return False
        else:
            self._last_fail_time = time.monotonic()
            self._build_state = None
            return True

    async def _build_deps(self) -> _Dependencies | None:
        """
        This method should only run as part of _check_build or the constructor to make sure
        it's never run concurrently.
        
        Returns the dependencies if the build was successful.
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
            condor = CondorClient(schedd, self._condor_config, self._s3config)
            self._logr.info("Initializing refdata service client...")
            refcli = await RefdataServiceClient.create(self._refserv_url, self._refserv_token)
            kbase = KBaseRunner(
                condor,
                self._mongodao,
                self._s3config,
                self._kafka,
                self._coman,
                refcli,
            )
            self._logr.info("Done")
            return _Dependencies(refcli, kbase)
        except Exception as e:
            self._logr.exception(f"Failed to initialize KBase job flow dependencies: {e}")
            return None
