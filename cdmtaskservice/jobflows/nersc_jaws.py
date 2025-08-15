"""
Manages running jobs at NERSC using the JAWS system.
"""

import logging
import os
from pathlib import Path
import traceback
from typing import Any, Callable, Awaitable
import uuid

from cdmtaskservice import logfields
from cdmtaskservice import models
from cdmtaskservice import sites
from cdmtaskservice import timestamp
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.callback_url_paths import (
    get_download_complete_callback,
    get_upload_complete_callback,
    get_error_log_upload_complete_callback,
    get_refdata_download_complete_callback,
)
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import (
    InvalidJobStateError,
    InvalidReferenceDataStateError,
    IllegalParameterError,
    UnauthorizedError,
)
from cdmtaskservice.jaws import client as jaws_client
from cdmtaskservice.jaws.poller import poll as poll_jaws
from cdmtaskservice.jobflows.flowmanager import JobFlow
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.nersc.manager import NERSCManager, TransferResult, TransferState
from cdmtaskservice.s3.client import S3Client, S3ObjectMeta, PresignedPost
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.update_state import (
    submitted_nersc_download,
    submitting_job,
    submitted_jaws_job,
    submitting_upload,
    submitted_nersc_upload,
    complete,
    submitting_error_processing,
    submitted_nersc_error_processing,
    error,
    JobUpdate,
    submitted_nersc_refdata_download,
    refdata_complete,
    refdata_error,
    RefdataUpdate,
)
from cdmtaskservice.user import CTSUser

# Not sure how other flows would work and how much code they might share. For now just make
# this work and pull it apart / refactor later.

# TODO RELIABILITY will need a system for detecting NERSC downs, not putting jobs into an
#                  error state while it's down, and resuming jobs when it's back up

# TODO COdE when a job is passed in, make sure the cluster matches.


class NERSCJAWSRunner(JobFlow):
    """
    Runs jobs at NERSC using JAWS.
    """
    
    CLUSTER = sites.Cluster.PERLMUTTER_JAWS
    """ The cluster on which this runner operates. """
    
    def __init__(
        self,
        nersc_manager: NERSCManager,
        jaws_client: jaws_client.JAWSClient,
        mongodao: MongoDAO,
        s3_client: S3Client,
        s3_external_client: S3Client,
        container_s3_log_dir: str,
        kafka: KafkaNotifier, 
        coro_manager: CoroutineWrangler,
        service_root_url: str,
        s3_insecure_ssl: bool = False,
        on_refdata_complete: Callable[[models.ReferenceData], Awaitable[None]] = None,
    ):
        """
        Create the runner.
        
        nersc_manager - the NERSC manager.
        jaws_client - a JAWS Central client.
        mongodao - the Mongo DAO object.
        s3_client - an S3 client pointed to the data stores.
        s3_external_client - an S3 client pointing to an external URL for the S3 data stores
            that may not be accessible from the current process, but is accessible to remote
            processes at NERSC.
        container_s3_log_dir - where to store container logs in S3.
        kafka - a kafka notifier.
        coro_manager - a coroutine manager.
        service_root_url - the URL of the service root, used for constructing service callbacks.
        s3_insecure_url - whether to skip checking the SSL certificate for the S3 instance,
            leaving the service open to MITM attacks.
        on_refdata_complete - an optional async function that will be called when refdata
            has completed staging at NERSC. Takes a refdata object as an argument.
        """
        self._nman = _not_falsy(nersc_manager, "nersc_manager")
        self._jaws = _not_falsy(jaws_client, "jaws_client")
        self._mongo = _not_falsy(mongodao, "mongodao")
        self._s3 = _not_falsy(s3_client, "s3_client")
        self._s3ext = _not_falsy(s3_external_client, "s3_external_client")
        self._s3insecure = s3_insecure_ssl
        self._s3logdir = _require_string(container_s3_log_dir, "container_s3_log_dir")
        self._kafka = _not_falsy(kafka, "kafka")
        self._coman = _not_falsy(coro_manager, "coro_manager")
        self._callback_root = _require_string(service_root_url, "service_root_url")
        self._on_refdata_complete = on_refdata_complete
    
    @classmethod
    def get_cluster(cls) -> sites.Cluster:
        """ Get the cluster on which this job flow manager operates. """
        return cls.CLUSTER
    
    async def _handle_exception(
        self, e: Exception, entity_id: str, errtype: str, refdata: bool = False
    ):
        logging.getLogger(__name__).exception(
            f"Error {errtype} {'refdata' if refdata else 'job'}.",
            extra={logfields.REFDATA_ID if refdata else logfields.JOB_ID: entity_id}
        )
        await self._save_err_to_mongo(
            entity_id,
            # We'll need to see what kinds of errors happen and change the user message
            # appropriately. Just provide a generic message for now, as most errors aren't
            # going to be fixable by users
            "An unexpected error occurred",
            str(e),
            traceback=traceback.format_exc(),
            refdata=refdata,
        )
    
    async def _get_transfer_result(
        self,
        trans_func: Callable[[], Awaitable[tuple[TransferResult, Any]]],
        entity_id: str,
        op: str,
        err_type: str,
        refdata: bool = False
    ) -> Any:
        # can't check that the NERSC task is complete first because the task
        # won't complete until the callback request returns, which won't happen
        # if we wait for the task to complete. IOW, deadlock
        try:
            res, data = await trans_func()
        except Exception as e:
            await self._handle_exception(e, entity_id, err_type, refdata=refdata)
            raise
        if res.state == TransferState.INCOMPLETE:
            errcls = InvalidReferenceDataStateError if refdata else InvalidJobStateError
            raise errcls(f"{op} task is not complete")
        elif res.state == TransferState.FAIL:
            logging.getLogger(__name__).error(
                f"{op} failed for {'refdata' if refdata else 'job'}.",
                extra={
                    logfields.REFDATA_ID if refdata else logfields.JOB_ID: entity_id,
                    logfields.REMOTE_ERROR: res.message,
                    logfields.REMOTE_TRACEBACK: res.traceback
                }
            )
            await self._save_err_to_mongo(
                entity_id,
                f"An unexpected error occurred during file {op.lower()}",
                res.message,
                traceback=res.traceback,
                refdata=refdata,
            )
            raise ValueError(f"{op} failed: {res.message}")
        else:
            return data
    
    async def _save_err_to_mongo(
        self,
        entity_id: str,
        user_err: str,
        admin_err: str,
        traceback: str = None,
        logpath: str = None,
        refdata=False,
    ):
        # if this fails, well, then we're screwed
        # could probably simplify this with a partial fn to hold the cluster arg.. meh
        if refdata:
            await self._update_refdata_state(entity_id, refdata_error(
                user_err, admin_err, traceback=traceback)
            )
        else:
            await self._update_job_state(entity_id, error(
                user_err, admin_err, traceback=traceback, log_files_path=logpath
            ))

    async def _update_job_state(self, job_id: str, update: JobUpdate):
        # may want to factor this to a shared module if we ever support other flows
        # TODO TEST will need to mock out uuid
        trans_id = str(uuid.uuid4())
        # TODO TEST will need a way to mock out timestamps
        update_time = timestamp.utcdatetime()
        async def cb():
            await self._mongo.job_update_sent(job_id, trans_id)
        await self._mongo.update_job_state(job_id, update, update_time, trans_id)
        await self._kafka.update_job_state(
            job_id, update.new_state, update_time, trans_id, callback=cb()
        )

    async def _update_refdata_state(self, refdata_id: str, update: RefdataUpdate
    ):
        await self._mongo.update_refdata_state(
            # TODO TEST will need a way to mock out timestamps
            self.CLUSTER, refdata_id, update, timestamp.utcdatetime()
        )

    def precheck(self, user: CTSUser, job_input: models.JobInput):
        """
        Check that the inputs to a job are acceptable prior to running a job. Will throw an
        error if the inputs don't meet requirements.
        
        user - the user running the job.
        job_input - the job input.
        """
        _not_falsy(user, "user")
        _not_falsy(job_input, "job_input")  # unused for now
        if not user.is_kbase_staff or not user.has_nersc_account:
            raise UnauthorizedError(
                f"To use the {self.CLUSTER.value} site, you must be a KBase staff member "
                + "and have a NERSC account"
            )

    async def get_job_external_runner_status(self, job: models.AdminJobDetails) -> dict[str, Any]:
        """
        Get details from the external job runner (JAWS in this case) about the job.
        
        Returns the JAWS status dict as returned from JAWS. If the job has not yet been submitted
        to JAWS, an empty dict is returned.
        """
        # allow getting details from earlier runs? Seems unnecessary
        if _not_falsy(job, "job").job_input.cluster != self.CLUSTER:
            raise ValueError(f"Job cluster must match {self.CLUSTER}")
        if not job.jaws_details or not job.jaws_details.run_id:
            return {}  # job not submitted yet
        jaws_id = job.jaws_details.run_id[-1]  # if run_id exists, there's a job ID in it
        return await self._jaws.status(jaws_id)

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
        # TODO PERF this validates the file paths yet again. Maybe the way to go is just have
        #           a validate method on S3Paths which can be called or not as needed, with
        #           a validated state boolean
        paths = S3Paths([p.path for p in _not_falsy(objmeta, "objmeta")])
        try:
            # TODO RELIABILITY config / set expiration time
            presigned = await self._s3ext.presign_get_urls(paths)
            callback_url = get_download_complete_callback(self._callback_root, job.id)
            # TODO PERF config / set concurrency
            # TODO DISKSPACE will need to clean up job downloads @ NERSC
            task_id = await self._nman.download_s3_files(
                job.id, objmeta, presigned, callback_url, insecure_ssl=self._s3insecure
            )
            # Hmm. really this should go through job state but that seems pointless right now.
            # May need to refactor this and the mongo method later to be more generic to
            # remote cluster and have job_state handle choosing the correct mongo method & params
            # to run
            await self._update_job_state(job.id, submitted_nersc_download(task_id))
        except Exception as e:
            await self._handle_exception(e, job.id, "starting file download for")

    async def download_complete(self, job: models.AdminJobDetails):
        """
        Continue a job after the download is complete. The job is expected to be in the 
        download submitted state.
        """
        if _not_falsy(job, "job").state != models.JobState.DOWNLOAD_SUBMITTED:
            raise InvalidJobStateError("Job must be in the download submitted state")
        async def tfunc():
            return await self._nman.get_s3_download_result(job), None
        await self._get_transfer_result(  # check for errors
            tfunc, job.id, "Download", "getting download results for",
        )
        await self._update_job_state(job.id, submitting_job())
        await self._coman.run_coroutine(self._submit_jaws_job(job))
    
    async def _submit_jaws_job(self, job: models.AdminJobDetails):
        try:
            # TODO PERF configure file download concurrency
            jaws_job_id = await self._nman.run_JAWS(job)
            # See notes above about adding the NERSC task id to the job
            await self._update_job_state(job.id, submitted_jaws_job(jaws_job_id))
            jaws_info = await poll_jaws(self._jaws, job.id, jaws_job_id)
            await self._job_complete(job, jaws_info)
        except Exception as e:
            await self._handle_exception(e, job.id, "starting JAWS job for")

    async def job_complete(self, job: models.AdminJobDetails):
        """
        Continue a job after the remote job run is complete. The job is expected to be in the
        job submitted state.
        """
        if _not_falsy(job, "job").state != models.JobState.JOB_SUBMITTED:
            raise InvalidJobStateError("Job must be in the job submitted state")
        # We assume this is a jaws job if it was mapped to this runner
        # TODO RETRIES this line might need changes
        jaws_info = await self._jaws.status(job.jaws_details.run_id[-1])
        await self._job_complete(job, jaws_info)
    
    async def _job_complete(self, job: models.AdminJobDetails, jaws_info: dict[str, Any]):
        if not jaws_client.is_done(jaws_info):
            raise InvalidJobStateError("JAWS run is incomplete")
        res = jaws_client.result(jaws_info)
        if res == jaws_client.JAWSResult.SUCCESS:
            await self._update_job_state(
                job.id, submitting_upload(cpu_hours=jaws_info["cpu_hours"])
            )
            await self._coman.run_coroutine(self._upload_files(job, jaws_info))
        elif res == jaws_client.JAWSResult.FAILED:
            await self._update_job_state(
                job.id, submitting_error_processing(cpu_hours=jaws_info["cpu_hours"])
            )
            await self._coman.run_coroutine(self._upload_container_logs(job, jaws_info))
        elif res == jaws_client.JAWSResult.CANCELED:
            await self._update_job_state(job.id, error(
                "The job was unexpectedly canceled",
                "JAWS reported the job as canceled",
                cpu_hours=jaws_info["cpu_hours"],
            ))
            
        elif res == jaws_client.JAWSResult.SYSTEM_ERROR:
            # there's no way to force a jaws system error that I'm aware of, will need to
            # test via unit tests
            await self._update_job_state(job.id, error(
                "An unexpected error occurred",
                "JAWS failed to run the job - check the JAWS job logs",
                cpu_hours=jaws_info["cpu_hours"],
            ))
        else:  # should never happen
            raise ValueError(f"unexpected JAWS result: {res}")
    
    async def _upload_container_logs(self, job: models.AdminJobDetails, jaws_info: dict[str, Any]):
        # we're assuming here that the errors.json file @ NERSC has the std* files
        # if not this will break badly, but it also means (I think) that JAWS is broken badly
        # So things are in a right old mess and a service admin will need to dig into it
        async def presign(output_files: list[Path]) -> list[PresignedPost]:
            root = Path(self._s3logdir) / job.id
            # TODO RELIABILITY config / set expiration time
            paths = S3Paths([os.path.join(root, f) for f in output_files])
            return await self._s3ext.presign_post_urls(paths)
        
        try:
            # TODO PERF config / set concurrency
            task_id = await self._nman.upload_JAWS_log_files_on_error(
                job,
                jaws_info["output_dir"],
                presign,
                get_error_log_upload_complete_callback(self._callback_root, job.id),
                insecure_ssl=self._s3insecure,
            )
            await self._update_job_state(job.id, submitted_nersc_error_processing(task_id))
        except Exception as e:
            await self._handle_exception(e, job.id, "starting error processing for")
    
    async def _upload_files(self, job: models.AdminJobDetails, jaws_info: dict[str, Any]):
        # This is kind of similar to the method above, not sure if trying to merge is worth it
        
        async def presign(output_files: list[Path], crc64nvmes: list[str]) -> list[PresignedPost]:
            root = job.job_input.output_dir
            # TODO RELIABILITY config / set expiration time
            paths = S3Paths([os.path.join(root, f) for f in output_files])
            return await self._s3ext.presign_post_urls(paths, crc64nvmes=crc64nvmes)
        
        try:
            # TODO PERF config / set concurrency
            task_id = await self._nman.upload_JAWS_job_files(
                job,
                jaws_info["output_dir"],
                presign,
                get_upload_complete_callback(self._callback_root, job.id),
                insecure_ssl=self._s3insecure,
            )
            # See notes above about adding the NERSC task id to the job
            await self._update_job_state(job.id, submitted_nersc_upload(task_id))
        except Exception as e:
            await self._handle_exception(e, job.id, "starting file upload for")

    async def upload_complete(self, job: models.AdminJobDetails):
        """
        Complete a job after the upload is complete. The job is expected to be in the 
        upload submitted state.
        """
        if _not_falsy(job, "job").state != models.JobState.UPLOAD_SUBMITTED:
            raise InvalidJobStateError("Job must be in the upload submitted state")
        async def tfunc():
            return await self._nman.get_presigned_upload_result(job), None
        await self._get_transfer_result(  # check for errors
            tfunc, job.id, "Upload", "getting upload results for",
        )
        await self._coman.run_coroutine(self._upload_complete(job))
    
    async def _upload_complete(self, job: models.AdminJobDetails):
        try:
            checksums = await self._nman.get_uploaded_JAWS_files(job)
            filechecksums = {
                os.path.join(job.job_input.output_dir, f): crc for f, crc in checksums.items()
            }
            # TODO PERF parsing the paths for the zillionth time
            # TODO PERF configure / set concurrency
            s3objs = await self._s3.get_object_meta(S3Paths(filechecksums.keys()))
            outfiles = []
            for o in s3objs:
                if o.crc64nvme != filechecksums[o.path]:
                    raise ValueError(
                        f"Expected CRC64/NVME checkusm {filechecksums[o.path]} but got "
                        + f"{o.crc64nvme} for uploaded file {o.path}"
                    )
                outfiles.append(models.S3File(file=o.path, crc64nvme=o.crc64nvme))
            # TODO DISKSPACE will need to clean up job results @ NERSC
            await self._update_job_state(job.id, complete(outfiles))
        except Exception as e:
            await self._handle_exception(e, job.id, "completing")


    async def error_log_upload_complete(self, job: models.AdminJobDetails):
        """
        Complete an errored job after the log file upload is complete. The job is expected to
        be in the error processing submitted state.
        """
        if _not_falsy(job, "job").state != models.JobState.ERROR_PROCESSING_SUBMITTED:
            raise InvalidJobStateError("Job must be in the error processing submitted state")
        async def tfunc():
            return await self._nman.get_presigned_error_log_upload_result(job)
        data = await self._get_transfer_result(
            tfunc,
            job.id,
            "Error log upload",
            "getting error log upload results for",
        )
        if {i[0] for i in data} != {0}:
            err = (f"At least one container did not start or exited with a non-zero error code. "
                   + "Please examine the logs for details.")
        else:  # will probably need to expand this as we learn about JAWS errors
            err = "An unexpected error occurred."
        # if we can't talk to mongo there's not much to do
        await self._save_err_to_mongo(
            job.id,
            err,
            f"Example container error: {data[0][1]}",
            logpath=os.path.join(self._s3logdir, job.id),
        )

    async def clean_job(self, job: models.AdminJobDetails, force: bool = False):
        """
        Clean up job files at the remote compute site.
        
        job - the job to clean up.
        force - perform the clean up even if the job isn't in a terminal state. This may cause
            undefined behavior.
        """
        # might need to have some sort of flowmanager wrapper class that checks these
        # sort of global issues and ensures that Job is not modified before passing it to the
        # flow
        if _not_falsy(job, "job").job_input.cluster != self.CLUSTER:
            raise ValueError(f"Job cluster must match {self.CLUSTER}")
        if not force and job.state not in models.JOB_TERMINAL_STATES:
            raise IllegalParameterError("Job is not in a terminal state and cannot be cleaned")
        jaws_paths = []
        if job.jaws_details and job.jaws_details.run_id:
            # this should be a very small number of runs, usually one, so don't bother
            # with parallelization
            for run in job.jaws_details.run_id:
                jaws_output = await self._jaws.status(run)
                if jaws_output.get("output_dir"):
                    # so this is a little hacky but it's less annoying than having to configure
                    # the jaws output dir
                    # Split on the run ID so the entire run is deleted, not just the sub run ID
                    d = jaws_output["output_dir"].split(run)[0] + str(run)
                    jaws_paths.append(Path(d))
        await self._nman.clean_job(job, jaws_paths)

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
        # TODO PERF this validates the file paths yet again.
        paths = S3Paths([objmeta.path])
        try:
            presigned = await self._s3ext.presign_get_urls(paths)
            callback_url = get_refdata_download_complete_callback(
                self._callback_root, refdata.id, self.CLUSTER
            )
            # TODO DISKSPACE clean up no longer used refdata @ NERSC
            #                keep the refdata mongo record so it can be restaged if necessary
            task_id = await self._nman.download_s3_files(
                refdata.id,
                [objmeta],
                presigned,
                callback_url,
                insecure_ssl=self._s3insecure,
                refdata=True,
                unpack=refdata.unpack,
            )
            await self._update_refdata_state(refdata.id, submitted_nersc_refdata_download(task_id))
        except Exception as e:
            await self._handle_exception(e, refdata.id, "starting file download for", refdata=True)

    async def refdata_complete(self, refdata_id: str):
        """
        Complete a refdata download task. The refdata is expected to be in the download
        submitted state for the cluster.
        """
        refdata = await self._mongo.get_refdata_by_id(_require_string(refdata_id, "refdata_id"))
        refstate = refdata.get_status_for_cluster(self.CLUSTER)
        if refstate.state != models.ReferenceDataState.DOWNLOAD_SUBMITTED:
            raise InvalidReferenceDataStateError(
                "Reference data must be in the download submitted state for "
                + f"cluster {refstate.cluster.value}"
            )
        async def tfunc():
            return await self._nman.get_s3_refdata_download_result(refdata), None
        await self._get_transfer_result(  # check for errors
            tfunc, refdata.id, "Download", "getting download results for", refdata=True
        )
        # TODO DISKSPACE will need to clean up refdata manifests & d/l result json files
        await self._update_refdata_state(refdata.id, refdata_complete())
        if self._on_refdata_complete:
            # don't wait for this function to run before returning
            await self._coman.run_coroutine(self._on_refdata_complete(refdata))

    async def clean_refdata(self, refdata: models.ReferenceData, force: bool = False):
        """
        Clean up refdata staging files at the remote compute site.
        
        refdata - the refdata to clean up.
        force - perform the clean up even if the refdata staging isn't in a terminal state.
            This may cause undefined behavior.
        """
        # might need to have some sort of flowmanager wrapper class that checks these
        # sort of global issues and ensures that ReferenceData is not modified before passing
        # it to the flow
        refstate = _not_falsy(refdata, "refdata").get_status_for_cluster(self.CLUSTER)
        if not force and refstate.state not in models.REFDATA_TERMINAL_STATES:
            raise IllegalParameterError("Refdata is not in a terminal state and cannot be cleaned")
        await self._nman.clean_refdata(refdata)
