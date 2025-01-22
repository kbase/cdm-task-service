"""
Manages running jobs at NERSC using the JAWS system.
"""

import logging
import os
from pathlib import Path
import traceback
from typing import Any, Callable, Awaitable

from cdmtaskservice import models
from cdmtaskservice import timestamp
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.callback_url_paths import (
    get_download_complete_callback,
    get_upload_complete_callback,
    get_error_log_upload_complete_callback,
)
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import InvalidJobStateError
from cdmtaskservice.jaws import client as jaws_client
from cdmtaskservice.jaws.poller import poll as poll_jaws
from cdmtaskservice.jobflows.flowmanager import JobFlow
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.nersc.manager import NERSCManager, TransferResult, TransferState
from cdmtaskservice.s3.client import S3Client, S3ObjectMeta, PresignedPost
from cdmtaskservice.s3.paths import S3Paths

# Not sure how other flows would work and how much code they might share. For now just make
# this work and pull it apart / refactor later.

# TODO RELIABILITY will need a system for detecting NERSC downs, not putting jobs into an
#                  error state while it's down, and resuming jobs when it's back up


class NERSCJAWSRunner(JobFlow):
    """
    Runs jobs at NERSC using JAWS.
    """
    
    def __init__(
        self,
        nersc_manager: NERSCManager,
        jaws_client: jaws_client.JAWSClient,
        mongodao: MongoDAO,
        s3_client: S3Client,
        s3_external_client: S3Client,
        container_s3_log_dir: str,
        coro_manager: CoroutineWrangler,
        service_root_url: str,
        s3_insecure_ssl: bool = False,
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
        coro_manager - a coroutine manager.
        service_root_url - the URL of the service root, used for constructing service callbacks.
        s3_insecure_url - whether to skip checking the SSL certificate for the S3 instance,
            leaving the service open to MITM attacks.
        """
        self._nman = _not_falsy(nersc_manager, "nersc_manager")
        self._jaws = _not_falsy(jaws_client, "jaws_client")
        self._mongo = _not_falsy(mongodao, "mongodao")
        self._s3 = _not_falsy(s3_client, "s3_client")
        self._s3ext = _not_falsy(s3_external_client, "s3_external_client")
        self._s3insecure = s3_insecure_ssl
        self._s3logdir = _require_string(container_s3_log_dir, "container_s3_log_dir")
        self._coman = _not_falsy(coro_manager, "coro_manager")
        self._callback_root = _require_string(service_root_url, "service_root_url")
        
    async def _handle_exception(self, e: Exception, job_id: str, errtype: str):
        # TODO LOGGING figure out how logging it going to work etc.
        logging.getLogger(__name__).exception(f"Error {errtype} job {job_id}")
        await self._handle_general_exc(
            job_id,
            # We'll need to see what kinds of errors happen and change the user message
            # appropriately. Just provide a generic message for now, as most errors aren't
            # going to be fixable by users
            "An unexpected error occurred",
            str(e),
            traceback.format_exc(),
        )
    
    async def _get_transfer_result(
        self,
        trans_func: Callable[[models.Job], Awaitable[tuple[TransferResult, Any]]],
        job: models.Job,
        op: str,
        err_type: str,
    ) -> Any:
        # can't check that the NERSC task is complete first because the task
        # won't complete until the callback request returns, which won't happen
        # if we wait for the task to complete. IOW, deadlock
        try:
            res, data = await trans_func(job)
        except Exception as e:
            await self._handle_exception(e, job.id, err_type)
            raise
        if res.state == TransferState.INCOMPLETE:
            raise InvalidJobStateError(f"{op} task is not complete")
        elif res.state == TransferState.FAIL:
            logging.getLogger(__name__).error(
                f"{op} failed for job {job.id}: {res.message}\n{res.traceback}"
            )
            await self._handle_general_exc(
                job.id,
                f"An unexpected error occurred during file {op.lower()}",
                res.message,
                res.traceback,
            )
            raise ValueError(f"{op} failed: {res.message}")
        else:
            return data
    
    async def _handle_general_exc(self, job_id: str, user_err: str, admin_err: str, traceback:str):
        # if this fails, well, then we're screwed
        await self._mongo.set_job_error(
            job_id,
            user_err,
            admin_err,
            models.JobState.ERROR,
            # TODO TEST will need a way to mock out timestamps
            timestamp.utcdatetime(),
            traceback=traceback,
        )

    async def start_job(self, job: models.Job, objmeta: list[S3ObjectMeta]):
        """
        Start running a job. It is expected that the Job has been persisted to the data
        storage system and is in the created state.
        
        job - the job
        objmeta - the S3 object metadata for the files in the job.
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
            # TODO PERF CACHING if the same files are used for a different job they're d/ld again
            #                   Instead d/l to a shared file location by etag or something
            #                   Need to ensure atomic writes otherwise 2 processes trying to
            #                   d/l the same file could corrupt it
            #                   Either make cache in JAWS staging area, in which case files
            #                   will be deleted automatically by JAWS, or need own file deletion
            # TODO DISKSPACE will need to clean up job downloads @ NERSC
            task_id = await self._nman.download_s3_files(
                job.id, objmeta, presigned, callback_url, insecure_ssl=self._s3insecure
            )
            # Hmm. really this should go through job state but that seems pointless right now.
            # May need to refactor this and the mongo method later to be more generic to
            # remote cluster and have job_state handle choosing the correct mongo method & params
            # to run
            await self._mongo.add_NERSC_download_task_id(
                job.id,
                task_id,
                models.JobState.CREATED,
                models.JobState.DOWNLOAD_SUBMITTED,
                # TODO TEST will need a way to mock out timestamps
                timestamp.utcdatetime(),
            )
        except Exception as e:
            await self._handle_exception(e, job.id, "starting file download for")

    async def download_complete(self, job: models.AdminJobDetails):
        """
        Continue a job after the download is complete. The job is expected to be in the 
        download submitted state.
        """
        if _not_falsy(job, "job").state != models.JobState.DOWNLOAD_SUBMITTED:
            raise InvalidJobStateError("Job must be in the download submitted state")
        async def tfunc(job: models.Job):
            return await self._nman.get_s3_download_result(job), None
        await self._get_transfer_result(  # check for errors
            tfunc, job, "Download", "getting download results for",
        )
        await self._mongo.update_job_state(
            job.id,
            models.JobState.DOWNLOAD_SUBMITTED,
            models.JobState.JOB_SUBMITTING,
            timestamp.utcdatetime()
        )
        await self._coman.run_coroutine(self._submit_jaws_job(job))
    
    async def _submit_jaws_job(self, job: models.AdminJobDetails):
        try:
            # TODO PERF configure file download concurrency
            jaws_job_id = await self._nman.run_JAWS(job)
            # See notes above about adding the NERSC task id to the job
            await self._mongo.add_JAWS_run_id(
                job.id,
                jaws_job_id,
                models.JobState.JOB_SUBMITTING,
                models.JobState.JOB_SUBMITTED,
                # TODO TEST will need a way to mock out timestamps
                timestamp.utcdatetime(),
            )
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
            await self._mongo.update_job_state(
                job.id,
                models.JobState.JOB_SUBMITTED,
                models.JobState.UPLOAD_SUBMITTING,
                timestamp.utcdatetime()
            )
            await self._coman.run_coroutine(self._upload_files(job, jaws_info))
        elif res == jaws_client.JAWSResult.FAILED:
            await self._mongo.update_job_state(
                job.id,
                models.JobState.JOB_SUBMITTED,
                models.JobState.ERROR_PROCESSING_SUBMITTING,
                timestamp.utcdatetime()
            )
            await self._coman.run_coroutine(self._upload_container_logs(job, jaws_info))
        elif res == jaws_client.JAWSResult.SYSTEM_ERROR:
            await self._mongo.set_job_error(
                job.id,
                "An unexpected error occurred",
                "JAWS failed to run the job - check the JAWS job logs",
                models.JobState.ERROR,
                # TODO TEST will need a way to mock out timestamps
                timestamp.utcdatetime(),
            )
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
            await self._mongo.add_NERSC_log_upload_task_id(
                job.id,
                task_id,
                models.JobState.ERROR_PROCESSING_SUBMITTING,
                models.JobState.ERROR_PROCESSING_SUBMITTED,
                # TODO TEST will need a way to mock out timestamps
                timestamp.utcdatetime(),
            )
        except Exception as e:
            await self._handle_exception(e, job.id, "starting error processing for")
    
    async def _upload_files(self, job: models.AdminJobDetails, jaws_info: dict[str, Any]):
        # This is kind of similar to the method above, not sure if trying to merge is worth it
        
        async def presign(output_files: list[Path]) -> list[PresignedPost]:
            root = job.job_input.output_dir
            # TODO RELIABILITY config / set expiration time
            paths = S3Paths([os.path.join(root, f) for f in output_files])
            return await self._s3ext.presign_post_urls(paths)
        
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
            await self._mongo.add_NERSC_upload_task_id(
                job.id,
                task_id,
                models.JobState.UPLOAD_SUBMITTING,
                models.JobState.UPLOAD_SUBMITTED,
                # TODO TEST will need a way to mock out timestamps
                timestamp.utcdatetime(),
            )
        except Exception as e:
            await self._handle_exception(e, job.id, "starting file upload for")

    async def upload_complete(self, job: models.AdminJobDetails):
        """
        Complete a job after the upload is complete. The job is expected to be in the 
        upload submitted state.
        """
        if _not_falsy(job, "job").state != models.JobState.UPLOAD_SUBMITTED:
            raise InvalidJobStateError("Job must be in the upload submitted state")
        async def tfunc(job: models.Job):
            return await self._nman.get_presigned_upload_result(job), None
        await self._get_transfer_result(  # check for errors
            tfunc, job, "Upload", "getting upload results for",
        )
        await self._coman.run_coroutine(self._upload_complete(job))
    
    async def _upload_complete(self, job: models.AdminJobDetails):
        try:
            md5s = await self._nman.get_uploaded_JAWS_files(job)
            filemd5s = {os.path.join(job.job_input.output_dir, f): md5 for f, md5 in md5s.items()}
            # TODO PERF parsing the paths for the zillionth time
            # TODO PERF configure / set concurrency
            s3objs = await self._s3.get_object_meta(S3Paths(filemd5s.keys()))
            outfiles = []
            for o in s3objs:
                if o.e_tag != filemd5s[o.path]:
                    raise ValueError(
                        f"Expected Etag {filemd5s[o.path]} but got {o.e_tag} for uploaded "
                        + f"file {o.path}"
                    )
                outfiles.append(models.S3File(file=o.path, etag=o.e_tag))
            # TODO DISKSPACE will need to clean up job results @ NERSC
            await self._mongo.add_output_files_to_job(
                job.id,
                outfiles,
                models.JobState.UPLOAD_SUBMITTED,
                models.JobState.COMPLETE,
                # TODO TEST will need a way to mock out timestamps
                timestamp.utcdatetime()
            )
        except Exception as e:
            await self._handle_exception(e, job.id, "completing")


    async def error_log_upload_complete(self, job: models.AdminJobDetails):
        """
        Complete an errored job after the log file upload is complete. The job is expected to
        be in the error processing submitted state.
        """
        if _not_falsy(job, "job").state != models.JobState.ERROR_PROCESSING_SUBMITTED:
            raise InvalidJobStateError("Job must be in the error processing submitted state")
        data = await self._get_transfer_result(
            self._nman.get_presigned_error_log_upload_result,
            job,
            "Error log upload",
            "getting error log upload results for",
        )
        if {i[0] for i in data} != {0}:
            err = (f"At least one container exited with a non-zero error code. "
                   + "Please examine the logs for details.")
        else:  # will probably need to expand this as we learn about JAWS errors
            err = "An unexpected error occurred."
        # if we can't talk to mongo there's not much to do
        await self._mongo.set_job_error(
            job.id,
            err,
            f"Example container error: {data[0][1]}",
            models.JobState.ERROR,
            # TODO TEST will need a way to mock out timestamps
            timestamp.utcdatetime(),
            logpath=os.path.join(self._s3logdir, job.id),
        )
