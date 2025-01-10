"""
Manages running jobs at NERSC using the JAWS system.
"""

import logging
import os
from pathlib import Path
from typing import Any

from cdmtaskservice import models
from cdmtaskservice import timestamp
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.callback_url_paths import (
    get_download_complete_callback,
    get_upload_complete_callback,
)
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import InvalidJobStateError
from cdmtaskservice.jaws import client as jaws_client
from cdmtaskservice.jaws.poller import poll as poll_jaws
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.nersc.manager import NERSCManager
from cdmtaskservice.s3.client import S3Client, S3ObjectMeta, S3PresignedPost
from cdmtaskservice.s3.paths import S3Paths

# Not sure how other flows would work and how much code they might share. For now just make
# this work and pull it apart / refactor later.


class NERSCJAWSRunner:
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
        self._coman = _not_falsy(coro_manager, "coro_manager")
        self._callback_root = _require_string(service_root_url, "service_root_url")

    async def start_job(self, job: models.Job, objmeta: list[S3ObjectMeta]):
        """
        Start running a job. It is expected that the Job has been persisted to the data
        storage system and is in the created state.
        
        job - the job
        objmeta - the S3 object metadata for the files in the job.
        """
        if _not_falsy(job, "job").state != models.JobState.CREATED:
            raise InvalidJobStateError("Job must be in the created state")
        logr = logging.getLogger(__name__)
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
            # TODO LOGGING make the remote code log summary of results and upload and store
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
            # TODO LOGGING figure out how logging it going to work etc.
            logr.exception(f"Error starting download for job {job.id}")
            # TODO IMPORTANT ERRORHANDLING update job state to ERROR w/ message and don't raise
            raise e

    async def download_complete(self, job: models.AdminJobDetails):
        """
        Continue a job after the download is complete. The job is expected to be in the 
        download submitted state.
        """
        if _not_falsy(job, "job").state != models.JobState.DOWNLOAD_SUBMITTED:
            raise InvalidJobStateError("Job must be in the download submitted state")
        # TODO ERRHANDLING IMPORTANT pull the task from the SFAPI. If it a) doesn't exist or b) has
        #                  no errors, continue, otherwise put the job into an errored state.
        # TODO ERRHANDLING IMPORTANT upload the output file from the download task and check for
        #                  errors. If any exist, put the job into an errored state.
        # TDOO LOGGING Add any relevant logs from the task / download task output file in state
        #                  call
        await self._mongo.update_job_state(
            job.id,
            models.JobState.DOWNLOAD_SUBMITTED,
            models.JobState.JOB_SUBMITTING,
            timestamp.utcdatetime()
        )
        await self._coman.run_coroutine(self._submit_jaws_job(job))
    
    async def _submit_jaws_job(self, job: models.AdminJobDetails):
        logr = logging.getLogger(__name__)
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
            # TODO LOGGING figure out how logging it going to work etc.
            logr.exception(f"Error starting JAWS job for job {job.id}")
            # TODO IMPORTANT ERRORHANDLING update job state to ERROR w/ message and don't raise
            raise e

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
        # TODO ERRHANDLING IMPORTANT if in an error state, pull the erros.json file from the
        #                  JAWS job dir and add stderr / out to job record (what do to about huge
        #                  logs?) and set job to error 
        await self._mongo.update_job_state(
            job.id,
            models.JobState.JOB_SUBMITTED,
            models.JobState.UPLOAD_SUBMITTING,
            timestamp.utcdatetime()
        )
        await self._coman.run_coroutine(self._upload_files(job, jaws_info))
    
    async def _upload_files(self, job: models.AdminJobDetails, jaws_info: dict[str, Any]):
        logr = logging.getLogger(__name__)

        async def presign(output_files: list[Path]) -> list[S3PresignedPost]:
            root = job.job_input.output_dir
            # TODO PERF this parses the paths yet again
            # TODO RELIABILITY config / set expiration time
            paths = S3Paths([os.path.join(root, f) for f in output_files])
            return await self._s3ext.presign_post_urls(paths)
        
        try:
            # TODO PERF config / set concurrency
            # TODO LOGGING make the remote code log summary of results and upload and store
            # TODO LOGGING upload the container std* logs to S3 and store locations in job
            #              or maybe store in GFS? Should discuss with group how this should work
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
            # TODO LOGGING figure out how logging it going to work etc.
            logr.exception(f"Error starting upload for job {job.id}")
            # TODO IMPORTANT ERRORHANDLING update job state to ERROR w/ message and don't raise
            raise e

    async def upload_complete(self, job: models.AdminJobDetails):
        """
        Complete a job after the upload is complete. The job is expected to be in the 
        upload submitted state.
        """
        if _not_falsy(job, "job").state != models.JobState.UPLOAD_SUBMITTED:
            raise InvalidJobStateError("Job must be in the upload submitted state")
        # TODO ERRHANDLING IMPORTANT pull the task from the SFAPI. If it a) doesn't exist or b) has
        #                  no errors, continue, otherwise put the job into an errored state.
        # TODO ERRHANDLING IMPORTANT upload the output file from the upload task and check for
        #                  errors. If any exist, put the job into an errored state.
        # TDOO LOGGING Add any relevant logs from the task / download task output file in state
        #                  call. Alternatively, just add code to fetch them from NERSC rather
        #                  than storing them permanently. 99% of the time they'll be uninteresting
        await self._coman.run_coroutine(self._upload_complete(job))
    
    async def _upload_complete(self, job: models.AdminJobDetails):
        logr = logging.getLogger(__name__)
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
                outfiles.append(models.S3FileOutput(file=o.path, etag=o.e_tag))
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
            # TODO LOGGING figure out how logging it going to work etc.
            logr.exception(f"Error completing job {job.id}")
            # TODO IMPORTANT ERRORHANDLING update job state to ERROR w/ message and don't raise
            raise e
