"""
Manages running jobs at NERSC using the JAWS system.
"""

import logging

from cdmtaskservice import models
from cdmtaskservice import timestamp
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.callback_url_paths import get_download_complete_callback
from cdmtaskservice.job_state import JobState
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.nersc.manager import NERSCManager
from cdmtaskservice.s3.client import S3Client, S3ObjectMeta
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
        job_state: JobState,
        mongodao: MongoDAO,
        s3_client: S3Client,
        s3_external_client: S3Client,
        jaws_token: str,
        jaws_group: str,
        service_root_url: str,
        s3_insecure_ssl: bool = False,
    ):
        """
        Create the runner.
        
        nersc_manager - the NERSC manager.
        job_state - the job state manager.
        mongodao - the Mongo DAO object.
        s3_client - an S3 client pointed to the data stores.
        s3_external_client - an S3 client pointing to an external URL for the S3 data stores
            that may not be accessible from the current process, but is accessible to remote
            processes at NERSC.
        jaws_token - a token for the JGI JAWS system.
        jaws_group - the group to use for running JAWS jobs.
        service_root_url - the URL of the service root, used for constructing service callbacks.
        s3_insecure_url - whether to skip checking the SSL certificate for the S3 instance,
            leaving the service open to MITM attacks.
        """
        self._nman = _not_falsy(nersc_manager, "nersc_manager")
        self._jstate = _not_falsy(job_state, "job_state")
        self._mongo = _not_falsy(mongodao, "mongodao")
        self._s3 = _not_falsy(s3_client, "s3_client")
        self._s3ext = _not_falsy(s3_external_client, "s3_external_client")
        self._s3insecure = s3_insecure_ssl
        self._jtoken = _require_string(jaws_token, "jaws_token")
        self._jgroup = _require_string(jaws_group, "jaws_group")
        self._callback_root = _require_string(service_root_url, "service_root_url")

    async def start_job(self, job: models.Job, objmeta: list[S3ObjectMeta]):
        """
        Start running a job. It is expected that the Job has been persisted to the data
        storage system and is in the created state.
        
        job - the job
        objmeta - the S3 object metadata for the files in the job.
        """
        if _not_falsy(job, "job").state != models.JobState.CREATED:
            raise ValueError("job must be in the created state")
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
            # TODO NOW how get remote paths at next step? 
            task_id = await self._nman.download_s3_files(
                job.id, objmeta, presigned, callback_url, insecure_ssl=self._s3insecure
            )
            # Hmm. really this should go through job state but that seems pointless right now.
            # May need to refactor this and the mongo method later to be more generic to
            # remote cluster and have job_state handle choosing the correct mongo method & params
            # to run
            await self._mongo.add_NERSC_download_task_id(
                # TODO TEST will need a way to mock out timestamps
                job.id,
                task_id,
                models.JobState.CREATED,
                models.JobState.UPLOAD_SUBMITTED,
                timestamp.utcdatetime(),
            )
        except Exception as e:
            # TODO LOGGING figure out how logging it going to work etc.
            logr.exception(f"Error starting download for job {job.id}")
            # TODO IMPORTANT ERRORHANDLING update job state to ERROR w/ message and don't raise
            raise e
