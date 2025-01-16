"""
Manages job state.
"""

import logging
import uuid
from typing import Any

from cdmtaskservice import models
from cdmtaskservice import kb_auth
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import UnauthorizedError
from cdmtaskservice.images import Images
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.s3.client import S3Client, S3BucketInaccessibleError
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.timestamp import utcdatetime

class JobState:
    """
    A manager for CDM job state.
    """
    
    def __init__(
        self,
        mongo: MongoDAO,
        s3client: S3Client,
        images: Images,
        coro_manager: CoroutineWrangler,
        job_runners: dict[models.Cluster, Any],  # Make abstract class if necessary
        log_bucket: str,
    ):
        """
        mongo - a MongoDB DAO object.
        s3Client - an S3Client pointed at the S3 storage system to use.
        images - a handler for getting images.
        coro_manager - a coroutine manager.
        job_runners - a mapping of remote compute cluster to the job runner for that cluster.
        log_bucket - the bucket in which logs are stored. Disallowed for writing for other cases.
        """
        self._images = _not_falsy(images, "images")
        self._s3 = _not_falsy(s3client, "s3client")
        self._mongo = _not_falsy(mongo, "mongo")
        self._coman = _not_falsy(coro_manager, "coro_manager")
        self._runners = _not_falsy(job_runners, "job_runners")
        self._logbuk = _require_string(log_bucket, "log_bucket")
        
    async def submit(self, job_input: models.JobInput, user: kb_auth.KBaseUser) -> str:
        """
        Submit a job.
        
        job_input - the input for the job.
        user - the username of the user submitting the job.
        
        Returns the opaque job ID.
        """
        _not_falsy(job_input, "job_input")
        _not_falsy(user, "user")
        # Could parallelize these ops but probably not worth it
        image = await self._images.get_image(job_input.image)
        bucket = job_input.output_dir.split("/", 1)[0]
        if bucket == self._logbuk:
            raise S3BucketInaccessibleError(f"Jobs may not write to bucket {self._logbuk}")
        await self._s3.is_bucket_writeable(bucket)
        paths = [f.file if isinstance(f, models.S3File) else f for f in job_input.input_files]
        # TODO PERF may want to make concurrency configurable here
        # TODO PERF this checks the file path syntax again, consider some way to avoid
        meta = await self._s3.get_object_meta(S3Paths(paths))
        new_input = []
        for m, f in zip(meta, job_input.input_files):
            data_id = None
            if isinstance(f, models.S3File):
                data_id = f.data_id
                if f.etag and f.etag != m.e_tag:
                    raise ETagMismatchError(
                        f"The expected ETag '{f.etag} for the path '{f.file}' does not match "
                        + f"the actual ETag '{m.e_tag}'"
                    )
            # no need to validate the path again
            new_input.append(models.S3File.model_construct(
                file=m.path, etag=m.e_tag, data_id=data_id)
            )
        ji = job_input.model_copy(update={"input_files": new_input})
        job_id = str(uuid.uuid4())  # TODO TEST for testing we'll need to set up a mock for this
        job = models.Job(
            id=job_id,
            job_input=ji,
            user=user.user,
            image=image,
            state=models.JobState.CREATED,
            transition_times=[
                (models.JobState.CREATED, utcdatetime())
            ]
        )
        # TDDO JOBSUBMIT if reference data is required, is it staged?
        await self._mongo.save_job(job)
        # Pass in the meta to avoid potential race conditions w/ etag changes
        await self._coman.run_coroutine(self._runners[job.job_input.cluster].start_job(job, meta))
        return job_id

    async def get_job(
        self,
        job_id: str,
        user: str,
        as_admin: bool = False
    ) -> models.Job | models.AdminJobDetails:
        """
        Get a job based on its ID. If the provided user doesn't match the job's owner,
        an error is thrown.
        
        job_id - the job ID
        user - the user requesting the job.
        as_admin - True if the user should always have access to the job and should access
            additional job details.
        """
        _require_string(user, "user")
        job = await self._mongo.get_job(_require_string(job_id, "job_id"), as_admin=as_admin)
        if not as_admin and job.user != user:
            # reveals the job ID exists in the system but I don't see a problem with that
            raise UnauthorizedError(f"User {user} may not access job {job_id}")
        msg = f"User {user} accessed job {job_id}"
        if as_admin:
            msg = f"Admin user {user} accessed {job.user}'s job {job_id}"
        logging.getLogger(__name__).info(msg)
        return job


class ETagMismatchError(Exception):
    """ Thrown when an specified ETag does not match the expected ETag. """
