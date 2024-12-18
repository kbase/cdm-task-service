"""
Manages submitting jobs.
"""

import uuid
from typing import Any

from cdmtaskservice import kb_auth
from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.image_remote_lookup import parse_image_name
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.timestamp import utcdatetime

class JobSubmit:
    """
    A manager for submitting CDM jobs.
    """
    
    def __init__(
        self,
        mongo: MongoDAO,
        s3client: S3Client,
        coro_manager: CoroutineWrangler,
        job_runners: dict[models.Cluster, Any],  # Make abstract class if necessary
    ):
        """
        mongo - a MongoDB DAO object.
        s3Client - an S3Client pointed at the S3 storage system to use.
        coro_manager - a coroutine manager.
        job_runners - a mapping of remote compute cluster to the job runner for that cluster.
        """
        self._s3 = _not_falsy(s3client, "s3client")
        self._mongo = _not_falsy(mongo, "mongo")
        self._coman = _not_falsy(coro_manager, "coro_manager")
        self._runners = _not_falsy(job_runners, "job_runners")
        
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
        parsedimage = parse_image_name(job_input.image)
        tag = parsedimage.tag
        if not parsedimage.tag and not parsedimage.digest:
            tag = "latest"
        image = await self._mongo.get_image(parsedimage.name, digest=parsedimage.digest, tag=tag)
        await self._s3.has_bucket(job_input.output_dir.split("/", 1)[0])
        paths = [f.file if isinstance(f, models.S3File) else f for f in job_input.input_files]
        # TODO PERF may wan to make concurrency configurable here
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


class ETagMismatchError(Exception):
    """ Thrown when an specified ETag does not match the expected ETag. """
