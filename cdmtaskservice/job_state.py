"""
Manages submitting jobs and getting and updating their current state.
"""

import uuid

from cdmtaskservice import kb_auth
from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths

class JobState:
    """
    A manager for CDM job state.
    """
    
    def __init__(self, s3client: S3Client):  # TODO MONOGO client
        """
        s3Client - an S3Client pointed at the S3 storage system to use.
        """
        self._s3 = _not_falsy(s3client, "s3client")
        
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
        print(ji)  # TODO JOBSUBMIT remove
        # TODO JOBSUBMIT check image
        # TODO JOBSUBMIT check container is allowed
        # TDDO JOBSUBMIT if reference data is required, is it staged?
        # TODO JOBSUBMIT save Job model in Mongo
        
        return job_id


class ETagMismatchError(Exception):
    """ Thrown when an specified ETag does not match the expected ETag. """