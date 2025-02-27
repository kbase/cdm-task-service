"""
Manages job state.
"""

import logging
import uuid

from cdmtaskservice import kb_auth
from cdmtaskservice import logfields
from cdmtaskservice import models
from cdmtaskservice.arg_checkers import (
    not_falsy as _not_falsy,
    require_string as _require_string,
    check_num as _check_num
)
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import (
    ChecksumMismatchError,
    IllegalParameterError,
    InvalidReferenceDataStateError,
    UnauthorizedError,
)
from cdmtaskservice.images import Images
from cdmtaskservice.jobflows.flowmanager import JobFlowManager
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.refdata import Refdata
from cdmtaskservice.s3.client import S3Client, S3BucketInaccessibleError
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.timestamp import utcdatetime


class JobState:
    """
    A manager for CDM job state.
    """
    
    def __init__(
        # getting too many args here...
        self,
        mongo: MongoDAO,
        s3client: S3Client,
        images: Images,
        refdata: Refdata,
        coro_manager: CoroutineWrangler,
        flow_manager: JobFlowManager,
        log_bucket: str,
        job_max_cpu_hours: float,
    ):
        """
        Create the job state manager.
        
        mongo - a MongoDB DAO object.
        s3Client - an S3Client pointed at the S3 storage system to use.
        images - a handler for getting images.
        refdata - a manager for reference data.
        coro_manager - a coroutine manager.
        flow_manager- the job flow manager.
        log_bucket - the bucket in which logs are stored. Disallowed for writing for other cases.
        job_max_cpu_hours - the maximum CPU hours allows for a job on submit.
        """
        self._mongo = _not_falsy(mongo, "mongo")
        self._s3 = _not_falsy(s3client, "s3client")
        self._images = _not_falsy(images, "images")
        self._ref = _not_falsy(refdata, "refdata")
        self._coman = _not_falsy(coro_manager, "coro_manager")
        self._flowman = _not_falsy(flow_manager, "flow_manager")
        self._logbuk = _require_string(log_bucket, "log_bucket")
        self._cpu_hrs = _check_num(job_max_cpu_hours, "job_max_cpu_hours")
        
    async def submit(self, job_input: models.JobInput, user: kb_auth.KBaseUser) -> str:
        """
        Submit a job.
        
        job_input - the input for the job.
        user - the username of the user submitting the job.
        
        Returns the opaque job ID.
        """
        _not_falsy(job_input, "job_input")
        _not_falsy(user, "user")
        compute_time = job_input.get_total_compute_time_sec() / 3600
        if compute_time > self._cpu_hrs:
            raise IllegalParameterError(
                f"Job compute time of {compute_time} CPU hours is greater than the limit of "
                + f"{self._cpu_hrs}"
        )
        # Could parallelize these ops but probably not worth it
        image = await self._images.get_image(job_input.image)
        await self._check_refdata(job_input, image)
        bucket = job_input.output_dir.split("/", 1)[0]
        if bucket == self._logbuk:
            raise S3BucketInaccessibleError(f"Jobs may not write to bucket {self._logbuk}")
        await self._s3.is_bucket_writeable(bucket)
        new_input, meta = await self._check_and_update_files(job_input)
        # check the flow is available before we make any changes
        flow = self._flowman.get_flow(job_input.cluster)
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
        await self._mongo.save_job(job)
        # Pass in the meta to avoid potential race conditions w/ etag changes
        await self._coman.run_coroutine(flow.start_job(job, meta))
        return job_id

    async def _check_and_update_files(self, job_input: models.JobInput):
        paths = [
            f.file if isinstance(f, models.S3FileWithDataID) else f
                 for f in job_input.input_files
        ]
        # TODO PERF may want to make concurrency configurable here
        # TODO PERF this checks the file path syntax again, consider some way to avoid
        meta = await self._s3.get_object_meta(S3Paths(paths))
        new_input = []
        for m, f in zip(meta, job_input.input_files):
            if not m.crc64nvme:
                raise IllegalParameterError(
                    f"The S3 path '{f.file}' does not have a CRC64/NVME checksum"
                )
            data_id = None
            if isinstance(f, models.S3FileWithDataID):
                data_id = f.data_id
                if f.crc64nvme and f.crc64nvme != m.crc64nvme:
                    raise ChecksumMismatchError(
                        f"The expected CRC64/NMVE checksum '{f.crc64nvme}' for the path "
                        + f"'{f.file}' does not match the actual checksum '{m.crc64nvme}'"
                    )
            # no need to validate the path again
            new_input.append(models.S3FileWithDataID.model_construct(
                file=m.path, crc64nvme=m.crc64nvme, data_id=data_id)
            )
        return new_input, meta

    async def _check_refdata(self, job_input: models.JobInput, image: models.Image):
        if not image.refdata_id:
            return
        if not job_input.params.refdata_mount_point:
            raise IllegalParameterError(
                "Image for job requires reference data but no refdata mount point "
                + "is specified in the job input parameters"
        )
        refdata = await self._ref.get_refdata_by_id(image.refdata_id)
        refstatus = refdata.get_status_for_cluster(job_input.cluster)
        if refstatus.state != models.ReferenceDataState.COMPLETE:
            raise InvalidReferenceDataStateError(
                f"Reference data '{refdata.id} required for job is not yet staged at "
                + f"remote compute environment {job_input.cluster.value}"
        )

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
        logging.getLogger(__name__).info(msg, extra={logfields.JOB_ID: job_id})
        return job
