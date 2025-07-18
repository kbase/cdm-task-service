"""
Manages job state.
"""

import datetime
import logging
import uuid

from cdmtaskservice import kb_auth
from cdmtaskservice import logfields
from cdmtaskservice import models
from cdmtaskservice import sites
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
from cdmtaskservice.mongo import MongoDAO, IllegalAdminMetaError
from cdmtaskservice.refdata import Refdata
from cdmtaskservice.s3.client import S3Client, S3BucketInaccessibleError
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.timestamp import utcdatetime
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier


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
        kafka: KafkaNotifier,
        refdata: Refdata,
        coro_manager: CoroutineWrangler,
        flow_manager: JobFlowManager,
        log_bucket: str,
        job_max_cpu_hours: float,
        test_mode: bool = False,
    ):
        """
        Create the job state manager.
        
        mongo - a MongoDB DAO object.
        s3Client - an S3Client pointed at the S3 storage system to use.
        images - a handler for getting images.
        kafka - a Kafka notifier.
        refdata - a manager for reference data.
        coro_manager - a coroutine manager.
        flow_manager- the job flow manager.
        log_bucket - the bucket in which logs are stored. Disallowed for writing for other cases.
        job_max_cpu_hours - the maximum CPU hours allows for a job on submit.
        test_mode - if true, availablity of job flows will not be checked and jobs will not be
            submitted.
        """
        self._mongo = _not_falsy(mongo, "mongo")
        self._s3 = _not_falsy(s3client, "s3client")
        self._images = _not_falsy(images, "images")
        # Maybe should make a wrapper around mongo & kafka for updates...?
        self._kafka = _not_falsy(kafka, "kafka")
        self._ref = _not_falsy(refdata, "refdata")
        self._coman = _not_falsy(coro_manager, "coro_manager")
        self._flowman = _not_falsy(flow_manager, "flow_manager")
        self._logbuk = _require_string(log_bucket, "log_bucket")
        self._cpu_hrs = _check_num(job_max_cpu_hours, "job_max_cpu_hours")
        self._test_mode = test_mode
        
    async def submit(self, job_input: models.JobInput, user: kb_auth.KBaseUser) -> str:
        """
        Submit a job.
        
        job_input - the input for the job.
        user - the username of the user submitting the job.
        
        Returns the opaque job ID.
        """
        # This function is right on the edge of needing to be split up for size
        _not_falsy(job_input, "job_input")
        _not_falsy(user, "user")
        self._check_site_limits(job_input)
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
        if not self._test_mode:
            # check the flow is available before we make any changes
            flow = self._flowman.get_flow(job_input.cluster)
        ji = job_input.model_copy(update={"input_files": new_input})
        job_id = str(uuid.uuid4())  # TODO TEST for testing we'll need to set up a mock for this
        # TODO TEST will need a way to mock out timestamps
        update_time = utcdatetime()
        # TODO TEST will need to mock out uuid
        trans_id = str(uuid.uuid4())
        job = models.AdminJobDetails(
            id=job_id,
            job_input=ji,
            user=user.user,
            image=image,
            input_file_count=len(new_input),
            state=models.JobState.CREATED,
            transition_times=[models.AdminJobStateTransition(
                state=models.JobState.CREATED,
                time=update_time,
                trans_id=trans_id,
                notif_sent=False,
            )]
        )
        async def cb():
            await self._mongo.job_update_sent(job_id, trans_id)
        await self._mongo.save_job(job)
        await self._kafka.update_job_state(
            job_id, models.JobState.CREATED, update_time, trans_id, callback=cb()
        )
        if not self._test_mode:
            # Pass in the meta to avoid potential race conditions w/ etag changes
            await self._coman.run_coroutine(flow.start_job(job, meta))
        return job_id

    def _check_site_limits(self, job_input: models.JobInput):
        site = sites.CLUSTER_TO_SITE[job_input.cluster]
        if job_input.cpus > site.cpus_per_node:
            raise IllegalParameterError(
                f"The maximum number of CPUs for site {job_input.cluster.value} is "
                f"{site.cpus_per_node} vs {job_input.cpus} submitted"
            )
        if (rt_max := job_input.runtime.total_seconds() / 60) > site.max_runtime_min:
            raise IllegalParameterError(
                f"The maximum runtime for site {job_input.cluster.value} is "
                f"{site.max_runtime_min} minutes vs {rt_max} submitted"
            )
        if (gb := int(job_input.memory) / 1_000_000_000) > site.memory_per_node_gb:
            raise IllegalParameterError(
                f"The maximum memory for site {job_input.cluster.value} is "
                f"{site.memory_per_node_gb}GB vs {gb}GB submitted"
            )

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
                    f"The S3 path '{m.path}' does not have a CRC64/NVME checksum"
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
        user: str,  # can't be a KBaseUser since may be the system calling the method
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

    async def get_job_status(
        self,
        job_id: str,
        user: kb_auth.KBaseUser,
    ) -> models.JobStatus:
        """
        Get minimal information about a job's status based on the job's ID.
        If the provided user doesn't match the job's owner, an error is thrown.
        
        job_id - the job ID.
        user - the user requesting the job.
        """
        _not_falsy(user, "user")
        job = await self._mongo.get_job_status(_require_string(job_id, "job_id"))
        if job.user != user.user:
            # reveals the job ID exists in the system but I don't see a problem with that
            raise UnauthorizedError(f"User {user.user} may not access job {job_id}")
        logging.getLogger(__name__).info(
            f"User {user.user} accessed job {job_id}'s status",
            extra={logfields.JOB_ID: job_id}
        )
        return job
    
    async def list_jobs(
        self,
        # can't be a KBaseUser since it may be provided by an admin as a parameter
        user: str | None = None,
        state: models.JobState | None = None,
        after: datetime.datetime | None = None,
        before: datetime.datetime | None = None,
        limit: int = 1000
    ) -> list[models.JobPreview]:
        """
        List jobs in the system.
        
        user - filter the jobs by a specific user.
        state - filter the jobs by the given state.
        after - filter jobs to jobs that entered the current state after the given time, inclusive.
        before - filter jobs to jobs that entered the current state before the given time,
            exclusive.
        limit - the maximum number of jobs to return between 1 and 1000.
        """
        # mostly a pass through method
        limit = 1000 if limit is None else limit
        if limit < 1 or limit > 1000:
            raise IllegalParameterError("Limit must be between 1 and 1000 inclusive")
        return await self._mongo.list_jobs(
            user=user,
            state=state,
            after=after,
            before=before,
            limit=limit,
        )

    async def update_job_admin_meta(
        self,
        job_id: str,
        admin: kb_auth.KBaseUser,
        set_fields: dict[str, str | int | float] | None = None,
        unset_keys: set[str] | None = None,
    ):
        """
        Updates the admin metadata for the specified job. Only admins should be allowed to
        call this method.
    
        Does not affect any extant keys other than those specified in the function input.
    
        job_id - the ID of the job.
        admin - the administrator altering the metadata
        set_fields - keys and their values to set in the admin metadata.
        unset_keys - keys to remove from the admin metadata.
    
        If both `set_fields` and `unset_keys` are None or empty, an error is thrown.
        """
        _not_falsy(admin, "admin")
        if not set_fields and not unset_keys:
            raise IllegalParameterError(
                "At least one of set_fields or unset_keys must contain keys to alter"
            )
        try:
            await self._mongo.update_job_admin_meta(
                _require_string(job_id, "job_Id"), set_fields, unset_keys
            )
            logging.getLogger(__name__).info(
                f"Admin {admin.user} updated job {job_id}'s admin metadata",
                # don't log the changes could be large
                extra={logfields.JOB_ID: job_id}
            )
        except IllegalAdminMetaError as e:
            raise IllegalParameterError(str(e)) from e
