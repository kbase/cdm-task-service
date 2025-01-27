"""
DAO for MongoDB.
"""
# Could make an interface here so that mongo can be swapped out, but the chance that happens
# low enough to YAGNI the idea.

import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import IndexModel, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
from typing import Any

from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string


_INDEX_TAG = "UNIQUE_IMAGE_TAG_INDEX"
_INDEX_DIGEST = "UNIQUE_IMAGE_DIGEST_INDEX"


class MongoDAO:
    """
    DAO for MongoDB.
    """
    
    @classmethod
    async def create(cls, db: AsyncIOMotorDatabase):
        """
        Create the DAO.
        
        db - the MongoDB database.
        """
        mdao = MongoDAO(db)
        await mdao._create_indexes()
        return mdao
    
    def __init__(self, db: AsyncIOMotorDatabase):
        if db is None:  # dbs don't work as a bool
            raise ValueError("db is required")
        self._db = db
        self._col_images = self._db.images
        self._col_jobs = self._db.jobs
        self._col_refdata = self._db.refdata
        
    async def _create_indexes(self):
        # TODO DATAINTEGRITY test unique index builds throw an error if there are non-unique
        #                    docs in the DB. Want the server to not start until the indexes
        #                    are confirmed correct. See
        #                    https://www.mongodb.com/docs/manual/core/index-creation/#constraint-violations-during-index-build
        #                    Otherwise, create a polling loop to wait until indexes exist
        
        # NOTE that since there's two unique indexes means this collection can't be sharded
        # but it's likely to be very small so shouldn't be an issue
        await self._col_images.create_indexes([
            IndexModel(
                [(models.FLD_IMAGE_NAME, ASCENDING), (models.FLD_IMAGE_DIGEST, ASCENDING)],
                 unique=True,
                 name=_INDEX_DIGEST
            ),
            # Only allow one instance of a tag per image name to avoid confusion
            IndexModel(
                [(models.FLD_IMAGE_NAME, ASCENDING), (models.FLD_IMAGE_TAG, ASCENDING)],
                unique=True,
                sparse=True,  # tags are optional
                name=_INDEX_TAG
            ),
        ])
        # Only need and want a single unique index for jobs so they can be sharded
        await self._col_jobs.create_indexes([
            IndexModel([(models.FLD_JOB_ID, ASCENDING)], unique=True)
        ])
        # TODO REFDATA etag and file indexes
        #          should etag be unique?
        await self._col_refdata.create_indexes([
            IndexModel([(models.FLD_REFDATA_ID, ASCENDING)], unique=True)
        ])

    async def save_image(self, image: models.Image):
        """
        Save details about a Docker image.
        
        Only one record per digest or tag is allowed per image
        """
        _not_falsy(image, "image")
        try:
            await self._col_images.insert_one(image.model_dump())
        except DuplicateKeyError as e:
            if _INDEX_TAG in e.args[0]:
                raise ImageTagExistsError(f"The tag {image.tag} for image {image.name} "
                                          + "already exists in the system")
            elif _INDEX_DIGEST in e.args[0]:
                raise ImageDigestExistsError(f"The digest {image.digest} for image {image.name} "
                                             + "already exists in the system")
            else:
                # no way to test this, but just in case
                raise ValueError(f"Unexpected duplicate key collision for image {image}") from e

    async def get_image(self, name: str, digest: str | None = None, tag: str | None = None
    ) -> models.Image:
        """
        Get an image via a name and tag or digest. Either a tag or digest is required. If both
        are provided, the tag is ignored.
        """
        name = _require_string(name, "name")
        digest = digest.strip() if digest else None
        tag = tag.strip() if tag else None
        query = {models.FLD_IMAGE_NAME: name}
        if digest:
            query[models.FLD_IMAGE_DIGEST] = digest
            err = f"with digest {digest}"
        elif tag:
            query[models.FLD_IMAGE_TAG] = tag
            err = f"with tag '{tag}'"
        else:
            raise ValueError("A digest or tag is required")
        doc = await self._col_images.find_one(query)
        if not doc:
            raise NoSuchImageError(f"No image {name} {err} exists in the system.")
        return models.Image.model_construct(**doc)

    async def save_job(self, job: models.Job):
        """ Save a job. Job IDs are expected to be unique."""
        _not_falsy(job, "job")
        jobd = job.model_dump()
        jobi = jobd[models.FLD_JOB_JOB_INPUT]
        jobi[models.FLD_JOB_INPUT_RUNTIME] = jobi[models.FLD_JOB_INPUT_RUNTIME].total_seconds()
        # don't bother checking for duplicate key exceptions since the service is supposed
        # to ensure unique IDs
        await self._col_jobs.insert_one(jobd)

    async def get_job(
        self, job_id: str, as_admin: bool = False
    ) -> models.Job | models.AdminJobDetails:
        """
        Get a job by its ID.
        
        job_id - the job ID.
        as_admin - get additional details about the job.
        """
        doc = await self._col_jobs.find_one({models.FLD_JOB_ID: _require_string(job_id, "job_id")})
        if not doc:
            raise NoSuchJobError(f"No job with ID '{job_id}' exists")
        # TODO PERF build up the job piece by piece to skip S3 path validation
        return models.AdminJobDetails(**doc) if as_admin else models.Job(**doc)

    async def _update_job_state(
        self,
        job_id: str,
        state: models.JobState,
        time: datetime.datetime,
        push: dict[str, Any] | None = None,
        set_: dict[str, Any] | None = None,
        current_state: models.JobState | None = None,
    ):
        query = {models.FLD_JOB_ID: _require_string(job_id, "job_id")}
        if current_state:
            query[models.FLD_JOB_STATE] = current_state.value
        res = await self._col_jobs.update_one(
            query,
            {
                "$push": (push if push else {}) | {
                    models.FLD_JOB_TRANS_TIMES:
                          (_not_falsy(state, "state").value, _not_falsy(time, "time")
                    )
                },
                "$set": (set_ if set_ else {}) | {models.FLD_JOB_STATE: state.value}
            },
        )
        if not res.matched_count:
            cs = f"in state {current_state.value} " if current_state else ""
            raise NoSuchJobError(
                f"No job with ID '{job_id}' {cs}exists"
            )
    
    async def update_job_state(
        self,
        job_id: str,
        current_state: models.JobState,
        state: models.JobState,
        time: datetime.datetime,
    ):
        """
        Update the job state.
        
        job_id - the job ID.
        current_state - the expected current state of the job. If the job is not in this state
            an error is thrown.
        state - the new state for the job.
        time - the time at which the job transitioned to the new state.
        """
        await self._update_job_state(job_id, state, time, current_state=current_state)

    _FLD_NERSC_DL_TASK = f"{models.FLD_JOB_NERSC_DETAILS}.{models.FLD_NERSC_DETAILS_DL_TASK_ID}"
    
    async def add_NERSC_download_task_id(
        self,
        job_id: str,
        task_id: str,
        current_state: models.JobState,
        state: models.JobState,
        time: datetime.datetime
    ):
        """
        Add a download task_id to the NERSC section of a job and update the state.
        
        Arguments are as update_job_state except for the addition of:
        
        task_id - the NERSC task ID.
        """
        # may need to make this more generic where the cluster is passed in and mapped to
        # a job structure location or something if we support more than NERSC
        await self._update_job_state(job_id, state, time, current_state=current_state, push={
            self._FLD_NERSC_DL_TASK: _require_string(task_id, "task_id")
        })

    _FLD_JAWS_RUN_ID = f"{models.FLD_JOB_JAWS_DETAILS}.{models.FLD_JAWS_DETAILS_RUN_ID}"

    async def add_JAWS_run_id(
        self,
        job_id: str,
        run_id: str,
        current_state: models.JobState,
        state: models.JobState,
        time: datetime.datetime
    ):
        """
        Add a run ID to the JAWS section of a job and update the state.
        
        Arguments are as update_job_state except for the addition of:
        
        run_id - the JAWS run ID.
        """
        # may need to make this more generic where the cluster is passed in and mapped to
        # a job structure location or something if we support more than NERSC
        await self._update_job_state(job_id, state, time, current_state=current_state, push={
            self._FLD_JAWS_RUN_ID: _require_string(run_id, "run_id")
        })

    _FLD_NERSC_UL_TASK = f"{models.FLD_JOB_NERSC_DETAILS}.{models.FLD_NERSC_DETAILS_UL_TASK_ID}"
    
    async def add_NERSC_upload_task_id(
        self,
        job_id: str,
        task_id: str,
        current_state: models.JobState,
        state: models.JobState,
        time: datetime.datetime
    ):
        """
        Add an upload task_id to the NERSC section of a job and update the state.
        
        Arguments are as update_job_state except for the addition of:
        
        task_id - the NERSC task ID.
        """
        # may need to make this more generic where the cluster is passed in and mapped to
        # a job structure location or something if we support more than NERSC
        await self._update_job_state(job_id, state, time, current_state=current_state, push={
            self._FLD_NERSC_UL_TASK: _require_string(task_id, "task_id")
        })
    
    _FLD_NERSC_LOG_UL_TASK = (
        f"{models.FLD_JOB_NERSC_DETAILS}.{models.FLD_NERSC_DETAILS_LOG_UL_TASK_ID}"
    )
    
    async def add_NERSC_log_upload_task_id(
        self,
        job_id: str,
        task_id: str,
        current_state: models.JobState,
        state: models.JobState,
        time: datetime.datetime
    ):
        """
        Add a log upload task_id to the NERSC section of a job and update the state.
        
        Arguments are as update_job_state except for the addition of:
        
        task_id - the NERSC task ID.
        """
        # may need to make this more generic where the cluster is passed in and mapped to
        # a job structure location or something if we support more than NERSC
        await self._update_job_state(job_id, state, time, current_state=current_state, push={
            self._FLD_NERSC_LOG_UL_TASK: _require_string(task_id, "task_id")
        })
    
    async def add_output_files_to_job(
        self,
        job_id: str,
        output: list[models.S3File],
        current_state: models.JobState,
        state: models.JobState,
        time: datetime.datetime
    ):
        """
        Add output files to a job and update the state.
        
        Arguments are as update_job_state except for the addition of:
        
        output - the output files.
        """
        out = [o.model_dump() for o in _not_falsy(output, "output")]
        await self._update_job_state(
            job_id, state, time, current_state=current_state, set_={models.FLD_JOB_OUTPUTS: out}
        )

    async def set_job_error(
        self,
        job_id: str,
        user_error: str,
        admin_error: str,
        state: models.JobState,
        time: datetime.datetime,
        traceback: str | None = None,
        logpath: str | None = None,
    ):
        """
        Put the job into an error state.
        
        job_id - the job ID.
        user_error - an error message targeted towards a service user.
        admin_error - an error message targeted towards a service admin.
        state - the new state for the job.
        time - the time at which the job transitioned to the new state.
        traceback - the error traceback.
        logpath - the path to any logs for the job.
        """
        # TODO RETRIES will need to clear the error fields when attempting a retry
        await self._update_job_state(job_id, state, time, set_={
            models.FLD_JOB_ERROR: user_error,
            models.FLD_JOB_ADMIN_ERROR: admin_error,
            models.FLD_JOB_TRACEBACK: traceback,
            models.FLD_JOB_LOGPATH: logpath
        })


    async def save_refdata(self, refdata: models.ReferenceData):
        """ Save reference data state. Reference data IDs are expected to be unique."""
        _not_falsy(refdata, "refdata")
        # don't bother checking for duplicate key exceptions since the service is supposed
        # to ensure unique IDs
        await self._col_refdata.insert_one(refdata.model_dump())


    async def get_refdata(
        # TODO REFDATA make use of the as admin toggle
        self, refdata_id: str, as_admin: bool = False
    ) -> models.ReferenceData:
        """
        Get reference data by its ID.
        
        refdata_id - the reference data ID.
        as_admin - get additional details about the reference data.
        """
        doc = await self._col_refdata.find_one(
            {models.FLD_REFDATA_ID: _require_string(refdata_id, "refdata_id")}
        )
        if not doc:
            raise NoSuchJobError(f"No reference data with ID '{refdata_id}' exists")
        return models.ReferenceData(**doc)

    async def _update_refdata_state(
        self,
        cluster: models.Cluster,
        refdata_id: str,
        state: models.ReferenceDataState,
        time: datetime.datetime,
        push: dict[str, Any] | None = None,
        set_: dict[str, Any] | None = None,
        current_state: models.JobState | None = None,
    ):
        sub = f"{models.FLD_REFDATA_STATUSES}."
        subs = f"{sub}$."
        query = {
            models.FLD_REFDATA_ID: _require_string(refdata_id, "refdata_id"),
            f"{sub}{models.FLD_REFDATA_CLUSTER}": _not_falsy(cluster, "cluster").value
        }
        set_ = {f"{subs}{k}": v for k, v in set_.items()} if set_ else {}
        push = {f"{subs}{k}": v for k, v in push.items()} if push else {}
        if current_state:
            query[f"{sub}{models.FLD_REFDATA_STATE}"] = current_state.value
        res = await self._col_refdata.update_one(
            query,
            {
                "$push": push | {f"{subs}{models.FLD_JOB_TRANS_TIMES}":
                        (_not_falsy(state, "state").value, _not_falsy(time, "time"))
                },
                "$set": set_ | {f"{subs}{models.FLD_REFDATA_STATE}": state.value}
            },
        )
        if not res.matched_count:
            cs = f"in state {current_state.value} " if current_state else ""
            raise NoSuchReferenceDataError(
                f"No reference data with ID '{refdata_id}' for cluster {cluster.value} {cs}exits"
            )

    async def update_refdata_state(
        self,
        cluster: models.Cluster,
        refdata_id: str,
        current_state: models.JobState,
        state: models.JobState,
        time: datetime.datetime,
    ):
        """
        Update the reference data state.
        
        cluster - the cluster to which this update applies.
        refdata_id - the reference data ID.
        current_state - the expected current state of the reference data. If the reference
            data is not in this state an error is thrown.
        state - the new state for the reference data.
        time - the time at which the reference data transitioned to the new state.
        """
        await self._update_refdata_state(
            cluster, refdata_id, state, time, current_state=current_state
        )

    async def add_NERSC_refdata_download_task_id(
        self,
        cluster: models.Cluster,
        refdata_id: str,
        task_id: str,
        current_state: models.ReferenceDataState,
        state: models.ReferenceDataState,
        time: datetime.datetime
    ):
        """
        Add a task_id to the NERSC section of a reference data download and update the state.
        
        Arguments are as update_refdata_state except for the addition of:
        
        task_id - the NERSC task ID.
        """
        # may need to make this more generic where the cluster is passed in and mapped to
        # a job structure location or something if we support more than NERSC
        await self._update_refdata_state(
            cluster,
            refdata_id,
            state,
            time,
            current_state=current_state,
            push={models.FLD_REFDATA_NERSC_DL_TASK_ID: _require_string(task_id, "task_id")}
        )

    async def set_refdata_error(
        self,
        cluster: models.Cluster,
        refdata_id: str,
        user_error: str,
        admin_error: str,
        state: models.ReferenceDataState,
        time: datetime.datetime,
        traceback: str | None = None,
    ):
        """
        Put the reference data operation into an error state.
        
        cluster - the cluster to which this update applies.
        refdata_id - the reference data ID.
        user_error - an error message targeted towards a service user.
        admin_error - an error message targeted towards a service admin.
        state - the new state for the reference data operation.
        time - the time at which the operation transitioned to the new state.
        traceback - the error traceback.
        """
        # TODO RETRIES will need to clear the error fields when attempting a retry
        await self._update_refdata_state(cluster, refdata_id, state, time, set_={
            models.FLD_REFDATA_ERROR: user_error,
            models.FLD_REFDATA_ADMIN_ERROR: admin_error,
            models.FLD_REFDATA_TRACEBACK: traceback,
        })


class NoSuchImageError(Exception):
    """ The image does not exist in the system. """


class NoSuchJobError(Exception):
    """ The job does not exist in the system. """


class NoSuchReferenceDataError(Exception):
    """ The reference data does not exist in the system. """


class ImageTagExistsError(Exception):
    """ The tag for the image already exists in the system. """


class ImageDigestExistsError(Exception):
    """ The digest for the image already exists in the system. """
