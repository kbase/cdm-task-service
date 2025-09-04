"""
DAO for MongoDB.
"""
# Could make an interface here so that mongo can be swapped out, but the chance that happens
# low enough to YAGNI the idea.

import datetime
from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection
from pymongo import IndexModel, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
from pymongo.results import DeleteResult
from typing import Any, Awaitable, Callable, Coroutine, Iterable

from cdmtaskservice import models
from cdmtaskservice import sites
from cdmtaskservice.arg_checkers import (
    not_falsy as _not_falsy,
    require_string as _require_string,
    check_num as _check_num,
    verify_aware_datetime
)
from cdmtaskservice.update_state import JobUpdate, UpdateField, RefdataUpdate


_INDEX_TAG = "UNIQUE_IMAGE_TAG_INDEX"
_INDEX_DIGEST = "UNIQUE_IMAGE_DIGEST_INDEX"

_FLD_MONGO_ID = "_id"

_FLD_UPDATE_TIME = "_update_time"  # mark as internal field
_FLD_TRANS_TIME_SEND = (
    f"{models.FLD_COMMON_TRANS_TIMES}.{models.FLD_JOB_STATE_TRANSITION_NOTIFICATION_SENT}"
)
_FLD_RETRY_ATTEMPT = "_retry"  # not really used for now, but preparation for later


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
        self._col_sites = self._db.sites
        self._col_images = self._db.images
        self._col_jobs = self._db.jobs
        self._col_subjobs = self._db.subjobs
        self._col_refdata = self._db.refdata
        self._setup_field_mappings()
        
    async def _create_indexes(self):
        # Tested that trying to create a unique index on a key that is not unique within documents
        # in the collection causes an error to be thrown 
        
        await self._col_sites.create_indexes([IndexModel([("site", ASCENDING)], unique=True)])
        
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
        timefield = f"{models.FLD_COMMON_TRANS_TIMES}.{models.FLD_COMMON_STATE_TRANSITION_TIME}"
        # Only need and want a single unique index for jobs so they can be sharded
        await self._col_jobs.create_indexes([
            IndexModel([(models.FLD_COMMON_ID, ASCENDING)], unique=True),
            # find & sort jobs by transition time (admin only) (job sharing may require changes)1
            IndexModel([(_FLD_UPDATE_TIME, DESCENDING)]),
            # find jobs by current state and state transition time (admin only)
            IndexModel([(models.FLD_COMMON_STATE, ASCENDING), (_FLD_UPDATE_TIME, DESCENDING)]),
            # find jobs by user and state transition time
            IndexModel([(models.FLD_JOB_USER, ASCENDING), (_FLD_UPDATE_TIME, DESCENDING)]),
            # find jobs by user, current state and state transition time
            IndexModel([
                (models.FLD_JOB_USER, ASCENDING),
                (models.FLD_COMMON_STATE, ASCENDING),
                (_FLD_UPDATE_TIME, DESCENDING)
            ]),
            # find jobs with unsent updates
            IndexModel(
                [(timefield, DESCENDING)],
                partialFilterExpression={_FLD_TRANS_TIME_SEND: False}
            ),
        ])
        statefield = f"{models.FLD_COMMON_TRANS_TIMES}.{models.FLD_COMMON_STATE_TRANSITION_STATE}"
        retryfield = f"{models.FLD_COMMON_TRANS_TIMES}.{_FLD_RETRY_ATTEMPT}"
        await self._col_subjobs.create_indexes([
            # Ensure there's no duplicate subjobs
            IndexModel(
                [(models.FLD_COMMON_ID, ASCENDING), (models.FLD_SUBJOB_ID, ASCENDING)],
                unique=True
            ),
            # Find subjobs in a specific state on a specific retry attempt
            # Could make it unique as there should only be one state per retry, but that would
            # prevent sharding the collection - which seems unlikely but there's no need to 
            # block the ability when a unique index isn't needed
            IndexModel([
                (models.FLD_COMMON_ID, ASCENDING),
                (statefield, ASCENDING),
                (retryfield, ASCENDING)
            ]),
        ])
        await self._col_refdata.create_indexes([
            IndexModel([(models.FLD_COMMON_ID, ASCENDING)], unique=True),
            # Non unique as files can be overwritten
            IndexModel([(models.FLD_REFDATA_FILE, ASCENDING)]),
        ])

    async def initialize_sites(self, sites: Iterable[sites.Cluster]):
        """
        Add sites to the database only if they don't already exist,
        and set them to active. Existing site states are left unchanged.
        """
        # Few sites, no need to parallelize
        for site in _not_falsy(sites, "sites"):
            _not_falsy(site, "site")
            await self._col_sites.update_one(
                {"site": site.value},
                {"$setOnInsert": {"site": site.value, "active": True}},
                upsert=True,
            )

    async def set_site_active(self, site: sites.Cluster):
        """
        Set a site to an active state.
        """
        await self._set_site_state(site, True)
        
    async def set_site_inactive(self, site: sites.Cluster):
        """
        Set a site to an inactive state.
        """
        await self._set_site_state(site, False)
        
    async def _set_site_state(self, site: sites.Cluster, active: bool):
        await self._col_sites.update_one(
            {"site": _not_falsy(site, "site").value},
            {"$set": {"active": active}},
            upsert=True,
        )

    async def list_active_clusters(self) -> set[sites.Cluster]:
        """
        Returns the currently active sites.
        """
        # don't bother with an index on active, not going to be that many sites
        cursor = self._col_sites.find({"active": True}, {"site": 1})
        return {sites.Cluster(doc["site"]) async for doc in cursor}

    async def save_image(self, image: models.Image):
        """
        Save details about a Docker image.
        
        Only one record per digest or tag is allowed per image
        """
        _not_falsy(image, "image")
        try:
            await self._col_images.insert_one(image.model_dump(mode="json"))
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

    def _clean_doc(self, doc: dict[str, Any]) -> dict[str, Any]:
        # removes the mongo _id in place.
        doc.pop(_FLD_MONGO_ID, None)
        return doc

    async def get_image(
        self, name: str, digest: str | None = None, tag: str | None = None
    ) -> models.Image:
        """
        Get an image via a name and tag or digest. Either a tag or digest is required. If both
        are provided, the tag is ignored.
        """
        doc, err = await self._process_image(name, digest, tag, self._col_images.find_one)
        if not doc:
            raise NoSuchImageError(f"No image {name} {err} exists in the system.")
        return self._to_image(doc)
    
    def _to_image(self, doc: dict[str, Any]) -> models.Image:
        return models.Image.model_construct(**self._clean_doc(doc))

    async def get_images(self) -> list[models.Image]:
        """
        Get images in the service in no particular order. At most 1000 images are returned.
        """
        # TODO FEATURE IMAGE paging - not really needed until > 1000 images
        # TODO FEATURE IMAGE sorting and filtering - not really needed until > 1000 images
        # For now sorting and filtering can be done client side. Seems likely we'll never have
        # 1000 active images
        images = []
        async for d in self._col_images.find().limit(1000):
            images.append(self._to_image(d))
        return images

    async def delete_image(self, name: str, digest: str | None = None, tag: str | None = None):
        """
        Delete an image via a name and tag or digest. Either a tag or digest is required. If both
        are provided, the tag is ignored.
        """
        delres, err = await self._process_image(name, digest, tag, self._col_images.delete_one)
        if delres.deleted_count != 1:
            raise NoSuchImageError(f"No image {name} {err} exists in the system.")

    async def _process_image(
        self, name: str, digest: str, tag: str, fn: Awaitable
    ) -> models.Image | DeleteResult:
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
        return await fn(query), err

    def _add_retry_attempt_to_trans_times(self, doc: dict[str, Any]):
        # modifies in place
        for ttdoc in doc[models.FLD_COMMON_TRANS_TIMES]:
            ttdoc[_FLD_RETRY_ATTEMPT] = 0  # unused for now

    async def save_job(self, job: models.AdminJobDetails):
        """ Save a job. Job IDs are expected to be unique. """
        _not_falsy(job, "job")
        jobd = job.model_dump(exclude_none=True)
        # Could add a check in the job model that jobs have > 0 transitions and the last
        # transition == the job state... probably not necessary.
        jobd[_FLD_UPDATE_TIME] = job.transition_times[-1].time
        jobd[_FLD_RETRY_ATTEMPT] = 0  # unused for now
        self._add_retry_attempt_to_trans_times(jobd)
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
        doc = await self._col_jobs.find_one(
            {models.FLD_COMMON_ID: _require_string(job_id, "job_id")}
        )
        if not doc:
            raise NoSuchJobError(f"No job with ID '{job_id}' exists")
        return self._doc_to_job(doc, as_admin=as_admin)
        
    def _doc_to_job(self, doc: dict[str, Any], as_admin: bool = False
    ) -> models.Job | models.AdminJobDetails:
        # TODO PERF build up the job piece by piece to skip S3 path validation
        doc = self._clean_doc(doc)
        return models.AdminJobDetails(**doc) if as_admin else models.Job(**doc)

    async def get_job_status(self, job_id: str) -> models.JobStatus:
        """
        Get minimal information about a job's status by the job's ID.
        """
        doc = await self._col_jobs.find_one(
            {models.FLD_COMMON_ID: _require_string(job_id, "job_id")},
            {
                _FLD_MONGO_ID: 0,
                models.FLD_COMMON_ID: 1,
                models.FLD_JOB_USER: 1,
                models.FLD_JOB_ADMIN_META: 1,
                models.FLD_COMMON_STATE: 1,
                models.FLD_COMMON_TRANS_TIMES: 1,
            },
        )
        if not doc:
            raise NoSuchJobError(f"No job with ID '{job_id}' exists")
        return models.JobStatus(**doc)

    async def list_jobs(
        self,
        user: str | None = None,
        state: models.JobState | None = None,
        after: datetime.datetime | None = None,
        before: datetime.datetime | None = None,
        limit: int = 1000
    ) -> list[models.JobPreview]:
        """
        List jobs.
        
        user - filter jobs by user.
        state - filter jobs by the job's current state.
        after - filter jobs to jobs that entered the current state after the given time, inclusive.
        before - filter jobs to jobs that entered the current state before the given time,
            exclusive.
        limit - the maximum number of jobs to return.
        """
        timequery = {}
        if after:
            timequery["$gte"] = verify_aware_datetime(after, "after")
        if before:
            timequery["$lt"] = verify_aware_datetime(before, "before")
        query = {}
        if user:
            query[models.FLD_JOB_USER] = user
        if state:
            query[models.FLD_COMMON_STATE] = state.value
        if timequery:
            query[_FLD_UPDATE_TIME] = timequery
        # drop the potentially large fields
        project = {
            models.FLD_JOB_OUTPUTS: 0,
            f"{models.FLD_JOB_JOB_INPUT}.{models.FLD_JOB_INPUT_INPUT_FILES}": 0
        }
        sort = [(_FLD_UPDATE_TIME, DESCENDING)]
        jobs = []
        async for j in self._col_jobs.find(
            query, project
            ).sort(sort
            ).limit(_check_num(limit, "limit")
        ):
            jobs.append(models.JobPreview(**self._clean_doc(j)))
        return jobs

    async def _update_job_state(
        self,
        col: AsyncIOMotorCollection,
        job_id: str,
        update: JobUpdate,
        time: datetime.datetime,
        trans_id: str | None = None,
        subjob_id: int | None = None,
    ):
        set_ = {}
        push = {}
        for fld, val in _not_falsy(update, "update").update_fields.items():
            jbfld, ispush = self._FIELD_TO_KEY_AND_PUSH[fld]
            target = push if ispush else set_
            target[jbfld] = val
        query = {models.FLD_COMMON_ID: _require_string(job_id, "job_id")}
        if update.current_state:
            query[models.FLD_COMMON_STATE] = update.current_state.value
        if subjob_id != None:
            query[models.FLD_SUBJOB_ID] = _check_num(subjob_id, "subjob_id")
        transition = {
            models.FLD_COMMON_STATE_TRANSITION_STATE: update.new_state.value,
            models.FLD_COMMON_STATE_TRANSITION_TIME: _not_falsy(time, "time"),
            _FLD_RETRY_ATTEMPT: 0,  # Unused for now
        }
        if trans_id:
            transition.update({
                models.FLD_JOB_STATE_TRANSITION_ID: trans_id,
                models.FLD_JOB_STATE_TRANSITION_NOTIFICATION_SENT: False
            })
        res = await col.update_one(
            query,
            {
                "$push": (push if push else {}) | {models.FLD_COMMON_TRANS_TIMES: transition},
                "$set": (set_ if set_ else {}) | {
                    models.FLD_COMMON_STATE: update.new_state.value,
                    _FLD_UPDATE_TIME: time,  # for indexing last state change time
                }
            },
        )
        if not res.matched_count:
            cs = f"in state {update.current_state.value} " if update.current_state else ""
            if subjob_id:
                raise NoSuchSubJobError(
                    f"No job with ID '{job_id}' and subjob ID {subjob_id} {cs}exists"
                )
            raise NoSuchJobError(f"No job with ID '{job_id}' {cs}exists")

    _FLD_NERSC_DL_TASK = f"{models.FLD_JOB_NERSC_DETAILS}.{models.FLD_NERSC_DETAILS_DL_TASK_ID}"
    _FLD_JAWS_RUN_ID = f"{models.FLD_JOB_JAWS_DETAILS}.{models.FLD_JAWS_DETAILS_RUN_ID}"
    _FLD_NERSC_UL_TASK = f"{models.FLD_JOB_NERSC_DETAILS}.{models.FLD_NERSC_DETAILS_UL_TASK_ID}"
    _FLD_NERSC_LOG_UL_TASK = (
        f"{models.FLD_JOB_NERSC_DETAILS}.{models.FLD_NERSC_DETAILS_LOG_UL_TASK_ID}"
    )
    def _setup_field_mappings(self):
        self._FIELD_TO_KEY_AND_PUSH = {
            UpdateField.NERSC_DOWNLOAD_TASK_ID: (self._FLD_NERSC_DL_TASK, True),
            UpdateField.JAWS_RUN_ID: (self._FLD_JAWS_RUN_ID, True),
            UpdateField.CPU_HOURS: (models.FLD_JOB_CPU_HOURS, False),
            UpdateField.NERSC_UPLOAD_TASK_ID: (self._FLD_NERSC_UL_TASK, True),
            UpdateField.OUTPUT_FILE_PATHS: (models.FLD_JOB_OUTPUTS, False),
            UpdateField.OUTPUT_FILE_COUNT: (models.FLD_JOB_OUTPUT_FILE_COUNT, False),
            UpdateField.NERSC_LOG_UPLOAD_TASK_ID: (self._FLD_NERSC_LOG_UL_TASK, True),
            UpdateField.USER_ERROR: (models.FLD_COMMON_ERROR, False),
            UpdateField.ADMIN_ERROR: (models.FLD_COMMON_ADMIN_ERROR, False),
            UpdateField.TRACEBACK: (models.FLD_COMMON_TRACEBACK, False),
            UpdateField.LOG_PATH: (models.FLD_JOB_LOGPATH, False),
        }
        self._REFDATA_FIELD_TO_KEY_AND_PUSH = {
            UpdateField.NERSC_DOWNLOAD_TASK_ID: (models.FLD_REFDATA_NERSC_DL_TASK_ID, True),
            UpdateField.USER_ERROR: (models.FLD_COMMON_ERROR, False),
            UpdateField.ADMIN_ERROR: (models.FLD_COMMON_ADMIN_ERROR, False),
            UpdateField.TRACEBACK: (models.FLD_COMMON_TRACEBACK, False),
        }

    async def update_job_state(
        self,
        job_id: str,
        update: JobUpdate,
        time: datetime.datetime,
        trans_id: str,
    ):
        """
        Update the job state, marking it as not yet sent to the notification system.
        
        job_id - the job ID.
        update - the update to apply to the job.
        time - the time at which the job transitioned to the new state.
        trans_id - a unique string representing an ID for the job state transition,
            to be used for sending to notification systems.
            The caller is responsible for ensuring uniqueness of IDs.
        """
        # If we need to send notifications to more than one place this will need a refactor. YAGNI
        await self._update_job_state(
            self._col_jobs,
            job_id,
            update,
            time,
            trans_id=_require_string(trans_id, "trans_id")
        )

    async def update_job_admin_meta(
        self,
        job_id: str,
        set_fields: dict[str, str | int | float] | None = None,
        unset_keys: set[str] | None = None,
    ) -> None:
        """
        Updates the admin metadata for the specified job.
    
        Does not affect any extant keys other than those specified in the function input.
    
        job_id - the ID of the job.
        set_fields - keys and their values to set in the admin metadata.
        unset_keys - keys to remove from the admin metadata.
    
        If both `set_fields` and `unset_keys` are None or empty, this method is a no-op.
        """
        # If we ever need to index these fields for queries, wildcard indexes seem like they
        # might work: https://www.mongodb.com/docs/manual/core/indexes/index-types/index-wildcard/
        # Alternatively, switch to a list of object with k / v keys like the workspace service.
        # Drawbacks to both; need to consider carefully
        # TODO CODE check that keys / values are under some maximum size. Ensure indexing can
        #           work with the max size and either of the two strategies above
        if set_fields and unset_keys:
            # needs to be checked here, otherwise mongo will throw an error
            conflict_keys = sorted(set(set_fields) & unset_keys)
            if conflict_keys:
                display_keys = conflict_keys[:10]
                msg = f"Cannot set and unset the same keys in a single update: {display_keys}"
                if len(conflict_keys) > 10:
                    msg += f" and {len(conflict_keys) - 10} more"
                raise IllegalAdminMetaError(msg)
        # TODO CODE should check for max meta size and throw an error if > META_LIMIT
        #           Since this is admin only for now probably safe w/o it
        #           Kind of a pain - need to fetch the meta, update in memory, check size,
        #           update in DB. Still chance for race conditions to exceed max 
        update = {}
        if set_fields:
            update["$set"] = {
                f"{models.FLD_JOB_ADMIN_META}.{k}": v for k, v in set_fields.items()
            }
        if unset_keys:
            update["$unset"] = {
                f"{models.FLD_JOB_ADMIN_META}.{k}": "" for k in unset_keys
            }

        if not update:
            return  # nothing to do

        result = await self._col_jobs.update_one(
            {models.FLD_COMMON_ID: _require_string(job_id, "job_id")},
            update
        )
        if not result.matched_count:
            raise NoSuchJobError(f"No job with ID '{job_id}' exists")

    async def job_update_sent(self, job_id: str, trans_id: str):
        """
        Mark a job state transition as sent to a notification system.
        
        job_id - the ID of the job.
        trans_id - the ID of the job state transition.
        """
        query = {
            models.FLD_COMMON_ID: _require_string(job_id, "job_id"),
            f"{models.FLD_COMMON_TRANS_TIMES}.{models.FLD_JOB_STATE_TRANSITION_ID}":
                _require_string(trans_id, "trans_id")
        }
        fld = (
            f"{models.FLD_COMMON_TRANS_TIMES}.$."
            + f"{models.FLD_JOB_STATE_TRANSITION_NOTIFICATION_SENT}"
        )
        res = await self._col_jobs.update_one(query, {"$set": {fld: True}})
        if not res.matched_count:
            raise NoSuchJobError(
                f"No job with ID '{job_id}' and state transition ID '{trans_id}' exists"
            )

    async def process_jobs_with_unsent_updates(
        self,
        processor: Callable[[models.AdminJobDetails], Coroutine[None, None, None]],
        older_than: datetime.datetime,
    ) -> int:
        """
        Find jobs with unsent state transitions older than a specified time and pass them to an
        async function.
        
        processor - an async function that takes a job as an argument and has no return.
            It will be called once per job found.
        older_than - Only jobs with state transitions that are older than the given date
            will be returned. Naive datetimes are not allowed.
            
        Returns the number of jobs found.
        """
        _not_falsy(processor, "processor")
        verify_aware_datetime(older_than, "older_than")
        # WARNING: There's a test in the mongo test file that ensures this query uses the
        # correct partial index. Be sure to update the test there if the query changes.
        query = {
            # needed to get the query planner to use the partial index, even though it's
            # redundant
            _FLD_TRANS_TIME_SEND: False,
            models.FLD_COMMON_TRANS_TIMES:{
                "$elemMatch": {
                    models.FLD_JOB_STATE_TRANSITION_NOTIFICATION_SENT: False,
                    models.FLD_COMMON_STATE_TRANSITION_TIME: {"$lt": older_than}
                }
            }
        }
        count = 0
        async for d in self._col_jobs.find(query):
            count += 1
            await processor(self._doc_to_job(d, as_admin=True))
        return count

    async def initialize_subjobs(self, subjobs: Iterable[models.SubJob]):
        """
        Initialize a set of subjobs for a job. The caller is expected to create the subjob
        objects correctly.
        
        Any index collisions are thrown, as it is assumed that subjobs are only initialized
        once, at the beginning of a job.
        """
        # TDOO CLEANUP it's possible that this method could fail part way though and leave
        #              orphaned subjob documents. Could add a cleanup thread. YAGNI for now.
        sjs = []
        for sj in _not_falsy(subjobs, "subjobs"):
            sjs.append(_not_falsy(sj, "subjob").model_dump())  # better exception message later
            self._add_retry_attempt_to_trans_times(sjs[-1])
        await self._col_subjobs.insert_many(sjs)

    async def get_subjob(self, job_id: str, subjob_id: int) -> models.SubJob:
        """
        Get a subjob given the job ID and the subjob ID.
        """
        # May want a bulk method at some point that takes a range of IDs or returns all
        doc = await self._col_subjobs.find_one({
            models.FLD_COMMON_ID: _require_string(job_id, "job_id"),
            models.FLD_SUBJOB_ID: _check_num(subjob_id, "subjob_id")
        })
        # if subjob_id isn't an integer, will throw an error. Could add a precheck later
        if not doc:
            raise NoSuchSubJobError(
                f"No sub job with job ID '{job_id}' and sub job ID {subjob_id} exists"
            )
        doc = self._clean_doc(doc)
        return models.SubJob(**doc)

    async def update_subjob_state(
        self,
        job_id: str,
        subjob_id: int,
        update: JobUpdate,
        time: datetime.datetime,
    ):
        """
        Update the sub job state.
    
        job_id - the job ID.
        subjob_id - the subjob ID.
        update - the update to apply to the job.
        time - the time at which the job transitioned to the new state.
        """
        await self._update_job_state(
            self._col_subjobs,
            job_id,
            update,
            time,
            # if not an int, error will occur. Could add a more stringent check later
            subjob_id=_check_num(subjob_id, "subjob_id")
        )

    async def have_subjobs_reached_state(self, job_id: str, *states: models.JobState
    ) -> dict[models.JobState, tuple[int, datetime.datetime | None]]:
        """
        Check if subjobs for a job_id have reached one or more specific job states.
        
        Subjobs may be in a state past the given state.
        
        Returns a dictionary of each input state to a tuple consisting of:
        * The number of subjobs that have reached the state
        * The latest time for the set of subjobs that have reached the state, or None if none
          of them have.
        
        Note that the method does not distinguish between no subjobs existing in the given state
        and no subjobs existing at all. Get the subjobs via the standard method to check for
        basic existence.
        """
        sts = [_not_falsy(s, "state").value for s in states]  # better exception later
        pipeline = [
            # Only process subjobs for this job_id, and only those that have a
            # transition in the states of interest.
            {"$match": {
                models.FLD_COMMON_ID: _require_string(job_id, "job_id"),
                models.FLD_COMMON_TRANS_TIMES: {
                    "$elemMatch": {
                        models.FLD_COMMON_STATE_TRANSITION_STATE: {"$in": sts},
                        _FLD_RETRY_ATTEMPT: 0  # unused for now
                    }
                }
            }},
            # Project matching transitions only
            {"$project": {
                models.FLD_SUBJOB_ID: 1,
                "matching_entries": {
                    "$filter": {
                        "input": f"${models.FLD_COMMON_TRANS_TIMES}",
                        "as": "tt",
                        "cond": {
                            "$and": [
                                {"$in": [
                                    f"$$tt.{models.FLD_COMMON_STATE_TRANSITION_STATE}", sts
                                ]},
                                {"$eq": [f"$$tt.{_FLD_RETRY_ATTEMPT}", 0]}  # unused for now
                            ]
                        }
                    }
                }
            }},
            # Flatten transitions so we can group by state
            {"$unwind": "$matching_entries"},
            # Deduplicate state per subjob, but still get latest timestamp per state/subjob
            {"$group": {
                "_id": {
                    "subjob": f"${models.FLD_SUBJOB_ID}",
                    "state": f"$matching_entries.{models.FLD_COMMON_STATE_TRANSITION_STATE}"
                },
                "latest_for_subjob": {
                    "$max": f"$matching_entries.{models.FLD_COMMON_STATE_TRANSITION_TIME}"
                }
            }},
            # Now group by state across all subjobs
            {"$group": {
                "_id": "$_id.state",
                "count": {"$sum": 1},  # one per subjob in this state
                "latest_ts": {"$max": "$latest_for_subjob"}
            }}
        ]
        # should only have 1 list entry per job state
        result = await self._col_subjobs.aggregate(pipeline).to_list(length=None)
        ret = {models.JobState(r["_id"]): (r["count"], r["latest_ts"]) for r in result}
        for s in states:
            if s not in ret:
                ret[s] = (0, None)
        return ret

    async def save_refdata(self, refdata: models.ReferenceData):
        """ Save reference data state. Reference data IDs are expected to be unique."""
        # don't bother checking for duplicate key exceptions since the service is supposed
        # to ensure unique IDs

        # TDOO REFDATA add a force option to allow for file overwrites if needed
        r = refdata.model_dump()
        # Could add a check in the refdata model that rds have > 0 statuses,
        # statuses have > 0 transitions and the last
        # transition == the redfdata cluster state... probably not necessary.
        for c, cm in zip(refdata.statuses, r[models.FLD_REFDATA_STATUSES]):
            cm[_FLD_UPDATE_TIME] = c.transition_times[-1].time
        res = await self._col_refdata.update_one(
            {"file": _not_falsy(refdata, "refdata").file},
            # do nothing if the document already exists
            {"$setOnInsert": r},
            upsert=True,
        )
        if not res.did_upsert:
            raise ReferenceDataExistsError(
                f"A reference data record for S3 path {refdata.file} already exists"
            )

    async def get_refdata(self) -> list[models.ReferenceData]:
        """
        Get reference data in the service in no particular order. At most 1000 are returned.
        """
        # TODO FEATURE REFDATA paging - not really needed until > 1000 images
        # TODO FEATURE REFDATA sorting and filtering - not really needed until > 1000 images
        # For now osrting and filtering can be done client side. Seems likely we'll never have
        # 1000 active images
        return await self._get_refdata(None, None)

    async def get_refdata_by_id(
        self, refdata_id: str, as_admin: bool = False
    ) -> models.ReferenceData | models.AdminReferenceData:
        """
        Get reference data by its unique ID.
        
        refdata_id - the reference data ID.
        as_admin - get additional details about the reference data.
        """
        doc = await self._col_refdata.find_one(
            {models.FLD_COMMON_ID: _require_string(refdata_id, "refdata_id")}
        )
        if not doc:
            raise NoSuchReferenceDataError(f"No reference data with ID '{refdata_id}' exists")
        return self._to_refdata(doc, as_admin=as_admin)

    async def get_refdata_by_path(self, s3_path: str) -> list[models.ReferenceData]:
        """
        Get reference data by the refdata file. Returns at most 1000 records.
        """
        return await self._get_refdata(
            models.FLD_REFDATA_FILE, _require_string(s3_path, "s3_path"),
        )

    # sorts by field ascending, so make sure there's an index for that field
    async def _get_refdata(self, field: str | None, value: Any) -> list[models.ReferenceData]:
        query = {field: value} if field else {}
        sort = field if field else models.FLD_REFDATA_FILE
        cursor = self._col_refdata.find(query).sort(sort, 1).limit(1000)
        return [self._to_refdata(d) for d in await cursor.to_list()]

    def _to_refdata(self, doc: dict[str, Any], as_admin: bool = False):
        doc = self._clean_doc(doc)
        return models.AdminReferenceData(**doc) if as_admin else models.ReferenceData(**doc)

    async def _update_refdata_state(
        self,
        cluster: sites.Cluster,
        refdata_id: str,
        state: models.ReferenceDataState,
        time: datetime.datetime,
        push: dict[str, Any] | None = None,
        set_: dict[str, Any] | None = None,
        current_state: models.ReferenceDataState | None = None,
    ):
        elemquery = {models.FLD_REFDATA_CLUSTER: _not_falsy(cluster, "cluster").value}
        if current_state:
            elemquery[models.FLD_COMMON_STATE] = current_state.value
        query = {
            models.FLD_COMMON_ID: _require_string(refdata_id, "refdata_id"),
            models.FLD_REFDATA_STATUSES: {"$elemMatch": elemquery},
        }
        subs = f"{models.FLD_REFDATA_STATUSES}.$[elem]."
        set_ = {f"{subs}{k}": v for k, v in set_.items()} if set_ else {}
        push = {f"{subs}{k}": v for k, v in push.items()} if push else {}
        res = await self._col_refdata.update_one(
            query,
            {
                "$push": push | {f"{subs}{models.FLD_COMMON_TRANS_TIMES}":
                    {
                        models.FLD_COMMON_STATE_TRANSITION_STATE:
                           _not_falsy(state, "state").value,
                        models.FLD_COMMON_STATE_TRANSITION_TIME: _not_falsy(time, "time"),
                    }
                },
                "$set": set_ | {
                    f"{subs}{models.FLD_COMMON_STATE}": state.value,
                    # for indexing last state change time. Unused currently
                    f"{subs}{_FLD_UPDATE_TIME}": time,
                }
            },
            array_filters=[{
                f"elem.{models.FLD_REFDATA_CLUSTER}": cluster.value}
            ],
        )
        if not res.matched_count:
            cs = f"in state {current_state.value} " if current_state else ""
            raise NoSuchReferenceDataError(
                f"No reference data with ID '{refdata_id}' for cluster {cluster.value} {cs}exits"
            )

    async def update_refdata_state(
        self,
        cluster: sites.Cluster,
        refdata_id: str,
        update: RefdataUpdate,
        time: datetime.datetime,
    ):
        """
        Update the reference data process state.
        
        cluster - the cluster to which this update applies.
        refdata_id - the reference data ID.
        update - the update to apply to the process.
        time - the time at which the process transitioned to the new state.
        """
        # Could merge this and and the above method...? Seems ok as is though
        set_ = {}
        push = {}
        for fld, val in update.update_fields.items():
            rffld, ispush = self._REFDATA_FIELD_TO_KEY_AND_PUSH[fld]
            target = push if ispush else set_
            target[rffld] = val
        await self._update_refdata_state(
            cluster,
            refdata_id,
            update.new_state,
            time,
            current_state=update.current_state,
            set_=set_,
            push=push
        )


class NoSuchImageError(Exception):
    """ The image does not exist in the system. """


class NoSuchJobError(Exception):
    """ The job does not exist in the system. """


class NoSuchSubJobError(Exception):
    """ The sub job does not exist in the system. """


class NoSuchReferenceDataError(Exception):
    """ The reference data does not exist in the system. """


class ReferenceDataExistsError(Exception):
    """ The reference data already exists in the system. """


class ImageTagExistsError(Exception):
    """ The tag for the image already exists in the system. """


class ImageDigestExistsError(Exception):
    """ The digest for the image already exists in the system. """

class IllegalAdminMetaError(Exception):
    """ The specified admin metadata update is illegal. """
