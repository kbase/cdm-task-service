"""
Helper class for performing job and refdata state updates for job flows.
"""

from collections.abc import Callable
import datetime
from dataclasses import dataclass
import logging
import traceback
import uuid

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice import models
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice import logfields
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice import sites
from cdmtaskservice import timestamp
from cdmtaskservice.update_state import refdata_error, error, JobUpdate, RefdataUpdate


@dataclass
class ParentJobUpdate:
    """
    The aggregate state result used to determine the parent job's next transition.
    Returned by SubjobFlowStateUpdates.get_parent_job_update.
    """

    state: models.JobState
    """ The state to transition the parent job to. """

    time: datetime.datetime
    """ The time at which the last subjob reached the equivalent state. """


class JobFlowStateUpdates:
    """
    Performs state updates for job flows.
    """
    
    def __init__(
        self,
        cluster: sites.Cluster,
        mongo: MongoDAO,
        kafka: KafkaNotifier,
        _timestamp_fn: Callable[[], datetime.datetime] = timestamp.utcdatetime,
        _trans_id_fn: Callable[[], str] = lambda: str(uuid.uuid4()),
    ):
        """
        Initialize the updater.

        cluster - the cluster to which updates will be applied.
        mongo - the MongoDB DAO.
        kafka - a Kafka notifier.
        """
        self._cluster = _not_falsy(cluster, "cluster")
        self._mongo = _not_falsy(mongo, "mongo")
        self._kafka = _not_falsy(kafka, "kafka")
        self._timestamp_fn = _timestamp_fn
        self._trans_id_fn = _trans_id_fn
        
    async def handle_exception(
        self, e: Exception, entity_id: str, erraction: str, refdata: bool = False
    ):
        """
        Update a job's state to register an exception. Expected to be called from within an
        except block only.
        
        e - the exception that occurred.
        entity_id - the job or refdata ID.
        erraction - the action that caused the error. Used in the logging string.  Examples:
           * downloading files for the
           * completing
        refdata - True if the exception occurred in a refdata operation.
        """
        _not_falsy(e, "e")
        _require_string(entity_id, "entity_id")
        _require_string(erraction, "erraction")
        logging.getLogger(__name__).exception(
            f"Error {erraction} {'refdata' if refdata else 'job'}.",
            extra={logfields.REFDATA_ID if refdata else logfields.JOB_ID: entity_id}
        )
        await self.save_error(
            entity_id,
            # We'll need to see what kinds of errors happen and change the user message
            # appropriately. Just provide a generic message for now, as most errors aren't
            # going to be fixable by users
            "An unexpected error occurred",
            str(e),
            traceback=traceback.format_exc(),
            refdata=refdata,
        )
        
    async def save_error(
        self,
        entity_id: str,
        user_err: str,
        admin_err: str,
        traceback: str = None,
        logpath: str = None,
        refdata=False,
    ):
        """
        Save an error to the database.
        
        entity_id - the job or refdata ID.
        user_err - the error to present to users.
        admin_err - the error to present to admins.
        traceback - the error traceback, if any.
        logpath - the path to error logs in an S3 instance, if any.
        refdata - True if the error occurred in a refdata operation.
        """
        _require_string(entity_id, "entity_id")
        _require_string(user_err, "user_err")
        _require_string(admin_err, "admin_err")
        # if this fails, well, then we're screwed
        if refdata:
            await self.update_refdata_state(entity_id, refdata_error(
                user_err, admin_err, traceback=traceback)
            )
        else:
            await self.update_job_state(entity_id, error(
                admin_err, user_error=user_err, traceback=traceback, log_files_path=logpath
            ))

    async def update_job_state(
        self,
        job_id: str,
        update: JobUpdate,
        update_time: datetime.datetime = None,
        recovery_cooldown: datetime.timedelta | None = None,
    ):
        """
        Update the state of a job.

        job_id - the job to update.
        update - the update to apply.
        update_time - the timestamp for the update, defaulting to now.
        recovery_cooldown - if provided and > 0, prevents a job recovery attempt if the prior
            recovery, if any, was less than the time provided in the past. If provided at all,
            sets the time of the last recovery attempt.
        """
        _require_string(job_id, "job_id")
        _not_falsy(update, "update")
        trans_id = self._trans_id_fn()
        update_time = update_time if update_time else self._timestamp_fn()
        async def cb():
            await self._mongo.job_update_sent(job_id, trans_id)
        await self._mongo.update_job_state(
            job_id, update, update_time, trans_id, recovery_cooldown=recovery_cooldown
        )
        await self._kafka.update_job_state(
            job_id, update.new_state, update_time, trans_id, callback=cb()
        )

    async def update_refdata_state(self, refdata_id: str, update: RefdataUpdate):
        """
        Update the state of a refdata operation.

        refdata_id - the ID of the refdata operation to update.
        update - the update to apply.
        """
        _require_string(refdata_id, "refdata_id")
        _not_falsy(update, "update")
        await self._mongo.update_refdata_state(
            self._cluster, refdata_id, update, self._timestamp_fn()
        )


class SubjobFlowStateUpdates(JobFlowStateUpdates):
    """
    Extends JobFlowStateUpdates with subjob state aggregation for job flows that use subjobs.
    """

    async def get_parent_job_update(
        self,
        job: models.Job,
        subjob_transition: models.JobState,
    ) -> ParentJobUpdate | None:
        """
        Determine whether a subjob transition should trigger a transition for the parent job.

        job - the parent job.
        subjob_transition - the state the subjob transitioned to.

        Returns the update to apply to the parent job, or None if not all subjobs have reached
        an equivalent state yet.
        """
        # Not liking this implementation, it has to know too much about equivalent job states
        # Making it work for now, maybe can be refactored later

        _not_falsy(job, "job")
        st = _not_falsy(subjob_transition, "subjob_transition")
        js = models.JobState
        if st.is_canceling():
            raise ValueError("Subjobs cannot transition to the canceling states.")
        if st == js.RECOVERING:
            raise ValueError("Subjobs cannot transition to the recovering state.")
        if st in {js.CREATED, js.DOWNLOAD_SUBMITTED, js.JOB_SUBMITTING, js.JOB_SUBMITTED}:
            stcount = (await self._mongo.have_subjobs_reached_state(job.id, st))[st]
            self._check_count_is_valid(st, stcount[0], stcount[0], job.job_input.num_containers)
            return ParentJobUpdate(
                st, stcount[1]
            ) if stcount[0] == job.job_input.num_containers else None
        if st in {js.UPLOAD_SUBMITTING, js.ERROR_PROCESSING_SUBMITTING}:
            return await self._check_equiv_states_complete(
                job, st, js.UPLOAD_SUBMITTING, js.ERROR_PROCESSING_SUBMITTING
            )
        if st in {js.UPLOAD_SUBMITTED, js.ERROR_PROCESSING_SUBMITTED}:
            return await self._check_equiv_states_complete(
                job, st, js.UPLOAD_SUBMITTED, js.ERROR_PROCESSING_SUBMITTED
            )
        if st.is_terminal():
            return await self._check_equiv_states_complete(job, st, js.COMPLETE, js.ERROR)
        raise ValueError("Seems like someone added a state without updating this method, oops")

    @staticmethod
    def _check_count_is_valid(
        st: models.JobState, state_count: int, all_count: int, ttl_count: int
    ):
        if all_count > ttl_count:
            raise ValueError(f"More subjobs found ({all_count}) than containers ({ttl_count})")
        if state_count < 1:
            raise ValueError(
                f"You reported that a subjob transitioned to state {st.value} but no "
                + "subjobs are in that state"
            )

    async def _check_equiv_states_complete(
        self,
        job: models.Job,
        target_state: models.JobState,
        stdstate: models.JobState,
        errstate: models.JobState,
    ) -> ParentJobUpdate | None:
        counts = await self._mongo.have_subjobs_reached_state(job.id, stdstate, errstate)
        ttl = sum(c[0] for c in counts.values())
        self._check_count_is_valid(
            target_state, counts[target_state][0], ttl, job.job_input.num_containers
        )
        if ttl != job.job_input.num_containers:
            return None
        t = max(c[1] for c in counts.values() if c[1])
        return ParentJobUpdate(errstate, t) if counts[errstate][0] > 0 else ParentJobUpdate(
            stdstate, t
        )
