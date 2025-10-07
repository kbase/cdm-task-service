"""
Helper class for performing job and refdata state updates for job flows.
"""

import logging
import traceback
import uuid

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice import logfields
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice import sites
from cdmtaskservice import timestamp
from cdmtaskservice.update_state import refdata_error, error, JobUpdate, RefdataUpdate


class JobFlowStateUpdates:
    """
    Performs state updates for job flows.
    """
    
    def __init__(self, cluster: sites.Cluster, mongo: MongoDAO, kafka: KafkaNotifier):
        """
        Initialize the updater.
        
        cluster - the cluster to which updates will be applied.
        mongo - the MongoDB DAO.
        kafka - a Kafka notifier.
        """
        self._cluster = _not_falsy(cluster, "cluster")
        self._mongo = _not_falsy(mongo, "mongo")
        self._kafka = _not_falsy(kafka, "kafka")
        
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
                user_err, admin_err, traceback=traceback, log_files_path=logpath
            ))

    async def update_job_state(self, job_id: str, update: JobUpdate):
        """
        Update the state of a job.
        
        job_id - the job to update.
        update - the update to apply.
        """
        _require_string(job_id, "job_id")
        _not_falsy(update, "update")
        # TODO TEST will need to mock out uuid
        trans_id = str(uuid.uuid4())
        # TODO TEST will need a way to mock out timestamps
        update_time = timestamp.utcdatetime()
        async def cb():
            await self._mongo.job_update_sent(job_id, trans_id)
        await self._mongo.update_job_state(job_id, update, update_time, trans_id)
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
            # TODO TEST will need a way to mock out timestamps
            self._cluster, refdata_id, update, timestamp.utcdatetime()
        )
