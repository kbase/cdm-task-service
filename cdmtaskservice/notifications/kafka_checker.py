"""
WARNING: read the documentation completely before running this code.

Checks for jobs with unsent notifications older than a specified time period, and sends the
messages.

Note that it is possible to cause messages to be sent multiple times by running this code.
For instance, if Kafka is down and at least one CTS server is up and processing jobs, the
job updates will be cached by the CTS's Kafka client. Running this code will cache all those
updates as well. If Kafka comes back up, all the updates, the originals and the updates
from this code, will be sent to the server.

It it safe to run this code while:
* all CTS servers are down and Kafka is up, or
* CTS servers and Kafka are up, and both have been up long enough such that there is a minimal
  update queue in the CTS servers; e.g. the CTS servers should send messages before the time
  delay specified in the arguments expires.
  
It is not safe to run this code when updates are not sent to Kafka within the time delay; in
that case running this code may cause duplicate messages.
"""

import datetime
import logging

from cdmtaskservice import logfields
from cdmtaskservice import models
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy

# If we add different types of notification we'll need to put that in the job 
# and handle it here. YAGNI


class KafkaChecker:
    """
    Checks that Kafka messages have been sent for jobs based on the job records and sends them
    if necessary.
    """
    
    def __init__(self, mongo: MongoDAO, kafka: KafkaNotifier):
        """
        Create the checker.
        
        mongo - the mongo instance from which jobs with unsent updates will be sourced.
        kafka - the kafka instance where updates will be sent.
        """
        self._mongo = _not_falsy(mongo, "mongo")
        self._kafka = _not_falsy(kafka, "kafka")
    
    async def check(self, older_than: datetime.datetime) -> tuple[int, int]:
        """
        WARNING: read the module documentation completely before running this method. 
        Check for unsent job updates in Mongo and send them to Kafka.
        
        older_than - only send updates that are older than the given time.
        
        Returns a tuple of the number of jobs affected and the number of updates sent,
        which could be > 1 per job.
        """
        jobs = set()
        updates = [0]
        logr = logging.getLogger(__name__)
        async def update(job: models.AdminJobDetails):
            for trans in job.transition_times:
                # only send updates for state changes where the message isn't sent and
                # is older than the given time
                if not trans.notif_sent and trans.time < older_than:
                    jobs.add(job.id)
                    updates[0] += 1
                    # args are important to capture variables in closure
                    async def cb(job_id: str, t: models.AdminJobStateTransition):
                        await self._mongo.job_update_sent(job_id, t.trans_id)
                        # TODO TEST logging
                        logr.info(
                            "Found unsent job update and sent to Kafka",
                            extra={
                                logfields.JOB_ID: job_id,
                                logfields.TRANS_ID: t.trans_id
                            }
                        )
                    await self._kafka.update_job_state(
                        job.id, trans.state, trans.time, trans.trans_id, callback=cb(job.id, trans)
                    )
        await self._mongo.process_jobs_with_unsent_updates(update, older_than)
        return len(jobs), updates[0]
