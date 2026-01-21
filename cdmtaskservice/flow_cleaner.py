"""
Handles cleaning up after completed jobs and refdata after some time delay.

Also includes a method for manually cleaning a job.
"""

# TOCO REFDATA CLEAN implement refdata cleanup

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import datetime
import logging

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.jobflows.flowmanager import JobFlowManager
from cdmtaskservice import logfields
from cdmtaskservice import models
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.timestamp import utcdatetime
from cdmtaskservice.user import CTSUser


class FlowCleaner:
    """
    The flow cleaner.
    """
    
    def __init__(
        self,
        mongo: MongoDAO,
        flowman: JobFlowManager,
        minimum_age: datetime.timedelta,
        frequency: datetime.timedelta,
    ):
        """
        Create the flow cleaner.
        
        mongo - a MongoDB DAO instance where job and refdata state will be queried and modified.
        flowman - the flowmanager for job flows.
        minimum_age - the minimum age of job / refdata where cleaning will be run.
        frequency - how often the cleanup process should run.
        """
        self._mongo = _not_falsy(mongo, "mongo")
        self._flowman = _not_falsy(flowman, "flowman")
        self._minage = _not_falsy(minimum_age, "minimum_age")
        _not_falsy(frequency, "frequency")
        self._logr = logging.getLogger(__name__)
        self._scheduler = AsyncIOScheduler()
        self._scheduler.add_job(
            self._run,
            # add up to 10m of random time before job starts to prevent multiple servers all
            # starting at the same time 
            trigger=IntervalTrigger(seconds=frequency.total_seconds(), jitter=600),
            max_instances=1,
            coalesce=True,
        )
        self._scheduler.start()
        self._logr.info(
            f"Started flow cleanup scheduler, running approximately every {frequency} "
            + f"on jobs older than {minimum_age}")
    
    async def clean_job(self, job: models.AdminJobDetails, user: CTSUser, force: bool = False):
        """
        Clean any transient files for a job. If the job is already in the cleaned state this is
        a noop. If the job is not in a terminal state the job will not be set to cleaned.
        
        WARNING: setting force to True may cause undefined behavior. True will 
        cause job files to be removed regardless of job state.
        """
        # Similar to the method below, but trying to merge them was a mess
        _not_falsy(job, "job")
        _not_falsy(user, user)
        if job.cleaned:
            return
        flow = await self._flowman.get_flow(job.job_input.cluster)
        await flow.clean_job(job, force=force)
        if job.state.is_terminal():
            # if force is True and the job isn't in the terminal state
            # the job may produce more dirt later
            await self._mongo.set_job_clean(job.id)
            self._logr.info(
                f"Cleaned job '{job.id}' at user {user.user}' request",
                extra={logfields.JOB_ID: job.id}
            )
        else:
            self._logr.info(
                f"{'Force c' if force else 'C'}leaned job '{job.id}' at user '{user.user}' "
                + "request but did not set clean state",
                extra={logfields.JOB_ID: job.id}
            )
    
    async def _process_job(self, job: models.AdminJobDetails):
        # Similar to the method above, but trying to merge them was a mess
        # May need to parallelize this, but YAGNI for now
        try:
            if job.job_input.cluster not in await self._flowman.list_usable_clusters():
                return
            flow = await self._flowman.get_flow(job.job_input.cluster)
            await flow.clean_job(job)
            await self._mongo.set_job_clean(job.id)
            self._logr.info(f"Cleaned job {job.id}", extra={logfields.JOB_ID: job.id})
        except Exception as e:
            # Nothing really to be done. Maybe fixed on next attempt
            self._logr.exception(
                f"Failed cleaning job {job.id}: {e}", extra={logfields.JOB_ID: job.id}
            )

    async def _run(self):
        older_than = utcdatetime() - self._minage
        self._logr.info(f"Cleaning jobs older than {older_than}")
        try:
            await self._mongo.process_dirty_jobs(older_than, self._process_job)
        except Exception as e:
            # Nothing really to be done. Something is very wrong. Maybe fixed on next attempt
            self._logr.exception(f"Failed processing jobs for cleanup: {e}")
    
    def close(self):
        """Shutdown the scheduler."""
        self._scheduler.shutdown(wait=True)
