"""
Handles cleaning up after completed jobs and refdata after some time delay.

Also includes methods for manually cleaning jobs and refdata.
"""

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import datetime
import logging

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.jobflows.flowmanager import JobFlow, JobFlowManager
from cdmtaskservice import logfields
from cdmtaskservice import models
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.pipelines import models as pipe_models
from cdmtaskservice import sites
from cdmtaskservice.timestamp import utcdatetime
from cdmtaskservice.user import CTSUser


# The staged entity types sharing the clean-up logic below (refdata, AI models) differ only in
# which flow / mongo methods to call and how to describe themselves in logs - everything else
# (the noop-if-cleaned check, the terminal-state branch, the sweep-all-statuses loop) is
# identical, so that logic lives once in _clean_staged_entity / _process_staged_entity below and
# dispatches on get_entity_type() for the handful of points that differ.
_STAGED_ENTITY_DESC: dict[models.EntityType, tuple[str, str]] = {
    models.EntityType.REFDATA: ("refdata", logfields.REFDATA_ID),
    models.EntityType.AI_MODEL: ("AI model", logfields.AI_MODEL_ID),
}


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
            + f"on jobs / refdata older than {minimum_age}")
    
    async def clean_job(
        self,
        job: models.AdminJobDetails | pipe_models.AdminPipelineJob,
        user: CTSUser,
        force: bool = False,
    ):
        """
        Clean any transient files for a job or pipeline job. If the job is already in the
        cleaned state this is a noop. If the job is not in a terminal state the job will not be
        set to cleaned.

        WARNING: setting force to True may cause undefined behavior. True will
        cause job files to be removed regardless of job state.
        """
        # Similar to the method below, but trying to merge them was a mess
        _not_falsy(job, "job")
        _not_falsy(user, "user")
        if job.cleaned:
            return
        flow = await self._flowman.get_flow(job.get_cluster())
        await flow.clean_job(job, force=force)
        if job.state.is_terminal():
            # if force is True and the job isn't in the terminal state
            # the job may produce more dirt later
            await self._set_clean(job)
            self._logr.info(
                f"Cleaned job '{job.id}' at user '{user.user}' request",
                extra={logfields.JOB_ID: job.id}
            )
        else:
            self._logr.info(
                f"{'Force c' if force else 'C'}leaned job '{job.id}' at user '{user.user}' "
                + "request but did not set clean state",
                extra={logfields.JOB_ID: job.id}
            )

    async def _set_clean(self, job: models.AdminJobDetails | pipe_models.AdminPipelineJob):
        if job.is_pipeline():
            await self._mongo.set_pipeline_job_clean(job.id)
        else:
            await self._mongo.set_job_clean(job.id)

    async def _process_job(self, job: models.AdminJobDetails | pipe_models.AdminPipelineJob):
        # Similar to the method above, but trying to merge them was a mess
        # May need to parallelize this, but YAGNI for now
        try:
            if job.get_cluster() not in await self._flowman.list_usable_clusters():
                return
            flow = await self._flowman.get_flow(job.get_cluster())
            await flow.clean_job(job)
            await self._set_clean(job)
            self._logr.info(f"Cleaned job '{job.id}'", extra={logfields.JOB_ID: job.id})
        except Exception as e:
            # Nothing really to be done. Maybe fixed on next attempt
            self._logr.exception(
                f"Failed cleaning job '{job.id}': {e}", extra={logfields.JOB_ID: job.id}
            )

    async def clean_refdata(
        self,
        refdata: models.AdminReferenceData,
        cluster: sites.Cluster,
        user: CTSUser,
        force: bool = False
    ):
        """
        Clean any transient refdata files for a cluster. If the refdata is already in the
        cleaned state for the cluster this is a noop. If the refdata cluster is not in a terminal
        state the refdata cluster will not be set to cleaned.

        WARNING: setting force to True may cause undefined behavior. True will
        cause refdata files to be removed regardless of refdata state.
        """
        _not_falsy(refdata, "refdata")
        await self._clean_staged_entity(refdata, cluster, user, force=force)

    async def clean_ai_model(
        self,
        ai_model: models.AdminAIModel,
        cluster: sites.Cluster,
        user: CTSUser,
        force: bool = False,
    ):
        """
        Clean any transient AI model staging files for a cluster. If the AI model is already
        in the cleaned state for the cluster this is a noop. If the AI model cluster is not in
        a terminal state the AI model cluster will not be set to cleaned.

        WARNING: setting force to True may cause undefined behavior. True will
        cause AI model files to be removed regardless of staging state.
        """
        _not_falsy(ai_model, "ai_model")
        await self._clean_staged_entity(ai_model, cluster, user, force=force)

    async def _clean_staged_entity(
        self,
        entity: models.AdminReferenceData | models.AdminAIModel,
        cluster: sites.Cluster,
        user: CTSUser,
        force: bool = False,
    ):
        # entity's own non-null check is the caller's responsibility (clean_refdata/clean_ai_model)
        # so the error message uses the public method's own parameter name (refdata/ai_model),
        # not the generic "entity" name used here.
        _not_falsy(cluster, "cluster")
        _not_falsy(user, "user")
        status = entity.get_status_for_cluster(cluster)
        if status.cleaned:
            return
        flow = await self._flowman.get_flow(cluster)
        await self._flow_clean_staged_entity(flow, entity, force)
        entity_id = self._staged_entity_id(entity)
        noun, logfield = _STAGED_ENTITY_DESC[entity.get_entity_type()]
        if status.state.is_terminal():
            # if force is True and the entity isn't in the terminal state
            # the entity may produce more dirt later
            await self._set_staged_entity_clean(cluster, entity)
            self._logr.info(
                f"Cleaned {noun} '{entity_id}' for cluster '{cluster.value}' "
                + f"at user '{user.user}' request",
                extra={logfield: entity_id, logfields.CLUSTER: cluster.value}
            )
        else:
            self._logr.info(
                f"{'Force c' if force else 'C'}leaned {noun} '{entity_id}' "
                + f"for cluster '{cluster.value}' at user '{user.user}' "
                + "request but did not set clean state",
                extra={logfield: entity_id, logfields.CLUSTER: cluster.value}
            )

    async def _process_staged_entity(
        self, entity: models.AdminReferenceData | models.AdminAIModel
    ):
        # May need to parallelize this, but YAGNI for now
        older_than = utcdatetime() - self._minage
        entity_id = self._staged_entity_id(entity)
        noun, logfield = _STAGED_ENTITY_DESC[entity.get_entity_type()]
        for s in entity.statuses:
            if s.cleaned or not s.state.is_terminal() or s.transition_times[-1].time >= older_than:
                continue
            try:
                if s.cluster not in await self._flowman.list_usable_clusters():
                    continue
                flow = await self._flowman.get_flow(s.cluster)
                await self._flow_clean_staged_entity(flow, entity, False)
                await self._set_staged_entity_clean(s.cluster, entity)
                self._logr.info(
                    f"Cleaned {noun} '{entity_id}' for cluster '{s.cluster.value}'",
                    extra={logfield: entity_id, logfields.CLUSTER: s.cluster.value}
                )
            except Exception as e:
                # Nothing really to be done. Maybe fixed on next attempt
                self._logr.exception(
                    f"Failed cleaning {noun} '{entity_id}' "
                    + f"for cluster '{s.cluster.value}': {e}",
                    extra={logfield: entity_id, logfields.CLUSTER: s.cluster.value}
                )

    @staticmethod
    def _staged_entity_id(entity: models.AdminReferenceData | models.AdminAIModel) -> str:
        if entity.get_entity_type() == models.EntityType.REFDATA:
            return entity.id
        return str(entity.key)

    @staticmethod
    async def _flow_clean_staged_entity(
        flow: JobFlow, entity: models.AdminReferenceData | models.AdminAIModel, force: bool
    ):
        if entity.get_entity_type() == models.EntityType.REFDATA:
            await flow.clean_refdata(entity, force=force)
        else:
            await flow.clean_ai_model(entity, force=force)

    async def _set_staged_entity_clean(
        self, cluster: sites.Cluster, entity: models.AdminReferenceData | models.AdminAIModel
    ):
        if entity.get_entity_type() == models.EntityType.REFDATA:
            await self._mongo.set_refdata_clean(cluster, entity.id)
        else:
            await self._mongo.set_ai_model_clean(cluster, entity.key)

    async def _run(self):
        older_than = utcdatetime() - self._minage
        self._logr.info(f"Cleaning jobs and refdata older than {older_than}")
        try:
            await self._mongo.process_dirty_jobs(older_than, self._process_job)
        except Exception as e:
            # Nothing really to be done. Something is very wrong. Maybe fixed on next attempt
            self._logr.exception(f"Failed processing jobs for cleanup: {e}")
        try:
            await self._mongo.process_dirty_pipeline_jobs(older_than, self._process_job)
        except Exception as e:
            # Nothing really to be done. Something is very wrong. Maybe fixed on next attempt
            self._logr.exception(f"Failed processing pipeline jobs for cleanup: {e}")
        try:
            await self._mongo.process_dirty_refdata(older_than, self._process_staged_entity)
        except Exception as e:
            # Nothing really to be done. Something is very wrong. Maybe fixed on next attempt
            self._logr.exception(f"Failed processing refdata for cleanup: {e}")
        # TODO AI_MODEL wire up an analogous mongo.process_dirty_ai_models(older_than,
        #      self._process_staged_entity) call here once that mongo method is added -
        #      _process_staged_entity is already AI-model-ready (Chunk 10.1).
    
    def close(self):
        """Shutdown the scheduler."""
        self._scheduler.shutdown(wait=True)
