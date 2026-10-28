"""
Handles what job flows are available to users and their current state.
"""

from abc import ABC, abstractmethod
import asyncio
from async_lru import alru_cache
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice import models
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.pipelines import models as pipe_models
from cdmtaskservice.s3.client import S3ObjectMeta
from cdmtaskservice import sites
from cdmtaskservice.user import CTSUser


class JobFlow(ABC):
    """
    A super class for job flows.
    """

    @abstractmethod
    async def preflight(self, user: CTSUser, job_id: str, job_input: models.JobInput):
        """ Check that a job can be run prior to submission. """

    @abstractmethod
    async def preflight_pipeline(self, user: CTSUser, job_id: str):
        """ Check that a pipeline job can be run prior to submission. """

    @abstractmethod
    async def get_subjobs(
        self, job_id: str, container_num: int = None
    ) -> models.SubJob | list[models.SubJob]:
        """ Get the subjob(s) for a job. """

    @abstractmethod
    async def get_job_external_runner_details(
        self,
        job: models.AdminJobDetails | pipe_models.AdminPipelineJob,
        container_number: int | None = None,
    ) -> dict[str, Any]:
        """ Get details about the external runner for a job. """

    @abstractmethod
    async def get_job_external_runner_status(
        self,
        job: models.AdminJobDetails | pipe_models.AdminPipelineJob,
    ) -> models.ExternalRunnerStatus:
        """ Get the status of the external runner for a job. """

    @abstractmethod
    async def start_job(
        self, job: models.Job | pipe_models.AdminPipelineJob, objmeta: list[S3ObjectMeta]
    ):
        """ Start a job running. """

    @abstractmethod
    async def download_complete(self, job: models.AdminJobDetails | pipe_models.AdminPipelineJob):
        """ Continue a job after its input files have finished downloading. """

    @abstractmethod
    async def job_complete(self, job: models.AdminJobDetails | pipe_models.AdminPipelineJob):
        """ Continue a job after it has finished running. """

    @abstractmethod
    async def upload_complete(self, job: models.AdminJobDetails | pipe_models.AdminPipelineJob):
        """ Continue a job after its output files have finished uploading. """

    @abstractmethod
    async def error_log_upload_complete(self, job: models.AdminJobDetails):
        """ Continue a job after its error logs have finished uploading. """

    @abstractmethod
    async def update_subjob_heartbeat(self, job_id: str, container_num: int):
        """ Update the heartbeat for a subjob. """

    @abstractmethod
    async def update_container_state(
        self,
        job: models.AdminJobDetails,
        container_num: int,
        new_state: models.JobState,
        update: models.ContainerUpdate,
    ):
        """ Update the state of a container (subjob). """

    @abstractmethod
    async def cancel_job(self, job: models.AdminJobDetails | pipe_models.AdminPipelineJob):
        """ Cancel a running job. """

    @abstractmethod
    async def clean_job(
        self, job: models.AdminJobDetails | pipe_models.AdminPipelineJob, force: bool = False
    ):
        """ Clean up any intermediate data for a job. """

    @abstractmethod
    async def recover_job(self, job: models.AdminJobDetails, force: bool = False):
        """ Attempt to recover a job that may be stuck. """

    @abstractmethod
    async def stage_refdata(self, refdata: models.ReferenceData, objmeta: S3ObjectMeta):
        """ Start staging reference data at this job flow's cluster. """

    @abstractmethod
    async def refdata_complete(self, refdata_id: str):
        """ Continue a refdata staging process after its download has completed. """

    @abstractmethod
    async def update_refdata_state(
        self,
        refdata_id: str,
        new_state: models.StagingState,
        update: models.StagingStateUpdate | None = None,
    ):
        """ Update the staging state of reference data for this job flow's cluster. """

    @abstractmethod
    async def clean_refdata(self, refdata: models.ReferenceData, force: bool = False):
        """ Clean up any intermediate staging data for reference data. """

    @abstractmethod
    async def clean_ai_model(self, ai_model: models.AdminAIModel, force: bool = False):
        """ Clean up any intermediate staging data for an AI model snapshot. """

    @abstractmethod
    async def stage_ai_model(self, ai_model: models.AdminAIModel):
        """ Start staging an AI model snapshot at this job flow's cluster. """

    @abstractmethod
    async def ai_model_complete(self, key: models.AIModelKey):
        """ Continue an AI model staging process after its download has completed. """

    @abstractmethod
    async def update_ai_model_state(
        self,
        key: models.AIModelKey,
        new_state: models.StagingState,
        update: models.StagingStateUpdate | None = None,
    ):
        """ Update the staging state of an AI model for this job flow's cluster. """


@dataclass(frozen=True)
class JobFlowOrError():
    """
    Represents a job flow or an error initializating the job flow. It is expected that
    only one of the fields is populated.
    """
    # TODO CODE check only one is initialized and check contents
    error: str | None = None
    jobflow: JobFlow | None = None


class JobFlowManager():
    
    def __init__(self, mongodao: MongoDAO):
        """ Create the job flow manager with a mongo client. """
        self._mongo = _not_falsy(mongodao, "mongodao")
        self._flows = {}

    def register_flow(
            self, cluster: sites.Cluster, flow_provider: Callable[[], Awaitable[JobFlowOrError]]
        ):
        """
        Add a job flow provider to the manager.
        
        cluster - the cluster associated with the job flow.
        flow_provider - the flow provider. The provider is expected to be called repeatedly
            and return quickly whenever a job flow is requested.
        """
        self._flows[_not_falsy(cluster, "cluster")] = _not_falsy(flow_provider, "flow_provider")
        
    async def get_flow(self, cluster: sites.Cluster) -> JobFlow:
        """
        Get a job flow for a cluster.
        
        Throws an error if the flow is inactive or unavailable.
        """
        if _not_falsy(cluster, "cluster") in self._flows:
            if cluster not in await self.list_active_clusters():
                raise InactiveJobFlowError(f"Job flow for cluster {cluster.value} is disabled")
            floworerr = await self._flows[cluster]()
            if floworerr.error:
                raise UnavailableJobFlowError(
                    f"Job flow for cluster {cluster.value} is unavailable: {floworerr.error}"
                )
            return floworerr.jobflow
        else:
            raise UnavailableJobFlowError(
                f"Job flow for cluster {cluster.value} is not registered"
            )
    
    async def list_available_clusters(self) -> dict[sites.Cluster, str]:
        """
        List the clusters with available job flows in this manager.
        
        Returns a dict of each cluster to either an error message if the cluster is not
        available or None.
        
        Does not take whether the cluster is marked as active or not into consideration.
        """
        results = {}
        async with asyncio.TaskGroup() as tg:
            for cluster, func in self._flows.items():
                results[cluster] = tg.create_task(func())
        return {cl: res.result().error for cl, res in results.items()}

    @alru_cache(maxsize=10, ttl=10)
    async def list_active_clusters(self) -> set[sites.Cluster]:
        """
        Get a list of all sites set to active.
        
        Returns all active clusters, available or not.
        """
        return await self._mongo.list_active_clusters()

    async def list_usable_clusters(self) -> set[sites.Cluster]:
        """
        Get a list of all usable sites, meaning they're available and active.
        """
        avail = await self.list_available_clusters()
        avail = {cl for cl, err in avail.items() if err is None}
        return await self.list_active_clusters() & avail

    async def set_site_active(self, cluster: sites.Cluster):
        """"
        Set a site to active.
        """
        await self._mongo.set_site_active(_not_falsy(cluster, "cluster"))

    async def set_site_inactive(self, cluster: sites.Cluster):
        """"
        Set a site to inactive.
        """
        await self._mongo.set_site_inactive(_not_falsy(cluster, "cluster"))


class InactiveJobFlowError(Exception):
    """ Thrown when an inactive job flow is requested. """ 


class UnavailableJobFlowError(Exception):
    """
    An error thrown when a job flow cannot be used due to unavailable resources or
    startup errors.
    """
