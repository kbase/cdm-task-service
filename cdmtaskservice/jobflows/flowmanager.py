"""
Handles what job flows are available to users and their current state.
"""

import asyncio
from async_lru import alru_cache
from dataclasses import dataclass
from typing import Awaitable, Callable

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice import sites


class JobFlow:
    """
    A super class for job flows.
    """
    # This could be developed into an abstract class if we want to be strict about typing.


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
    
    async def list_available_clusters(self) -> set[sites.Cluster]:
        """
        List the clusters with available job flows in this manager.
        
        Returns all available clusters, active or not.
        """
        results = {}
        async with asyncio.TaskGroup() as tg:
            for cluster, func in self._flows.items():
                results[cluster] = tg.create_task(func())
        return {cl for cl, res in results.items() if res.result().jobflow}

    @alru_cache(maxsize=10, ttl=10)
    async def list_active_clusters(self) -> set[sites.Cluster]:
        """
        Get a list of all sites set to active.
        
        Returns all active clusters, available or not.
        """
        return await self._mongo.list_active_clusters()

    async def list_usable_clusters(self) -> set[sites.Cluster]:
        """
        Get a list of all usable sites, meaning they're availble and active.
        """
        return await self.list_active_clusters() & await self.list_available_clusters()

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
