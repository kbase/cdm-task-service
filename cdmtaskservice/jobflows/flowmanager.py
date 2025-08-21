"""
Handles what job flows are available to users and their current state.
"""

from typing import Awaitable, Callable

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.exceptions import UnavailableJobFlowError
from cdmtaskservice import sites
from dataclasses import dataclass
import asyncio

# Currently this isn't too useful since there's only one job flow, so if it's not available
# the service might as well not start up.
# In the future, this module should be able to enable and disable job flowa and report their
# status to users.


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
    
    def __init__(self):
        """ Create the job flow manager """
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
        # TODO DYNAMICFLOWS check DB to see if a flow has been marked inactive
        if _not_falsy(cluster, "cluster") in self._flows:
            floworerr = await self._flows[cluster]()
            if floworerr.error:
                raise UnavailableJobFlowError(
                    f"Job flow for cluster {cluster.value} is unavailable: {floworerr.error}"
                )
            return floworerr.jobflow
        else:
            raise ValueError(f"Job flow for cluster {cluster.value} is not registered")
    
    async def list_clusters(self) -> set[sites.Cluster]:
        """ List the clusters with active job flows in this manager. """
        # TODO DYNAMICFLOWS check DB to see if a flow has been marked inactive
        results = {}
        async with asyncio.TaskGroup() as tg:
            for cluster, func in self._flows.items():
                results[cluster] = tg.create_task(func())
        return {cl for cl, res in results.items() if res.result().jobflow}


class InactiveJobFlowError(Exception):
    """ Thrown when an inactive job flow is requested. """ 
