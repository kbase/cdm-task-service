"""
Handles what job flows are available to users and their current state.
"""

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice import sites

# Currently this isn't too useful since there's only one job flow, so if it's not available
# the service might as well not start up.
# In the future, this module should be able to enable and disable job flowa and report their
# status to users.


class JobFlow:
    """
    A super class for job flows.
    """
    # This could be developed into an abstract class if we want to be strict about typing.


class JobFlowManager():
    
    def __init__(self):
        """ Create the job flow manager """
        self._flows = {}
        self._inactive = {}
        
    def register_flow(self, flow: JobFlow):
        """
        Add a running job flow to the manager.
        
        flow - the job flow.
        """
        # Longer term, the manager should also accept methods to start up / restart the job flow
        self._flows[_not_falsy(flow, "flow").get_cluster()] = flow
        self._inactive.pop(flow.get_cluster(), None)
        
    def mark_flow_inactive(self, cluster: sites.Cluster, reason: str):
        """
        Mark a flow as inactive.
        
        cluster - the cluster the job flow is associated with - a 1:1 relationship.
        reason - the reason why the job flow is unavailable.
        """
        self._flows.pop(cluster, None)
        self._inactive[_not_falsy(cluster, "cluster")] = _require_string(reason, "reason")
        
    def get_flow(self, cluster: sites.Cluster) -> JobFlow:
        """
        Get a job flow for a cluster.
        
        Throws an error if the flow is inactive.
        """
        if _not_falsy(cluster, "cluster") in self._flows:
            return self._flows[cluster]
        elif cluster in self._inactive:
            raise InactiveJobFlowError(
                f"Job flow for cluster {cluster.value} is inactive: {self._inactive[cluster]}"
            )
        else:
            raise ValueError(f"Job flow for cluster {cluster.value} is not registered")
    
    def list_clusters(self) -> set[sites.Cluster]:
        """ List the clusters with active job flows in this manager. """
        return set(self._flows.keys())
    
    def list_inactive_clusters(self) -> set[sites.Cluster]:
        """ List the clusters with inactive job flows in this manager. """
        return set(self._inactive.keys())


class InactiveJobFlowError(Exception):
    """ Thrown when an inactive job flow is requested. """ 
