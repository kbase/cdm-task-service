"""
Static information about compute sites available to the CTS.
"""

from pydantic import BaseModel, Field
from typing import Annotated
from enum import Enum


class Cluster(str, Enum):
    """
    All clusters that have ever been registered in the system, including those that are no longer
    registered but may have associated job records.

    perlmutter-jaws: The Perlmutter cluster at NERSC run via JAWS.  
    lawrencium-jaws: The Lawrencium cluster at LBNL run via JAWS.  
    kbase: KBase compute nodes.
    """

    PERLMUTTER_JAWS = "perlmutter-jaws"
    LAWRENCIUM_JAWS = "lawrencium-jaws"
    KBASE = "kbase"


class SubmittableCluster(str, Enum):
    """
    Clusters for job submission..

    perlmutter-jaws: The Perlmutter cluster at NERSC run via JAWS.  
    kbase: KBase compute nodes.
    """
    
    # NOTE: If Lawrencium (lawrencium-jaws) is reactivated, add it back here and restore the
    # LawrenciumJAWSRunner instantiation and flow registration — see jaws_flows_provider.py
    # and app_state.py.

    PERLMUTTER_JAWS = "perlmutter-jaws"
    KBASE = "kbase"


class NodeType(BaseModel):
    """ Represents a class of nodes within a compute site. """

    nodes: Annotated[int | None, Field(
        examples=[3042],
        description="The number of nodes of this type, or null if the count is not static."
    )] = None
    cpus_per_node: Annotated[int, Field(
        examples=[256],
        description="The maximum number of virtual CPUs per node.",
    )]
    memory_per_node_gb: Annotated[int, Field(
        examples=[492],
        # GB, not GiB
        description="The maximum amount of memory per node in GB."
    )]
    max_runtime_min: Annotated[int, Field(
        examples=[2 * 24 * 60 - 15],
        description="The maximum runtime of a job container in minutes."
    )]
    gpus_per_node: Annotated[int, Field(
        examples=[4],
        description="The number of GPUs per node."
    )] = 0
    notes: Annotated[list[str], Field(
        examples=[["These nodes have 4 A100 GPUs each."]],
        description="Any notes about this node type."
    )] = []


class ComputeSite(BaseModel):
    """ Represents a remote compute site. """

    cluster: Annotated[Cluster, Field(
        examples=[Cluster.PERLMUTTER_JAWS.value],
        description="The site identifier",
    )]
    node_types: Annotated[list[NodeType], Field(
        description="The node types available at this site."
    )]
    notes: Annotated[list[str], Field(
        examples=[["Queue times are typically shorter here."]],
        description="Any notes about the site."
    )] = []


# https://jaws-docs.jgi.doe.gov/en/latest/Resources/compute_resources.html
PERLMUTTER_JAWS = ComputeSite(
    cluster=Cluster.PERLMUTTER_JAWS,
    node_types=[
        NodeType(
            nodes=3072,
            cpus_per_node=2 * 2 * 64,  # 2 CPUs × 64 cores × 2 hyperthreads
            memory_per_node_gb=492,  # in GB, not GiB, per the JAWS team
            max_runtime_min=2 * 24 * 60 - 15,
            notes=[
                "Standard nodes, no GPUS",
                "Queue times are on the order of hours to days"
            ],
        ),
        NodeType(
            nodes=1536,
            cpus_per_node=2 * 64,  # 1 CPUs × 64 cores × 2 hyperthreads
            memory_per_node_gb=236,  # in GB, not GiB, per the JAWS team
            max_runtime_min=24 * 60 - 15,
            gpus_per_node=4,
            notes=[
                "GPUs have 40GB of memory",
                "Queue times are on the order of several days"
            ]
        ),
    ],
    notes=["The Perlmutter supercomputer at NERSC, serviced by the JAWS job running system."]
)


# https://jaws-docs.jgi.doe.gov/en/latest/Resources/compute_resources.html
LAWRENCIUM_JAWS = ComputeSite(
    cluster=Cluster.LAWRENCIUM_JAWS,
    node_types=[
        NodeType(
            nodes=8,
            cpus_per_node=32,
            memory_per_node_gb=492,  # in GB, not GiB, per the JAWS team
            max_runtime_min=3 * 24 * 60 - 15,
        ),
    ],
    notes=[
        "The Lawrencium cluster at LBNL, serviced by the JAWS job running system.",
        "Queue times are typically shorter here for smaller jobs."
    ]
)


KBASE = ComputeSite(
    cluster=Cluster.KBASE,
    node_types=[
        NodeType(
            nodes=None,
            cpus_per_node=84 * 2,
            memory_per_node_gb=990,  # Leave 10GB for overhead
            # 7 days; the condor client adds a 6-hour buffer
            max_runtime_min=7 * 24 * 60,
            notes=["Standard nodes, no GPUS"],
        ),
        NodeType(
            nodes=None,
            cpus_per_node=256,
            memory_per_node_gb=990,  # Leave 10GB for overhead
            max_runtime_min=7 * 24 * 60,
            # 7 days; the condor client adds a 6-hour buffer
            gpus_per_node=4,
            notes=["GPUs have 80GB of memory"],
        ),
    ],
    notes=[
        "The DOE Systems Biology Knowledge Base compute systems.",
        "The number of nodes may be adjusted up or down to support the needs of KBase "
        "as a whole.",
        "Queue times are highly dependent on the nodes available and user demand but "
        "are typically short"
    ]
)


CLUSTER_TO_SITE = {
    Cluster.PERLMUTTER_JAWS: PERLMUTTER_JAWS,
    Cluster.LAWRENCIUM_JAWS: LAWRENCIUM_JAWS,
    Cluster.KBASE: KBASE,
}
""" A mapping of compute clusters to their site information. """


CLUSTER_TO_EXECUTION_TYPE = {
    Cluster.PERLMUTTER_JAWS: False,
    Cluster.LAWRENCIUM_JAWS: False,
    Cluster.KBASE: True,
}
"""
A mapping of compute clusters to their job container management type.

True - managed by this service
False - managed by an external service (e.g. JAWS)
"""

MAX_GPUS = max([nt.gpus_per_node for s in CLUSTER_TO_SITE.values() for nt in s.node_types])
""" The maximum number of GPUs that can be requested for a container across all clusters. """


MAX_CPUS = max([nt.cpus_per_node for s in CLUSTER_TO_SITE.values() for nt in s.node_types])
"""
The maximum number of cpus that can be requested for a container across all clusters.
"""


MAX_MEM_GB = max([nt.memory_per_node_gb for s in CLUSTER_TO_SITE.values() for nt in s.node_types])
""" The maximum amount of memory that can be requested for a container across all clusters. """


MAX_RUNTIME_MIN = max(
    [nt.max_runtime_min for s in CLUSTER_TO_SITE.values() for nt in s.node_types]
)
""" The maximum runtime that can be requested for a container across all clusters. """
