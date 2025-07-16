"""
Static information about compute sites available to the CTS.
"""

from pydantic import BaseModel, Field
from typing import Annotated
from enum import Enum


class Cluster(str, Enum):
    """
    The location where a job should run.
    
    perlmutter-jaws: The Perlmutter cluster at NERSC run via JAWS.
    """

    PERLMUTTER_JAWS = "perlmutter-jaws"
    # TODO LAWRENCIUM add when available


class ComputeSite(BaseModel):
    """ Represents a remote compute site. """
    
    cluster: Annotated[Cluster, Field(
        examples=[Cluster.PERLMUTTER_JAWS.value],
        description="The site identifier",
    )]
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
        examples=[47 * 60 + 15],
        description="The maximum runtime of a job container in minutes."
    )]
    notes: Annotated[list[str], Field(
        examples=[["Queue times are typically shorter here."]],
        description="Any notes about the site."
    )] = []


# https://jaws-docs.jgi.doe.gov/en/latest/Resources/compute_resources.html
PERLMUTTER_JAWS = ComputeSite(
    cluster=Cluster.PERLMUTTER_JAWS,
    cpus_per_node=256,
    memory_per_node_gb=492,  # in GB, not GiB, per the JAWS team
    max_runtime_min=47 * 60 + 15,
    notes=["The Perlmutter supercomputer at NERSC, serviced by the JAWS job running system."]
)


# TODO LAWRENCIUM add LRC


CLUSTER_TO_SITE = {
    Cluster.PERLMUTTER_JAWS: PERLMUTTER_JAWS
}
""" A mapping of compute clusters to their site information. """


MAX_CPUS = max([cl.cpus_per_node for cl in CLUSTER_TO_SITE.values()])
"""
The maximum number of cpus that can be requested for a container across all clusters.
"""


MAX_MEM_GB = max([cl.memory_per_node_gb for cl in CLUSTER_TO_SITE.values()])
""" The maximum amount of memory that can be requested for a container across all clusters. """


MAX_RUNTIME_MIN = max([cl.max_runtime_min for cl in CLUSTER_TO_SITE.values()])
""" The maximum runtime that can be requested for a container across all clusters. """
