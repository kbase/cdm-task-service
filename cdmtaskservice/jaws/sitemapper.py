"""
Maps CTS Clusters to the JAWS name for the site.
"""

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice import sites


_CLUSTER_TO_JAWS_SITE = {
    # It seems very unlikely, but we may need to add other JAWS sites for groups other than kbase
    # If we do that the perlmutter-jaws name is unfortunate since there are multiple virtual
    # sites there
    sites.Cluster.PERLMUTTER_JAWS: "kbase",  # what would they call a site actually at kbase...?
    sites.Cluster.LAWRENCIUM_JAWS: "jgi",  # historical name
}


def get_jaws_site(cluster: sites.Cluster) -> str:
    """
    Get the JAWS site name for a cluster.
    """
    site = _CLUSTER_TO_JAWS_SITE.get(_not_falsy(cluster, "cluster"))
    if not site:
        raise ValueError(f"No JAWS site for cluster {cluster.value}")
    return site
