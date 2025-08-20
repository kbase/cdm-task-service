"""
Configuration for interacting with JAWS.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class JAWSConfig:
    """ Configuration information for interacting with the JAWS service. """
    
    # TODO CODE check args aren't None / whitespace only. Not a lib class so YAGNI for now
    
    token: str
    """ The JAWS authentication token. """
    
    user: str
    """ The username associated with the token. """
    
    group: str
    """ The JAWS group to use when submitting a job. """
    
    url: str
    """ The URL of the JAWS central server. """
