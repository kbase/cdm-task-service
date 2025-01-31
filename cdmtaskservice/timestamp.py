"""
Contains functions for creating timestamps.
"""

from datetime import datetime, timezone


def utcdatetime() -> datetime:
    """ Creates a current datetime with the timezone set to UTC. """
    return datetime.now(timezone.utc)
