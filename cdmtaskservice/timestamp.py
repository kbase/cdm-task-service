"""
Contains functions for creating timestamps.
"""

from datetime import datetime, timezone


def utcdatetime() -> datetime:
    """ Creates a current datetime with the timezone set to UTC. """
    return datetime.now(timezone.utc)


def timestamp():
    """ Creates a timestamp from the current time in ISO8601 format. """
    return utcdatetime().isoformat()

