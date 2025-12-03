"""
Class to manage resources that need to be destroyed on shutdown or error.
"""

import logging
from typing import Awaitable, Callable

from cdmtaskservice.arg_checkers import require_string as _require_string, not_falsy as _not_falsy
import asyncio


class ResourceDestructor:
    """
    Register and destroy resources.
    """
    
    def __init__(self):
        """ Create the resource destructor. """
        self._resources = []  # allow resources with the same name
    
    def register(self, name: str, destructor: Awaitable | Callable[[], None]):
        """
        Register a resource for eventual destruction.
        
        name - the name of the resource for logging purposes.
        destructor - a sync function to be called or an aysnc awaitable to awaited to destroy
            the resource.
        """
        self._resources.append(
            (_require_string(name, "name"), _not_falsy(destructor, "destructor"))
        )
    
    async def destruct(self):
        # don't worry about multiple calls for now, add fixes if needed later
        logr = logging.getLogger(__name__)
        for name, destructor in self._resources:
            logr.info(f"Destroying {name}")
            if asyncio.iscoroutine(destructor):
                await destructor
            else:
                destructor()
