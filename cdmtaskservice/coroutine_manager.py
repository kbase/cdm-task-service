"""
A manager of coroutines.

Specifically, manages coroutines that need to run but the current thread or coroutine does not
want to await them. As such, any return value of the coroutine is discarded. Keeps a reference
to the coroutine task so that it's not garbage collected.
"""

import asyncio
import logging
from typing import Coroutine, Any

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy


class CoroutineWrangler:
    """ The coroutine manager. """
    
    def __init__(self):
        """ Create the coroutine manager. """
        self._coros = set()
        self._destroy = False

    # TODO CODE this doesn't need to be async. Would need to manually test a bunch of changes
    #           so leave as is for now
    async def run_coroutine(self, coro: Coroutine[None, Any, None]):
        """ Run a coroutine to completion. """
        if self._destroy:
            raise ValueError("Manager is destroyed")
        task = asyncio.create_task(self._exception_wrapper(_not_falsy(coro, "coro")))
        self._coros.add(task)
        task.add_done_callback(self._coros.discard)
        logging.getLogger(__name__).info(
            f"Running coroutine {coro}. {len(self._coros)} coroutines running."
        )
        
    async def _exception_wrapper(self, coro):
        try:
            await coro
        except Exception as e:
            logging.getLogger(__name__).exception(
                f"Coroutine {coro} threw an unhandled exception: {e}"
            )
            # Nothing else can be done

    def destroy(self):
        """ Cancel all coroutines and destroy the manager. """
        self._destroy = True
        for coro in self._coros:
            coro.cancel()
