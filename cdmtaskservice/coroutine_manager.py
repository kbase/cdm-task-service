"""
A manager of coroutines.

Specifically, manages coroutines that need to run but the current thread or coroutine does not
want to await them. As such, any return value of the coroutine is discarded. Keeps a reference
to the coroutine task so that it's not garbage collected.
"""

import asyncio
import logging
from typing import Awaitable, Self

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy


_CLOSE_DELAY_SEC = 60


class CoroutineWrangler:
    """ The coroutine manager. """
    
    @classmethod
    async def create(cls) -> Self:
        """ Create the coroutine manager. """
        cw = CoroutineWrangler()
        cw._reaper_task = asyncio.create_task(cw._reaper())
        return cw
    
    def __init__(self):
        self._closedelay = _CLOSE_DELAY_SEC  # may want to make configurable?
        self._coros = []
        self._destroy = False

    async def _reaper(self):
        logr = logging.getLogger(__name__)
        while not self._destroy:
            await asyncio.sleep(self._closedelay)
            logr.info(f"Reaper processing {len(self._coros)} coroutines")
            self._coros = [coro for coro in self._coros if not coro.done()]
            logr.info(f"Reaper: {len(self._coros)} coroutines remaining")

    async def run_coroutine(self, coro: Awaitable):
        """ Run a coroutine to completion. """
        if self._destroy:
            raise ValueError("Manager is destroyed")
        task = asyncio.create_task(_not_falsy(coro, "coro"))
        self._coros.append(task)

    async def destroy(self):
        """ Cancel all coroutines and destroy the manager. """
        self._destroy = True
        self._reaper_task.cancel()
        for coro in self._coros:
            coro.cancel()
