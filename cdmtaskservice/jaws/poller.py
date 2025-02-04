"""
Polls a JAWS job until the job is complete and returns the job information or a fatal error
occurs.
"""

import asyncio
import logging
from typing import Any

from cdmtaskservice import logfields
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.jaws import client as jaws_client
from cdmtaskservice.jaws.client import NoSuchJAWSJobError


_BACKOFF = [10, 30, 60, 120, 300]


async def poll(cli: jaws_client.JAWSClient, job_id: str, run_id: str) -> dict[str, Any]:
    """
    Poll the JAWS server for a job until the job is complete or a fatal error occurs.
    
    cli - the JAWS client.
    job_id - the CTS job ID.
    run_id - the JAWS run ID.
    """
    _not_falsy(cli, "cli")
    _require_string(job_id, "job_id")
    _require_string(run_id, "run_id")
    logr = logging.getLogger(__name__)
    backoff_index = -1
    while True:
        backoff_index, backoff = _get_next_backoff(backoff_index)
        try:
            res = await cli.status(run_id)
            if jaws_client.is_done(res):
                return res
            logr.info(
                "Polled JAWS run",
                extra={
                    logfields.JOB_ID: job_id,
                    logfields.JAWS_RUN_ID: run_id,
                    logfields.NEXT_ACTION_SEC: backoff
                }
            )
        except NoSuchJAWSJobError:  # fatal
            raise
        except Exception:
            # TODO ERRORHANDLING it'll probably take some experience to determine fatal vs.
            #                    non-fatal errors
            logr.exception(
                f"*** Failed to get status from JAWS. Trying again in {backoff}s ***",
                extra={
                    logfields.JOB_ID: job_id,
                    logfields.JAWS_RUN_ID: run_id,
                    logfields.NEXT_ACTION_SEC: backoff
                }
            )
        await asyncio.sleep(backoff)


def _get_next_backoff(backoff_index: int):
    backoff_index = backoff_index + 1 if backoff_index < len(_BACKOFF) - 1 else backoff_index
    return backoff_index, _BACKOFF[backoff_index]
