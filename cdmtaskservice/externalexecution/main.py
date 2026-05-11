""" Run the executor. """

import asyncio
import os
import sys

from cdmtaskservice.externalexecution.executor import run_executor
import logging


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    cts_log_level = os.environ.get("CTS_LOG_LEVEL", "INFO").upper()
    try:
        logging.getLogger("cdmtaskservice").setLevel(cts_log_level)
    except ValueError as e:
        raise ValueError(
            f"Invalid CTS_LOG_LEVEL '{cts_log_level}': must be a standard log level "
            + "e.g. DEBUG, INFO, WARNING, ERROR, CRITICAL"
        ) from e
    exit_code = asyncio.run(run_executor(sys.stderr))  # allow testing via replacing streams
    # pick any old number that's less likely to collide with with container exit codes
    exit_code = 119 if exit_code < 0 else exit_code
    sys.exit(exit_code)
