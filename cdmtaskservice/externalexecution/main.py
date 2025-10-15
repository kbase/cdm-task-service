""" Run the executor. """

import asyncio
import sys

from cdmtaskservice.externalexecution.executor import run_executor


if __name__ == "__main__":
    res = asyncio.run(run_executor(sys.stderr))  # allow testing via replacing streams
    if not res:
        sys.exit(1)
