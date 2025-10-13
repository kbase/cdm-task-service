""" Run the executor. """

import asyncio
import sys

from cdmtaskservice.externalexecution.executor import run_executor


if __name__ == "__main__":
    asyncio.run(run_executor(sys.stdout, sys.stderr))  # allow testing via replacing streams
