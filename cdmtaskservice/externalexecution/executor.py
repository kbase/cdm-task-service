"""
The main CTS external executor class.
"""

import asyncio
import sys
from typing import TextIO

from cdmtaskservice.externalexecution.config import Config
from cdmtaskservice.git_commit import GIT_COMMIT
from cdmtaskservice.version import VERSION


async def main(stdout: TextIO, stderr: TextIO):
    stdout.write(f"Executor version: {VERSION} githash: {GIT_COMMIT}\n")
    cfg = Config()
    stdout.write("Executor config:\n")
    for k, v in cfg.safe_dump().items():
        stdout.write(f"{k}: {v}\n")


if __name__ == "__main__":
    asyncio.run(main(sys.stdout, sys.stderr))  # allow testing via replacing streams
