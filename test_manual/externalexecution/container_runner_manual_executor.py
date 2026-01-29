from pathlib import Path

from cdmtaskservice.externalexecution.container_runner import run_container
import asyncio
import logging

IMAGE = "ghcr.io/kbasetest/cts_test_image:0.1.2"

STDOUT = Path("./stdout.txt")
STDERR = Path("./stderr.txt")

ARGS = ["python", "/opt/tester.py", "-s", "1000"]

MOUNTS = {
    str(Path("~/github/kbase/cdm-task-service/mount_test_rw").expanduser()): ("/rw", True),
    str(Path("~/github/kbase/cdm-task-service/mount_test_ro").expanduser()): ("/ro", False)
}

logging.basicConfig(level=logging.INFO)

async def start_cb():
    print("started container")


async def main():
    await run_container(
        IMAGE,
        STDOUT,
        STDERR,
        command=ARGS,
        mounts=MOUNTS,
        post_start_callback=start_cb(),
        sigterm_callback=lambda signum: print(f"exited: {signum}")
    )

if __name__ == "__main__":
    asyncio.run(main())
