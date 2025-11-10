"""
Utility to run a container image.
"""

import docker
from functools import lru_cache
import logging
from pathlib import Path
import threading
from typing import Awaitable

from cdmtaskservice.arg_checkers import require_string as _require_string, not_falsy as _not_falsy


@lru_cache
def _get_client():
    # don't create the client on module load
    return docker.from_env()


def _stream_logs(
    container: docker.models.containers.Container, stdout_path: Path, stderr_path: Path
):
    # Stream stderr in a background thread
    def stream_err():
        with stderr_path.open("wb") as f_err:
            for chunk in container.logs(stdout=False, stderr=True, stream=True, follow=True):
                f_err.write(chunk)
                f_err.flush()

    err_thread = threading.Thread(target=stream_err, daemon=True)
    err_thread.start()

    # Stream stdout in the main thread
    with stdout_path.open("wb") as f_out:
        for chunk in container.logs(stdout=True, stderr=False, stream=True, follow=True):
            f_out.write(chunk)
            f_out.flush()

    err_thread.join()  # ensure stderr is done


async def run_container(
    image: str,
    stdout_path: Path,
    stderr_path: Path,
    *,
    command: list[str] | None = None,
    env: dict[str, str] | None = None,
    mounts: dict[str, str] | None = None,
    post_start_callback: Awaitable[None] | None = None
):
    """
    Run a container and wait for it to complete.
    
    image - the image to run.
    stdout_path - a file where stdout logs should be streamed.
    stderr_path - a file where stderr logs should be streamed.
    command - a command to provide to the container.
    env - a map from environment variable to environment value to provide to the container
    mounts - a map from host mount path to container mount path to provide to the container
    post_start_callback - an awaitable that will be awaited when the container has started but
        before streaming logs.
    """
    _require_string(image, "image")
    _not_falsy(stdout_path, "stdout_path")
    _not_falsy(stderr_path, "stderr_path")
    logr = logging.getLogger(__name__)

    client = _get_client()
    mounts = mounts or {}
    volumes = {host: {"bind": container, "mode": "rw"} for host, container in mounts.items()}

    container = client.containers.run(
        image=image,
        entrypoint=command,
        environment=env,
        volumes=volumes,
        detach=True,
        tty=False,
        remove=False,
    )

    try:
        logr.info(f"Container started: {container.short_id}")
        if post_start_callback:
            await post_start_callback

        _stream_logs(container, stdout_path, stderr_path)

        result = container.wait()
        return result.get("StatusCode", -1)
    finally:
        try:
            container.remove(force=True)
        except docker.errors.APIError as e:
            logr.exception(f"Cleanup failed: {e}")
