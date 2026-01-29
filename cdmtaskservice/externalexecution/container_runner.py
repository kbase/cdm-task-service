"""
Utility to run a container image.
"""

import docker
from functools import lru_cache
import logging
from pathlib import Path
import signal
import threading
from typing import Awaitable, Callable

from cdmtaskservice.arg_checkers import require_string as _require_string, not_falsy as _not_falsy


@lru_cache
def _get_client():
    # don't create the client on module load
    return docker.from_env()


def _stream_logs(
    container: docker.models.containers.Container, stdout_path: Path, stderr_path: Path
):
    # Stream logs in a background threads to avoid blocking main thread and signals
    def stream_file(path: Path, stdout: bool):
        with path.open("wb") as f:
            for chunk in container.logs(stdout=stdout, stderr=not stdout, stream=True, follow=True):
                f.write(chunk)
                f.flush()

    threads = [
        threading.Thread(target=stream_file, args=(stdout_path, True), daemon=True),
        threading.Thread(target=stream_file, args=(stderr_path, False), daemon=True),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


async def run_container(
    image: str,
    stdout_path: Path,
    stderr_path: Path,
    *,
    command: list[str] | None = None,
    env: dict[str, str] | None = None,
    mounts: dict[str, tuple[str, bool]] | None = None,
    post_start_callback: Awaitable[None] | None = None,
    sigterm_callback: Callable[[int], None] | None = None,
):
    """
    Run a container and wait for it to complete.
    
    image - the image to run.
    stdout_path - a file where stdout logs should be streamed.
    stderr_path - a file where stderr logs should be streamed.
    command - a command to provide to the container.
    env - a map from environment variable to environment value to provide to the container
    mounts - mounting directives for the container. A map from the host mount path to a tuple of
        * the container mount path
        * A boolean denoting whether the mounts should be read write (True) or just read (False)
    post_start_callback - an awaitable that will be awaited when the container has started but
        before streaming logs.
    sigterm_callback - a callable that will be called if a SIGTERM or SIGINT is sent to the
        process, after a stop signal is sent to the docker container. The argument is the signal
        number.
    """
    _require_string(image, "image")
    _not_falsy(stdout_path, "stdout_path")
    _not_falsy(stderr_path, "stderr_path")
    logr = logging.getLogger(__name__)

    client = _get_client()
    mounts = mounts or {}
    volumes = {
        host: {"bind": container, "mode": "rw" if rw else "ro"}
        for host, (container, rw) in mounts.items()
    }

    container = client.containers.run(
        image=image,
        entrypoint=command,
        environment=env,
        volumes=volumes,
        detach=True,
        tty=False,
        remove=False,  # Don't remove immediately to ensure logs are written
    )
    
    def cleanup(signum, frame):
        logr.info(f"Got signum {signum}")
        if container:
            logr.info(f"Stopping container {container.short_id}")
            try:
                container.stop(timeout=10)
                logr.info(f"Stopped container {container.short_id}")
            except Exception as e:
                logr.exception(f"Failed to stop container: {e}")
        # Probably the right way to do this is to turn this into a class and add a stop()
        # method or something, but this is internal only code for now and this way is faster.
        # If needed move the signal catching out of here and make the class.
        sigterm_callback(signum)

    try:
        signal.signal(signal.SIGTERM, cleanup)
        signal.signal(signal.SIGINT, cleanup)
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
