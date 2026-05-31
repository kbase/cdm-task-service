"""
Utility to run a container image.
"""

from dataclasses import dataclass
import datetime
import docker
from functools import lru_cache
import logging
from pathlib import Path
import signal
import threading
from typing import Awaitable, Callable

from cdmtaskservice.arg_checkers import require_string as _require_string, not_falsy as _not_falsy


@dataclass
class ContainerResult:
    """Result from running a container."""
    exit_code: int
    runtime_seconds: float
    cpu_hours: float | None
    max_memory_bytes: int | None


@lru_cache
def _get_client():
    # don't create the client on module load
    return docker.from_env()


def _make_log_thread(
    container: docker.models.containers.Container, path: Path, stdout: bool
) -> threading.Thread:
    def stream():
        with path.open("wb") as f:
            for chunk in container.logs(stdout=stdout, stderr=not stdout, stream=True, follow=True):
                f.write(chunk)
                f.flush()
    return threading.Thread(target=stream, daemon=True)


def _make_stats_thread(
    container, stop_event: threading.Event
) -> tuple[threading.Thread, dict]:
    """
    Create a thread that streams container stats, returning the thread and a result dict.

    The result dict is populated when the thread completes with:
      max_memory_bytes - peak memory in bytes across all samples, or None if unavailable.
        This is sampled at ~1s resolution, not the kernel high-water mark.
      cpu_total_ns - cumulative CPU nanoseconds at container exit, or None if unavailable.

    The stats stream does not self-terminate when the container exits. The caller must set
    stop_event to signal the thread to stop after the next sample (~1s) and join it.
    """
    result = {}

    def stream():
        max_memory = 0
        last_cpu_ns = 0
        gen = container.stats(stream=True, decode=True)
        try:
            for stat in gen:
                if stop_event.is_set():
                    break
                mem = stat.get("memory_stats", {}).get("usage", 0)
                if mem:
                    max_memory = max(max_memory, mem)
                cpu = stat.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0)
                if cpu:
                    last_cpu_ns = cpu
        except Exception:
            logging.getLogger(__name__).exception("Error streaming container stats")
        finally:
            try:
                gen.close()
            except Exception:
                logging.getLogger(__name__).exception("Error closing container stats stream")
        result["max_memory_bytes"] = max_memory or None
        result["cpu_total_ns"] = last_cpu_ns or None

    return threading.Thread(target=stream, daemon=True), result


def _stream_data(
    container: docker.models.containers.Container, stdout_path: Path, stderr_path: Path
) -> dict:
    """
    Stream container logs and stats in background threads, blocking until all complete.
    Returns the stats result dict from _make_stats_thread.
    """
    stop_stats = threading.Event()
    stats_thread, stats = _make_stats_thread(container, stop_stats)
    log_threads = [
        _make_log_thread(container, stdout_path, stdout=True),
        _make_log_thread(container, stderr_path, stdout=False),
    ]
    for t in [*log_threads, stats_thread]:
        t.start()
    for t in log_threads:
        t.join()
    stop_stats.set()
    stats_thread.join()
    return stats


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
) -> ContainerResult:
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

    Returns a ContainerResult with the exit code, wall-clock runtime in seconds from the Docker
    daemon's StartedAt/FinishedAt timestamps, and optionally peak memory usage in bytes and total
    CPU usage in hours collected by streaming stats during container execution.
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
        cap_drop=["ALL"],
        security_opt=["no-new-privileges:true"],
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

        stats = _stream_data(container, stdout_path, stderr_path)

        result = container.wait()
        exit_code = result["StatusCode"]

        container.reload()
        started_at = datetime.datetime.fromisoformat(container.attrs["State"]["StartedAt"])
        finished_at = datetime.datetime.fromisoformat(container.attrs["State"]["FinishedAt"])
        runtime_seconds = (finished_at - started_at).total_seconds()

        cpu_total_ns = stats.get("cpu_total_ns")
        cpu_hours = cpu_total_ns / (1e9 * 3600) if cpu_total_ns is not None else None

        return ContainerResult(
            exit_code=exit_code,
            runtime_seconds=runtime_seconds,
            cpu_hours=cpu_hours,
            max_memory_bytes=stats.get("max_memory_bytes"),
        )
    finally:
        try:
            container.remove(force=True)
        except docker.errors.APIError as e:
            logr.exception(f"Cleanup failed: {e}")
