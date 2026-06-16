"""
Utility to run a container image.
"""

import asyncio
from dataclasses import dataclass
import datetime
import docker
import logging
from pathlib import Path
import threading

from cdmtaskservice.arg_checkers import require_string as _require_string, not_falsy as _not_falsy


@dataclass
class ContainerResult:
    """Result from running a container."""
    exit_code: int
    runtime_seconds: float
    cpu_hours: float | None
    max_memory_bytes: int | None


def _make_log_thread(
    container: docker.models.containers.Container, path: Path, stdout: bool
) -> threading.Thread:
    def stream():
        with path.open("wb") as f:
            for chunk in container.logs(
                stdout=stdout, stderr=not stdout, stream=True, follow=True
            ):
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


class RunningContainer:
    """
    A handle to a running Docker container. Not concurrency-safe.

    Obtain instances via ContainerCreator.start_container().
    """

    def __init__(
        self,
        container: docker.models.containers.Container,
        stdout_path: Path,
        stderr_path: Path,
    ):
        self._container = container
        self._logr = logging.getLogger(__name__)
        self._stop_stats = threading.Event()
        self._stats_thread, self._stats_result = _make_stats_thread(container, self._stop_stats)
        self._log_threads = [
            _make_log_thread(container, stdout_path, stdout=True),
            _make_log_thread(container, stderr_path, stdout=False),
        ]
        for t in [*self._log_threads, self._stats_thread]:
            t.start()
        self._result: ContainerResult | None = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cancel()

    def _join_threads(self, timeout: float | None = None):
        for t in self._log_threads:
            t.join(timeout=timeout)
        self._stop_stats.set()
        self._stats_thread.join(timeout=timeout)

    async def _finish(self, *, cancel: bool = False) -> ContainerResult:
        if self._result:
            return self._result
        try:
            if cancel:
                await asyncio.to_thread(self._stop_container)
            # Run in a thread so the event loop stays free during container execution
            await asyncio.to_thread(self._join_threads, 30 if cancel else None)
            # Saved so subsequent calls return the cached result without querying
            # the removed container if there's a race, since _collect_result is synchronous
            self._result = self._result or self._collect_result()
            return self._result
        finally:
            self._remove_container()

    def _stop_container(self):
        self._logr.info(f"Stopping container {self._container.short_id}")
        try:
            self._container.stop(timeout=10)
            self._logr.info(f"Stopped container {self._container.short_id}")
        except docker.errors.NotFound:
            pass
        except Exception as e:
            self._logr.exception(f"Failed to stop container: {e}")

    async def wait(self) -> ContainerResult:
        """
        Wait for the container to complete.

        Returns a ContainerResult with the exit code, wall-clock runtime, and optionally
        peak memory and CPU hours. Idempotent — subsequent calls return the cached result.
        """
        return await self._finish()

    async def cancel(self) -> ContainerResult:
        """
        Stop the container and wait for all log and stats threads to exit.

        Returns a ContainerResult with the exit code, wall-clock runtime, and optionally
        peak memory and CPU hours. Idempotent — subsequent calls return the cached result.
        """
        return await self._finish(cancel=True)

    def _collect_result(self) -> ContainerResult:
        exit_code = self._container.wait()["StatusCode"]
        self._container.reload()
        started_at = datetime.datetime.fromisoformat(
            self._container.attrs["State"]["StartedAt"]
        )
        finished_at = datetime.datetime.fromisoformat(
            self._container.attrs["State"]["FinishedAt"]
        )
        runtime_seconds = (finished_at - started_at).total_seconds()
        cpu_total_ns = self._stats_result.get("cpu_total_ns")
        return ContainerResult(
            exit_code=exit_code,
            runtime_seconds=runtime_seconds,
            cpu_hours=cpu_total_ns / (1e9 * 3600) if cpu_total_ns is not None else None,
            max_memory_bytes=self._stats_result.get("max_memory_bytes"),
        )

    def _remove_container(self):
        try:
            self._container.remove(force=True)
        except docker.errors.NotFound:
            pass
        except docker.errors.APIError as e:
            self._logr.exception(f"Cleanup failed: {e}")



class ContainerCreator:
    """ Creates and starts Docker containers. """

    def __init__(self):
        self._client = docker.from_env()

    async def start_container(
        self,
        image: str,
        stdout_path: Path,
        stderr_path: Path,
        *,
        command: list[str] | None = None,
        env: dict[str, str] | None = None,
        mounts: dict[str, tuple[str, bool]] | None = None,
    ) -> RunningContainer:
        """
        Start a container and return a handle to it.

        image - the image to run.
        stdout_path - a file where stdout logs should be streamed.
        stderr_path - a file where stderr logs should be streamed.
        command - a command to provide to the container.
        env - a map from environment variable name to value for the container.
        mounts - mounting directives. A map from host path to a tuple of:
            * the container mount path
            * a boolean: True for read-write, False for read-only.
        """
        _require_string(image, "image")
        _not_falsy(stdout_path, "stdout_path")
        _not_falsy(stderr_path, "stderr_path")
        volumes = {
            host: {"bind": container_path, "mode": "rw" if rw else "ro"}
            for host, (container_path, rw) in (mounts or {}).items()
        }
        container = self._client.containers.run(
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
        logging.getLogger(__name__).info(f"Container started: {container.short_id}")
        return RunningContainer(container, stdout_path, stderr_path)
