"""
A client for submitting jobs to a remote HTCondor instance targeted for use by the CDM
Task Service.
"""

import asyncio
from classad2 import ClassAd, ExprTree
from dataclasses import dataclass
import enum
import htcondor2
import os
from pathlib import Path
import posixpath
from typing import Any, Callable, Collection, TypeVar
from yarl import URL

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, check_num as _check_num
from cdmtaskservice.condor.config import CondorClientConfig
from cdmtaskservice.config_s3 import S3Config
from cdmtaskservice import models


# Some of this stuff is pretty specific to KBase but the likelihood we'll ever be submitting
# to some other condor instance is pretty low, so don't worry about it 


_V = TypeVar("_V")

_JOB_TIMEOUT_MIN = 7 * 24 * 60
_JOB_TIMEOUT_FUDGE_FACTOR_MIN = 6 * 60


_AD_CLUSTER_ID = "ClusterId"
_AD_PROC_ID = "ProcId"
_AD_JOB_ID = "CTSJobID"
_AD_CONTAINER_NUMBER = "CTSContainerNumber"
_AD_CPU_SYS = "RemoteSysCpu"
_AD_CPU_USER = "RemoteUserCpu"
_AD_MEM = "MemoryUsage"
_AD_COMMITTED_TIME = "CommittedTime"
_AD_JOB_STATUS = "JobStatus"
_AD_HOLD_REASON = "HoldReason"
_AD_HOLD_REASON_CODE = "HoldReasonCode"
_AD_STATE = "_state"
_JOB_STATUS_HELD = 5


class ProcState(enum.Enum):
    """ The state of an HTCondor process. """
    QUEUED = "queued"
    RUNNING = "running"
    HELD = "held"
    COMPLETE = "complete"
    CANCELED = "canceled"
    OTHER = "other"
    MISSING = "missing"

    def is_healthy(self) -> bool:
        """ Return True if this state does not indicate a problem requiring intervention. """
        return self in {ProcState.QUEUED, ProcState.RUNNING, ProcState.COMPLETE}


_JOB_STATUS_TO_PROC_STATE = {
    1:                ProcState.QUEUED,    # Idle
    2:                ProcState.RUNNING,   # Running
    3:                ProcState.CANCELED,  # Removed
    4:                ProcState.COMPLETE,  # Completed
    _JOB_STATUS_HELD: ProcState.HELD,      # Held
    6:                ProcState.RUNNING,   # Transferring Output
    7:                ProcState.OTHER,     # Suspended
}


def _status_to_proc_state(status: int) -> ProcState:
    if status not in _JOB_STATUS_TO_PROC_STATE:
        raise ValueError(f"Unknown HTCondor job status: {status}")
    return _JOB_STATUS_TO_PROC_STATE[status]


_STATUS_ONLY = [_AD_PROC_ID, _AD_JOB_STATUS]
_STATUS_AND_HOLD = [_AD_PROC_ID, _AD_JOB_STATUS, _AD_HOLD_REASON, _AD_HOLD_REASON_CODE]

_LOCATIONS = ["Iwd", "Err", "Out", "UserLog"]
_IDS = [_AD_CLUSTER_ID, _AD_PROC_ID, _AD_JOB_ID, _AD_CONTAINER_NUMBER]
_RESOURCES = [
    "RequestCpus",
    "RequestDisk",
    "RequestMemory",
    "CpusProvisioned",
    "DiskProvisioned",
    "GPUsProvisioned",
    "CpusUsage",
    _AD_CPU_USER,
    _AD_CPU_SYS,
    "CumulativeRemoteSysCpu",
    "CumulativeRemoteUserCpu",
    "DiskUsage",
    "DiskUsage_RAW",
    "ResidentSetSize",
    "ResidentSetSize_RAW",
    "ImageSize",
    "ImageSize_RAW",
    _AD_MEM,
]
_CONDOR_DEETS = ["Owner", "User", "CondorVersion", "CondorPlatform"]
_INPUTS = ["Environment", "Requirements", "TransferInput", "LeaveJobInQueue"]
_JOB_STATE = [
    "Rank",
    "JobPrio",
    _AD_JOB_STATUS,
    "ExitCode",
    "ExitStatus",
    "JobRunCount",
    "NumRestarts",
    "NumJobStarts",
    _AD_HOLD_REASON,
    _AD_HOLD_REASON_CODE,
    "PeriodicHold",
    "VacateReason",
    "VacateReasonCode",
    "WantGracefulRemoval",
]
_FAIR_SHARE = ["ConcurrencyLimits", "AccountingGroup"]
_TIME = [
    "JobStartDate",
    _AD_COMMITTED_TIME,
    "CommittedSuspensionTime",
    "CompletionDate",
    "CumulativeSuspensionTime",
    "CumulativeTransferTime",
    "JobCurrentFinishTransferInputDate",
    "JobCurrentFinishTransferOutputDate",
    "JobCurrentStartDate",
    "JobCurrentStartExecutingDate",
    "JobCurrentStartTransferInputDate",
    "JobCurrentStartTransferOutputDate",
    "RemoteWallClockTime",
]
_OTHER = ["StartdPrincipal", "RemoteHost", "LastRemoteHost"]
_RETURNED_JOB_ADS = (
    _LOCATIONS + _IDS + _RESOURCES + _CONDOR_DEETS + _INPUTS + _JOB_STATE + _FAIR_SHARE + _TIME
    + _OTHER
)


# A lot of this is copied from
# https://github.com/kbase/execution_engine2/blob/develop/lib/execution_engine2/utils/Condor.py
STATIC_SUB = {
    #  Allow up to 12 hours of no response from job
    "JobLeaseDuration": "43200",
    #  Allow up to 12 hours for condor drain
    "MaxJobRetirementTime": "43200",
    # If a job exits incorrectly put it on hold
    "on_exit_hold": "ExitCode =!= 0",
    "should_transfer_files": "yes",
    "when_to_transfer_output": "on_exit_or_evict",
    "getenv": "False",
}


def condor_jobs_all_held(job_classads_as_dict: list[dict[str, Any]]) -> bool:
    """
    Given HTCondor job ClassAds converted to dictionaries for a job, return True if all of them
    are in the held state. 
    """
    _not_falsy(job_classads_as_dict, "job_classads_as_dict")
    return not {ad[_AD_JOB_STATUS] for ad in job_classads_as_dict} - {_JOB_STATUS_HELD}


@dataclass
class ProcDetails:
    """State and hold information for an HTCondor process."""
    state: ProcState
    hold_reason: str | None = None
    hold_reason_code: int | None = None


@dataclass
class CondorJobStats:
    """Stats derived from HTCondor ClassAds for a job."""
    cpu_hours: float | None
    max_memory: int | None
    runtime_seconds: float | None


def condor_job_stats(job_classads_as_dict: list[dict[str, Any]]) -> CondorJobStats:
    """
    Given HTCondor job ClassAds converted to dictionaries for a job, compute stats for the job.

    cpu_hours - total (RemoteSysCpu + RemoteUserCpu) / 3600
    max_memory - maximum MemoryUsage in bytes across all containers (MemoryUsage is in MiB)
    runtime_seconds - total CommittedTime in seconds across all containers
    """
    _not_falsy(job_classads_as_dict, "job_classads_as_dict")
    # Seems like condor uses MiB, although docs aren't great
    # https://htcondor.readthedocs.io/en/24.x/man-pages/condor_submit.html?utm_source=chatgpt.com#request_memory
    mems = [c[_AD_MEM] for c in job_classads_as_dict if _AD_MEM in c]
    cpu_sec = 0
    runtime_sec = 0
    for c in job_classads_as_dict:
        cpu_sec += (c.get(_AD_CPU_USER) or 0) + (c.get(_AD_CPU_SYS) or 0)
        runtime_sec += (c.get(_AD_COMMITTED_TIME) or 0)
    return CondorJobStats(
        cpu_hours=cpu_sec / 3600.0 if cpu_sec else None,
        max_memory=max(mems) * 1024 * 1024 if mems else None,
        runtime_seconds=float(runtime_sec) if runtime_sec else None,
    )


class CondorClient:
    """
    The condor client.
    """

    def __init__(
        self,
        schedd: htcondor2.Schedd,
        config: CondorClientConfig,
        s3config: S3Config,
        heartbeat_interval_min: int,
    ):
        """
        Create the client.

        schedd: An htcondor Schedd instance, configured to submit jobs to the cluster.
        config - the configuration for the client.
        s3Config - the configuration for the S3 instance where files are stored.
        heartbeat_interval_min - how often, in minutes, the executor sends a heartbeat.
        """
        self._schedd = _not_falsy(schedd, "schedd")
        self._config = _not_falsy(config, "config")
        self._heartbeat_interval_min = _check_num(heartbeat_interval_min, "heartbeat_interval_min", minimum=1)
        # Why this has to exist locally is beyond me
        Path(self._config.initial_dir).mkdir(parents=True, exist_ok=True)
        self._exe_url = config.get_executable_url()
        self._exe_name = self._get_name_from_url(self._exe_url)
        self._code_archive_url = config.get_code_archive_url()
        self._code_archive_name = self._get_name_from_url(self._code_archive_url)
        self._s3config = _not_falsy(s3config, "s3config")
        
    def _get_name_from_url(self, url: str) -> str:
        parsed = URL(url)
        if parsed.query_string or parsed.fragment:
            raise ValueError(
                f"Condor url {url} cannot contain query or fragment sections")

        # posixpath ensures use of "/"
        filename = posixpath.basename(parsed.path)
        if not filename:
            raise ValueError(f"Condor url {url} does not end in a file name")
        return filename
    
    def _get_environment(self, job: models.Job) -> str:
        env = {
            "JOB_ID": job.id,
            "CONTAINER_NUMBER": f"$$([{_AD_CONTAINER_NUMBER}])",
            "CODE_ARCHIVE": self._code_archive_name,
            "GLOBAL_CACHE_DIR": self._config.cache_dir,
            "SERVICE_ROOT_URL": self._config.service_root_url,
            "TOKEN_PATH": self._config.token_path,
            "S3_URL": self._s3config.get_url(self._config.use_S3_external_url),
            "S3_ACCESS_KEY": self._s3config.access_key,
            "S3_SECRET_PATH": self._config.s3_access_secret_path,
            "S3_ERROR_LOG_PATH": f"{self._s3config.error_log_path.strip('/')}/{job.id}",
            "JOB_UPDATE_TIMEOUT_MIN": self._config.job_update_timeout_min,
            "JOB_TIMEOUT_MIN": _JOB_TIMEOUT_MIN,
            "REFDATA_HOST_PATH": self._config.refdata_host_path,
            "HEARTBEAT_INTERVAL_MIN": self._heartbeat_interval_min,
        }
        if self._config.mount_prefix_override:
            env["MOUNT_PREFIX_OVERRIDE"] = self._config.mount_prefix_override
        if self._config.additional_path:
            env["ADDITIONAL_PATH"] = self._config.additional_path
        if self._s3config.get_insecure(self._config.use_S3_external_url):
            env["S3_INSECURE"] = "TRUE"
        if log_level := os.environ.get("CTS_LOG_LEVEL"):
            env["CTS_LOG_LEVEL"] = log_level
        environment = ""
        for key, val in env.items():
            environment += f"{key}={val} "

        return f'"{environment}"'

    def _get_sub(self, job: models.Job) -> tuple[htcondor2.Submit, list[dict[str, str]]]:
        # A lot of this is copied from
        # https://github.com/kbase/execution_engine2/blob/develop/lib/execution_engine2/utils/Condor.py
        mem = str(int(job.job_input.memory / (1024 * 1024)))  # Condor expects MiB
        logprefix = f"cts-{job.id}-$(container_number)-container"
        # Hold jobs running longer than job_timeout + job_update_timeout + fudge.
        # The executor times out at job_timeout; the extra time allows it to update state
        # and upload error logs before condor puts the job on hold.
        hold_sec = (
            _JOB_TIMEOUT_MIN
            + self._config.job_update_timeout_min
            + _JOB_TIMEOUT_FUDGE_FACTOR_MIN
        ) * 60
        subdict = {
            "shell": f"bash {self._exe_name}",
            # Has to exist locally and on the condor Schedd host
            # Which doesn't make any sense
            "initialdir": self._config.initial_dir,
            "transfer_input_files": f"{self._exe_url}, {self._code_archive_url}",
            "environment": self._get_environment(job),
            # Prefixing the log files with directories seems to make log creation unreliable
            # and / or fail depending on the condor version. Not sure why
            "output":  f"cts-{job.id}-$(container_number).out",
            "error": f"cts-{job.id}-$(container_number).err",
            "log": f"cts-{job.id}-$(container_number).log",
            "transfer_output_files": f"{logprefix}.out, {logprefix}.err",
            "Periodic_Hold": f"( RemoteWallClockTime > {hold_sec} )",
            "request_cpus": str(job.job_input.cpus),
            "request_memory": mem,
            # request_disk needed?
            "want_graceful_removal": True,  # send a sigterm to the job, allowing cleanup

            # Fair share stuff - this is way too hard to test. Eventually just check it
            # shows up in the job classad
            "Concurrency_Limits": job.user,
            "+AccountingGroup": f'"{job.user}"',
            
            # Make finding jobs with query / history easier
            f"+{_AD_JOB_ID}": f'"{job.id}"',  # must be quoted
            f"+{_AD_CONTAINER_NUMBER}": "$(container_number)",
        }
        if self._config.client_group:
            # HTCondor will && this with its own requirements
            # Regex so it can match workers with multiple client groups specified
            subdict["requirements"] =  f'regexp("{self._config.client_group}",CLIENTGROUP)'
        sub = htcondor2.Submit(subdict | STATIC_SUB)
        itemdata = [
            {"container_number": str(i)}
            for i in range(job.job_input.num_containers)
        ]
        return sub, itemdata

    async def run_job(self, job: models.Job) -> int:
        """
        Run a job on HTCondor.
        
        Returns the HTCondor cluster ID for the job.
        """
        sub, itemdata = self._get_sub(job)
        # Don't block the event loop
        # could probably make itemdata an generator, YAGNI
        jobres = await asyncio.to_thread(self._schedd.submit, sub, itemdata=iter(itemdata))
        return jobres.cluster()
    
    def _classad_to_dict(self, ca: ClassAd) -> dict[str, Any]:
        ret = {}
        for k in _RETURNED_JOB_ADS:
            v = ca.get(k)
            if v is not None:
                if k == _AD_MEM:
                    v = v.eval(scope=ca)
                if isinstance(v, ExprTree):
                    v = str(v)  # eval()ing he ExprTree isn't helpful, want the expression
                ret[k] = v
        return ret

    async def get_container_classad(self, cluster_id: int, container_number: int
    ) -> dict[str, Any]:
        """
        Get the HTCondor status for a specific container for a job, specified by the job's
        HTCondor ClusterID.
        The container_number in practice is the same as the HTCondor ProcId.
        A subset of the job ClassAd fields are returned.
        """
        _check_num(cluster_id, "cluster_id")
        _check_num(container_number, "container_number", minimum=0)
        constraint = (
            f"{_AD_CLUSTER_ID} == {cluster_id} && "
            + f"{_AD_CONTAINER_NUMBER} == {container_number}"
        )
        job_ads = await asyncio.to_thread(  # Don't block the event loop
            self._schedd.query,
            constraint=constraint,
            projection=_RETURNED_JOB_ADS,
        )
        if not job_ads:
            job_ads = await asyncio.to_thread(
                self._schedd.history,
                constraint=constraint,
                projection=_RETURNED_JOB_ADS,
            )
        if not job_ads:
            return {_AD_STATE: ProcState.MISSING}
        classad = self._classad_to_dict(job_ads[0])
        return {_AD_STATE: _status_to_proc_state(classad[_AD_JOB_STATUS])} | classad
        
    async def get_cluster_classads(self, cluster_id: int
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Get the htcondor status for a job. Returns all containers.
        A subset of the job ClassAd fields are returned.
        
        Returns a 2-tuple of running jobs and completed jobs.
        """
        _check_num(cluster_id, "cluster_id")
        running_job_ads, complete_job_ads = await self._fetch_cluster_ads(
            cluster_id, _RETURNED_JOB_ADS
        )
        if not running_job_ads and not complete_job_ads:
            raise ValueError(f"No records found for cluster ID {cluster_id}")
        id2ad = {ad[_AD_PROC_ID]: ad for ad in running_job_ads}
        for ad in complete_job_ads:
            # remove jobs that have transitioned to complete between the queries
            id2ad.pop(ad[_AD_PROC_ID], None)
        running = [self._classad_to_dict(ad) for ad in id2ad.values()]
        complete = [self._classad_to_dict(ad) for ad in complete_job_ads]
        return running, complete

    async def _fetch_cluster_ads(
        self, cluster_id: int, projection: list[str],
        proc_ids: Collection[int] | None = None,
    ) -> tuple[list, list]:
        constraint = f"{_AD_CLUSTER_ID} == {cluster_id}"
        if proc_ids is not None:
            proc_constraint = " || ".join(f"{_AD_PROC_ID} == {pid}" for pid in proc_ids)
            constraint = f"{constraint} && ({proc_constraint})"
        active_ads = await asyncio.to_thread(  # Don't block the event loop
            self._schedd.query,
            constraint=constraint,
            projection=projection,
        )
        complete_ads = await asyncio.to_thread(
            self._schedd.history,
            constraint=constraint,
            projection=projection,
        )
        return active_ads, complete_ads

    async def _fetch_proc_map(
        self,
        cluster_id: int,
        projection: list[str],
        expected_procs: int | Collection[int],
        transform: Callable[[Any], _V],
        missing: _V | None = None,
    ) -> dict[int, _V]:
        _check_num(cluster_id, "cluster_id")
        if expected_procs is None:
            raise ValueError("expected_procs is required")
        if isinstance(expected_procs, int):
            if expected_procs < 0:
                raise ValueError("expected_procs must be >= 0")
            filter_ids: Collection[int] | None = None
            expected_ids: Collection[int] = range(expected_procs)
        else:
            invalid = sorted(pid for pid in expected_procs if pid < 0)
            if invalid:
                raise ValueError(f"expected_procs contains proc IDs less than 0: {invalid}")
            filter_ids = expected_procs
            expected_ids = expected_procs
        if filter_ids is not None and not filter_ids:
            return {}
        active_ads, complete_ads = await self._fetch_cluster_ads(cluster_id, projection, filter_ids)
        result: dict[int, _V] = {}
        for ad in active_ads:
            result[ad[_AD_PROC_ID]] = transform(ad)
        for ad in complete_ads:
            # override active entry if a proc raced from query to history between calls
            result[ad[_AD_PROC_ID]] = transform(ad)
        if filter_ids is None:  # unrestricted query — check for unexpected proc IDs
            expected_set = set(expected_ids)
            unexpected = sorted(pid for pid in result if pid not in expected_set)
            if unexpected:
                raise ValueError(
                    f"HTCondor returned unexpected proc IDs {unexpected} "
                    f"for cluster {cluster_id}"
                )
        for proc_id in expected_ids:
            if proc_id not in result:
                result[proc_id] = missing
        return result

    async def get_cluster_proc_states(
        self,
        cluster_id: int,
        expected_procs: int | Collection[int],
    ) -> dict[int, ProcState]:
        """
        Get the state of each process in an HTC cluster using minimal data transfer.

        Returns a dict mapping proc ID (== container number) to ProcState, with a full
        entry for every expected proc. Procs absent from both the active queue and history
        are returned with ProcState.MISSING.

        cluster_id - the HTCondor cluster ID.
        expected_procs - the expected set of proc IDs. If an int n, the expected set is
            range(n) and the HTCondor query is unrestricted to proc IDs. If a collection,
            it is used directly as both the expected set and the query filter. An empty
            collection returns an empty dict without querying HTCondor.
        """
        return await self._fetch_proc_map(
            cluster_id, _STATUS_ONLY, expected_procs,
            lambda ad: _status_to_proc_state(ad[_AD_JOB_STATUS]),
            missing=ProcState.MISSING,
        )

    async def get_cluster_proc_details(
        self,
        cluster_id: int,
        expected_procs: int | Collection[int],
    ) -> dict[int, ProcDetails]:
        """
        Get the state and hold details of each process in an HTC cluster.

        Returns a dict mapping proc ID (== container number) to ProcDetails, with a full
        entry for every expected proc. Procs absent from both the active queue and history
        are returned with ProcDetails(state=ProcState.MISSING).

        cluster_id - the HTCondor cluster ID.
        expected_procs - the expected set of proc IDs. If an int n, the expected set is
            range(n) and the HTCondor query is unrestricted to proc IDs. If a collection,
            it is used directly as both the expected set and the query filter. An empty
            collection returns an empty dict without querying HTCondor.
        """
        return await self._fetch_proc_map(
            cluster_id, _STATUS_AND_HOLD, expected_procs,
            lambda ad: ProcDetails(
                state=_status_to_proc_state(ad[_AD_JOB_STATUS]),
                hold_reason=ad.get(_AD_HOLD_REASON),
                hold_reason_code=ad.get(_AD_HOLD_REASON_CODE),
            ),
            missing=ProcDetails(state=ProcState.MISSING),
        )

    async def release_job(self, cluster_id: int):
        """
        Release all held processes in an HTCondor job cluster.

        If no processes are held this is a noop.
        """
        _check_num(cluster_id, "cluster_id")
        await asyncio.to_thread(
            self._schedd.act,
            htcondor2.JobAction.Release,
            f"{_AD_CLUSTER_ID} == {cluster_id}",
        )

    async def cancel_job(self, cluster_id: int):
        """
        Cancel a job by its cluster ID.
        
        If the job is complete, held, or non-existant this is a noop.
        """
        _check_num(cluster_id, "cluster_id")
        await asyncio.to_thread(  # don't block the event loop
            self._schedd.act,
            htcondor2.JobAction.Remove,
            f"{_AD_CLUSTER_ID} == {cluster_id}"
        )
