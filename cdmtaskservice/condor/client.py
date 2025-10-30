"""
A client for submitting jobs to a remote HTCondor instance targeted for use by the CDM
Task Service.
"""

import asyncio
from classad2 import ClassAd, ExprTree
import htcondor2
from pathlib import Path
import posixpath
from typing import Any
from yarl import URL

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, check_num as _check_num
from cdmtaskservice.condor.config import CondorClientConfig
from cdmtaskservice.config_s3 import S3Config
from cdmtaskservice import models


# Some of this stuff is pretty specific to KBase but the likelihood we'll ever be submitting
# to some other condor instance is pretty low, so don't worry about it 


_AD_JOB_ID = "CTSJobID"
_AD_CONTAINER_NUMBER = "CTSContainerNumber"

_LOCATIONS = ["Iwd", "Err", "Out", "UserLog"]
_IDS = ["ClusterId", "ProcId", _AD_JOB_ID, _AD_CONTAINER_NUMBER]
_RESOURCES = [
    "RequestCpus",
    "RequestDisk",
    "RequestMemory",
    "CpusProvisioned",
    "DiskProvisioned",
    "GPUsProvisioned",
    "CpusUsage",
    "RemoteUserCpu",
    "CumulativeRemoteSysCpu",
    "CumulativeRemoteUserCpu",
    "DiskUsage",
    "DiskUsage_RAW",
    "ResidentSetSize",
    "ResidentSetSize_RAW",
    "ImageSize",
    "ImageSize_RAW",
]
_CONDOR_DEETS = ["Owner", "User", "CondorVersion", "CondorPlatform"]
_INPUTS = ["Environment", "Requirements", "TransferInput", "LeaveJobInQueue"]
_JOB_STATE = [
    "Rank",
    "JobPrio",
    "JobStatus",
    "ExitCode",
    "ExitStatus",
    "JobRunCount",
    "NumRestarts",
    "NumJobStarts",
    "HoldReason",
    "HoldReasonCode",
    "VacateReason",
    "VacateReasonCode",
]
_FAIR_SHARE = ["ConcurrencyLimits", "AccountingGroup"]
_TIME = [
    "JobStartDate",
    "CommittedTime",
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
    # Hold jobs running longer than 7 days
    "Periodic_Hold": "( RemoteWallClockTime > 604800 )",
    # If a job exits incorrectly put it on hold
    "on_exit_hold": "ExitCode =!= 0",
    "should_transfer_files": "yes",
    "when_to_transfer_output": "on_exit_or_evict",
    "getenv": "False",
}


class CondorClient:
    """
    The condor client.
    """

    def __init__(self, schedd: htcondor2.Schedd, config: CondorClientConfig, s3config: S3Config):
        """
        Create the client.
        
        schedd: An htcondor Schedd instance, configured to submit jobs to the cluster.
        config - the configuration for the client.
        s3Config - the configuration for the S3 instance where files are stored.
        """
        self._schedd = _not_falsy(schedd, "schedd")
        self._config = _not_falsy(config, "config")
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
            "CONTAINER_NUMBER": "$(container_number)",
            "CODE_ARCHIVE": self._code_archive_name,
            "SERVICE_ROOT_URL": self._config.service_root_url,
            "TOKEN_PATH": self._config.token_path,
            "S3_URL": self._s3config.internal_url,  # could add a toggle to use external if needed
            "S3_ACCESS_KEY": self._s3config.access_key,
            "S3_SECRET_PATH": self._config.s3_access_secret_path,
            "S3_ERROR_LOG_PATH": self._s3config.error_log_path,
            "JOB_UPDATE_TIMEOUT_MIN": self._config.job_update_timeout_min,
        }
        if self._config.mount_prefix_override:
            env["MOUNT_PREFIX_OVERRIDE"] = self._config.mount_prefix_override
        if self._s3config.insecure:
            env["S3_INSECURE"] = "TRUE"
        environment = ""
        for key, val in env.items():
            environment += f"{key}={val} "

        return f'"{environment}"'

    def _get_sub(self, job: models.Job) -> tuple[htcondor2.Submit, list[dict[str, str]]]:
        # A lot of this is copied from
        # https://github.com/kbase/execution_engine2/blob/develop/lib/execution_engine2/utils/Condor.py
        mem = str(int(job.job_input.memory / (1024 * 1024)))  # Condor expects MiB
        logprefix = f"container_log_{job.id}-$(container_number)"
        subdict = {
            "shell": f"bash {self._exe_name}",
            # Has to exist locally and on the condor Schedd host
            # Which doesn't make any sense
            "initialdir": self._config.initial_dir,
            "transfer_input_files": f"{self._exe_url}, {self._code_archive_url}",
            "environment": self._get_environment(job),
            "output":  f"cts/{job.id}/cts-{job.id}-$(container_number).out",
            "error": f"cts/{job.id}/cts-{job.id}-$(container_number).err",
            # Prefixing the log file with directories seems to make log creation unreliable.
            # Not sure why
            "log": f"cts-{job.id}-$(container_number).log",
            "transfer_output_files": f"{logprefix}.out, {logprefix}.err",
            "transfer_output_remaps": f"{logprefix}.out = cts/{job.id}/{logprefix}.out; "
                + f"{logprefix}.err = cts/{job.id}/{logprefix}.err",
            "request_cpus": str(job.job_input.cpus),
            "request_memory": mem,
            # request_disk needed?

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
            subdict["requirements"] =  f'(CLIENTGROUP == "{self._config.client_group}")'
        sub = htcondor2.Submit(subdict | STATIC_SUB)
        itemdata = [
            {"container_number": str(i)}
            for i in range(1, job.job_input.num_containers + 1)
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
    
    # TODO CANCEL_JOB for condor

    def _classad_to_dict(self, ca: ClassAd) -> dict[str, Any]:
        ret = {}
        for k in _RETURNED_JOB_ADS:
            v = ca.get(k)
            if v is not None:
                if isinstance(v, ExprTree):
                    v = str(v)  # eval()ing he ExprTree isn't helpful, want the expression
                ret[k] = v
        return ret

    async def get_container_status(self, cluster_id: int, container_number: int) -> dict[str, Any]:
        """
        Get the HTCondor status for a specific container for a job, specified by the job's
        HTCondor ClusterID.
        The container_number in practice is the HTCondor ProcId + 1.
        A subset of the job ClassAd fields are returned.
        """
        _check_num(cluster_id, "cluster_id")
        _check_num(container_number, "container_number")
        constraint = f"ClusterId == {cluster_id} && {_AD_CONTAINER_NUMBER} == {container_number}"
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
            raise ValueError(
                f"No record found for cluster ID {cluster_id} and container number "
                + f"{container_number}"
            )
        return self._classad_to_dict(job_ads[0])
        
    async def get_job_status(self, cluster_id: int
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """
        Get the htcondor status for a job. Returns all containers.
        A subset of the job ClassAd fields are returned.
        
        Returns a 2-tuple of running jobs and completed jobs.
        """
        _check_num(cluster_id, "cluster_id")
        constraint = f"ClusterId == {cluster_id}"
        running_job_ads = await asyncio.to_thread(  # Don't block the event loop
            self._schedd.query,
            constraint=constraint,
            projection=_RETURNED_JOB_ADS,
        )
        complete_job_ads = await asyncio.to_thread(
            self._schedd.history,
            constraint=constraint,
            projection=_RETURNED_JOB_ADS,
        )
        if not running_job_ads and not complete_job_ads:
            raise ValueError(f"No records found for cluster ID {cluster_id}")
        id2ad = {(ad["ClusterId"], ad["ProcId"]): ad for ad in running_job_ads}
        for ad in complete_job_ads:
            job_key = (ad["ClusterId"], ad["ProcId"])
            # remove jobs that have transitioned to complete between the queries
            id2ad.pop(job_key, None)
        running = [self._classad_to_dict(ad) for ad in id2ad.values()]
        complete = [self._classad_to_dict(ad) for ad in complete_job_ads]
        return running, complete
