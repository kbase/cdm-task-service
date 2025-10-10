"""
A client for submitting jobs to a remote HTCondor instance targeted for use by the CDM
Task Service.
"""

import asyncio
from classad2 import ClassAd, ExprTree
from dataclasses import dataclass
import htcondor2
from pathlib import Path
import posixpath
from typing import Any
from yarl import URL

from cdmtaskservice.arg_checkers import (
    not_falsy as _not_falsy,
    require_string as _require_string,
    check_num as _check_num,
)
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
    # For some reason setting transfer_output_files to the empty string isn't working.
    # We specify an empty file so that large job output doesn't get transferred.
    "transfer_output_files": "__DUMMY_OUTPUT__",
    "getenv": "False",
}


@dataclass(frozen=True)
class HTCondorWorkerPaths:
    """ Paths where external executors should look up secrets. """
    
    # TODO CODE check args aren't None / whitespace only. Not a lib class so YAGNI for now
    
    token_path: str
    """
    The path on the condor worker containing a KBase token for use when
    contacting the service.
    """

    s3_access_secret_path: str
    """
    The path on the condor worker containing the s3 access secret for the S3 instance.
    """


class CondorClient:
    """
    The condor client.
    """

    def __init__(
        self,
        schedd: htcondor2.Schedd,
        initial_dir: Path,
        service_root_url: str,
        executable_url: str,
        code_archive_url: str,
        paths: HTCondorWorkerPaths,
        client_group: str | None = None,
    ):
        """
        Create the client.
        
        schedd: An htcondor Schedd instance, configured to submit jobs to the cluster.
        initial_dir - where the job logs should be stored on the condor scheduler host.
            The path must exist there and locally.
        service_root_url - the URL of the service root, used by the remote job to get and update
            job state.
        executable_url - the url for the executable to run in the condor worker for each job.
            Must end in a file name and have no query or fragment.
        code_archive_url - the url for the *.tgz file code archive to transfer to the condor worker
            for each job. Must end in a file name and have no query or fragment.
        paths - the paths on the HTCondor workers should look for job secrets.
        client_group - the client group to submit jobs to, if any. This is a classad on
            a worker with the name CLIENTGROUP.
        """
        self._schedd = _not_falsy(schedd, "schedd")
        self._initial_dir = _not_falsy(initial_dir, "initial_dir")
        # Why this has to exist locally is beyond me
        self._initial_dir.mkdir(parents=True, exist_ok=True)
        self._service_root_url = _require_string(service_root_url, "service_root_url")
        self._exe_url = _require_string(executable_url, "executable_url")
        self._exe_name = self._get_name_from_url(self._exe_url)
        self._code_archive_url = _require_string(code_archive_url, "code_archive_url")
        self._code_archive_name = self._get_name_from_url(self._code_archive_url)
        self._paths = _not_falsy(paths, "paths")
        self._cligrp = client_group
        
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
            "SERVICE_ROOT_URL": self._service_root_url,
            "TOKEN_PATH": self._paths.token_path,
            "S3_SECRET_PATH": self._paths.s3_access_secret_path,
            # TODO CONDOR need minio url
        }
        environment = ""
        for key, val in env.items():
            environment += f"{key}={val} "

        return f'"{environment}"'

    def _get_sub(self, job: models.Job) -> tuple[htcondor2.Submit, list[dict[str, str]]]:
        # A lot of this is copied from
        # https://github.com/kbase/execution_engine2/blob/develop/lib/execution_engine2/utils/Condor.py
        mem = str(int(job.job_input.memory / (1024 * 1024)))  # Condor expects MiB
        subdict = {
            "shell": f"bash {self._exe_name}",
            # Has to exist locally and on the condor Schedd host
            # Which doesn't make any sense
            "initialdir": str(self._initial_dir),
            "transfer_input_files": f"{self._exe_url}, {self._code_archive_url}",
            "environment": self._get_environment(job),
            "output":  f"cts/{job.id}/cts-{job.id}-$(container_number).out",
            "error": f"cts/{job.id}/cts-{job.id}-$(container_number).err",
            # Prefixing the log file with directories seems to make log creation unreliable.
            # Not sure why
            "log": f"cts-{job.id}-$(container_number).log",
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
        if self._cligrp:
            # HTCondor will && this with its own requirements
            subdict["requirements"] =  f'(CLIENTGROUP == "{self._cligrp}")'
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
