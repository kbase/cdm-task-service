"""
A client for submitting jobs to a remote HTCondor instance targeted for use by the CDM
Task Service.
"""

import asyncio
import htcondor2
from pathlib import Path
import posixpath
from yarl import URL

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice import models


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
    "getenv": "False"
}


class CondorClient:
    """
    The condor client.
    """

    def __init__(
        self,
        schedd: htcondor2.Schedd,
        initial_dir: Path,
        executable_url: str,
        runner_archive_url: str,
        client_group: str | None = None
    ):
        """
        Create the client.
        
        schedd: An htcondor Schedd instance, configured to submit jobs to the cluster.
        initial_dir - where the job should start on the condor worker. The path must exist there.
        executable_url - the url for the executable to run in the condor worker for each job.
            Must end in a file name and have no query or fragment.
        runner_archive_url - the url for the *.tgz file archive to transfer to the condor worker
            for each job. Must end in a file name and have no query or fragment.
        client_group - the client group to submit jobs to, if any. This is a classad on
            a worker with the name CLIENTGROUP.
        """
        self._schedd = _not_falsy(schedd, "schedd")
        self._initial_dir = _not_falsy(initial_dir, "initial_dir")
        # Why this has to exist locally is beyond me
        self._initial_dir.mkdir(parents=True, exist_ok=True)
        self._exe_url = _require_string(executable_url, "executable_url")
        self._exe_name = self._get_name_from_url(self._exe_url)
        self._runner_archive_url = _require_string(runner_archive_url, "runner_archive_url")
        self._runner_archive_name = self._get_name_from_url(self._runner_archive_url)
        self._cligrp = client_group
        
    def _get_name_from_url(self, url: str) -> str:
        parsed = URL(url)
        if parsed.query_string or parsed.fragment:
            raise ValueError(
                f"Condor url {url} cannot contain query or fragment sections")

        # posizpath ensures use of "/"
        filename = posixpath.basename(parsed.path)
        if not filename:
            raise ValueError(f"Condor url {url} does not end in a file name")
        return filename
    
    def _get_environment(self, job: models.Job) -> str:
        env = {
            "JOB_ID": job.id,
            "CONTAINER_NUMBER": "$(container_number)",
            "JOBRUNNER_ARCHIVE": self._runner_archive_name
            # TODO CONDOR need CTS and minio urls
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
            "transfer_input_files": f"{self._exe_url}, {self._runner_archive_url}",
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
            "+CTS_Job_ID": job.id,
            "+container_number": "$(container_number)",
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
