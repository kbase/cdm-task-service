'''
Handler for data transfer between CDM sources and NERSC.
'''

import asyncio
from collections.abc import Callable
from enum import Enum
import io
import inspect
import json
import logging
import os
from pathlib import Path
from sfapi_client import AsyncClient
from sfapi_client.exceptions import SfApiError
from sfapi_client.paths import AsyncRemotePath
from sfapi_client.compute import Machine, AsyncCompute
import sys
from types import ModuleType
from typing import Self, Awaitable, Any, NamedTuple

from cdmtaskservice import logfields
from cdmtaskservice import models
from cdmtaskservice import sites
from cdmtaskservice.arg_checkers import (
    not_falsy as _not_falsy,
    require_string as _require_string,
    check_num as _check_num,
)
from cdmtaskservice.jaws import wdl
from cdmtaskservice.jaws.config import JAWSConfig
from cdmtaskservice.jaws.remote import ERRORS_JSON_FILE
from cdmtaskservice.jaws.sitemapper import get_jaws_site
from cdmtaskservice.jobflows.container_filenames import get_filenames_for_container
from cdmtaskservice.manifest_files import generate_manifest_files
from cdmtaskservice.nersc import remote
from cdmtaskservice.nersc.paths import NERSCPaths
from cdmtaskservice.s3.client import S3ObjectMeta, PresignedPost
from cdmtaskservice.s3.remote import get_cache_path

# This is mostly tested manually to avoid modifying files at NERSC.

# TODO TEST add tests in test_manual
# TODO TEST add automated tests for stuff that doesn't contact nersc (arg checks etc.)
# TODO ERRORHANDLING wrap sfapi errors in server specific errors

# TODO CLEANUP clean up old code versions @NERSC somehow. Not particularly important
#              actually this is impossible to do safely - could be a different cluster of
#              servers on a different version. Note in admin docs that old unused installs
#              can be deleted. Server code is tiny anyway

_MIN_TIMEOUT_SEC = 300
_SEC_PER_GB = 2 * 60  # may want to make this configurable
# Large file transfers are submitted as Slurm jobs on the `xfer` QOS (NERSC's data transfer
# queue) rather than SFAPI async tasks, since tasks are capped at 10 minutes of execution time.
# The `xfer` QOS runs on Perlmutter login nodes, requires `--licenses=SCRATCH`, forbids `-N`/
# `--nodes`, and has a 48 hour wall time ceiling. See https://docs.nersc.gov/jobs/policy/
_XFER_QOS = "xfer"
_MAX_SBATCH_TIME_SEC = 48 * 60 * 60
# Buffer applied to the size-based transfer time estimate before submitting the Slurm job, so
# a hung transfer still hits Slurm's own wall time limit rather than running indefinitely and
# tying up one of the `xfer` QOS' limited (15) concurrent job slots per user.
#
# Kept small (1.5x) rather than large because the estimate it's applied to is already very
# conservative on its own:
#   * _SEC_PER_GB (2 min/GB, ~67 Mbps) is a slow rate for a transfer expected to run over ESnet
#     between two DOE-facility endpoints.
#   * The estimate sums bytes across every file in the batch with no credit for concurrency, i.e.
#     it assumes the whole batch transfers serially even though multiple files transfer at once.
#   * Actual hangs on individual files/connections are caught independently and much sooner by
#     the per-file timeout in s3/remote.py's _timeout(), so this buffer isn't the primary hang
#     detector - it mainly needs to absorb variance in the rate estimate, not find hangs itself.
_TIME_BUFFER_MULTIPLIER = 1.5
_STAGING_DIR_NAME = "staged"
# `ssh dtn` resolves to a NERSC Data Transfer Node from a Perlmutter login node without further
# configuration, so no configurable hostname is needed here.
_DTN_HOST = "dtn"

_JOB_MANIFESTS = Path("manifests")
_MANIFEST_FILE_PREFIX = "manifest-"
_CRC64NVME_CHECKSUMS_JSON_FILE_NAME = "upload_checksums.json"
_JOB_LOGS = "logs"
_JOBS_DIR = "jobs"
_REFDATA_DIR = "refdata"


_JAWS_CONF_TEMPLATE = """
[USER]
token = {token}
default_team = {group}
"""
_JAWS_COMMAND_TEMPLATE = f"""
module use /global/cfs/projectdirs/kbase/jaws/modulefiles
module load jaws
export JAWS_USER_CONFIG=~/{{conf_file}}
jaws submit --tag {{job_id}} {{wdlpath}} {{inputjsonpath}} {{site}}
"""
_JAWS_INPUT_WDL = "input.wdl"
_JAWS_INPUT_JSON = "input.json"


# TODO PERF add start and end time to task output and log / record in db / put in result file)
# Pinned to a specific NERSC `python` module rather than a bare `module load python` (which
# floats to whatever NERSC currently defaults to): pip dependencies (see
# _install_pip_dependencies) are installed once, at server startup, into the site-packages of
# whatever Python version is resolved at that moment. Every later job run resolves the same
# module load again from scratch. If the default moved to a different Python version in
# between, the two resolutions disagree and previously installed packages become invisible to
# the newer interpreter, e.g. NoModuleFoundError: awscrt at job runtime despite a successful
# install at startup.
# To check for newer versions to pin to, run `module avail python` on a NERSC Perlmutter login
# node. After updating this constant, restart the CTS server so _install_pip_dependencies
# reinstalls under the newly pinned version.
_PYTHON_MODULE = "python/3.13-26.8.0"
_RUN_CTS_REMOTE_CODE_FILENAME = "run_cts_remote_code.sh"
# Might want to make a shared constants module for all these env var names and update this
# file and remote.py
_RUN_CTS_REMOTE_CODE = f"""
#!/usr/bin/env bash

module load {_PYTHON_MODULE}

export PYTHONPATH=$CTS_CODE_LOCATION
export CTS_MODE=$CTS_MODE
export CTS_MANIFEST_LOCATION=$CTS_MANIFEST_LOCATION
export CTS_ERRORS_JSON_LOCATION=$CTS_ERRORS_JSON_LOCATION
export CTS_CONTAINER_LOGS_LOCATION=$CTS_CONTAINER_LOGS_LOCATION
export CTS_JAWS_OUTPUT_DIR=$CTS_JAWS_OUTPUT_DIR
export CTS_CHECKSUM_FILE_LOCATION=$CTS_CHECKSUM_FILE_LOCATION
export CTS_STAGING_DIR=$CTS_STAGING_DIR
export CTS_DTN_HOST=$CTS_DTN_HOST
export CTS_REFDATA_DEST_DIR=$CTS_REFDATA_DEST_DIR
export CTS_COMPLETION_FILE_LOCATION=$CTS_COMPLETION_FILE_LOCATION
export CTS_COMPLETION_FILE_CONTENTS=$CTS_COMPLETION_FILE_CONTENTS
export CTS_RESULT_FILE_LOCATION=$CTS_RESULT_FILE_LOCATION
export CTS_LOG_FILE_LOCATION=$CTS_LOG_FILE_LOCATION
export CTS_CALLBACK_URL=$CTS_CALLBACK_URL
export SCRATCH=$SCRATCH

# Redirect all further output to a durable log file so a killed (e.g. OOM) process still
# leaves a trace, since the SFAPI task's captured stdout is only available for a short time
# after the task ends (successfully or not).
exec >> "$CTS_LOG_FILE_LOCATION" 2>&1

echo "PYTHONPATH=[$PYTHONPATH]"
echo "CTS_MODE=[$CTS_MODE]"
echo "CTS_MANIFEST_LOCATION=[$CTS_MANIFEST_LOCATION]"
echo "CTS_ERRORS_JSON_LOCATION=[$CTS_ERRORS_JSON_LOCATION]"
echo "CTS_CONTAINER_LOGS_LOCATION=[$CTS_CONTAINER_LOGS_LOCATION]"
echo "CTS_JAWS_OUTPUT_DIR=[$CTS_JAWS_OUTPUT_DIR]"
echo "CTS_CHECKSUM_FILE_LOCATION=[$CTS_CHECKSUM_FILE_LOCATION]"
echo "CTS_STAGING_DIR=[$CTS_STAGING_DIR]"
echo "CTS_DTN_HOST=[$CTS_DTN_HOST]"
echo "CTS_REFDATA_DEST_DIR=[$CTS_REFDATA_DEST_DIR]"
echo "CTS_COMPLETION_FILE_LOCATION=[$CTS_COMPLETION_FILE_LOCATION]"
echo "CTS_COMPLETION_FILE_CONTENTS=[$CTS_COMPLETION_FILE_CONTENTS]"
echo "CTS_RESULT_FILE_LOCATION=[$CTS_RESULT_FILE_LOCATION]"
echo "CTS_LOG_FILE_LOCATION=[$CTS_LOG_FILE_LOCATION]"
echo "CTS_CALLBACK_URL=[$CTS_CALLBACK_URL]"
echo "SCRATCH=[$SCRATCH]"

# -u disables python stdout/stderr buffering so output isn't lost if the process is killed
python -u $CTS_CODE_LOCATION/{"/".join(remote.__name__.split("."))}.py
echo "python exited with code $?"
"""

# Submitted to the `xfer` QOS on Perlmutter. `-N`/`--nodes` is forbidden on this QOS since it
# runs on shared login nodes rather than dedicated compute nodes.
_SBATCH_SCRIPT_TEMPLATE = f"""#!/usr/bin/env bash
#SBATCH --qos={_XFER_QOS}
#SBATCH --licenses=SCRATCH
#SBATCH --time={{time}}

{{body}}
"""


# Note there's a race condition that theoretically could happen here if the path is removed
# after the existence check but before the rm. Since this is just for clean up not an issue. 
# Also note I wasted way too much time trying to make this fail cleanly if there was a write
# protected file in the path tree, which should never happen.
# It'll still fail if it really can't delete the path.
_REMOVE_PATH_TEMPLATE = """
if [ ! -e "{{path}}" ]; then
    exit 0
fi
rm -rf -- "{{path}}"
"""


_CTS_ROOT = __name__.split(".")[0]
_CTS_DEPENDENCIES = {remote}
_PIP_DEPENDENCIES = set()


def _get_dependencies(mod: ModuleType, cts_dep: set[ModuleType], pip_dep: set[ModuleType]):
    for md in inspect.getmembers(mod):
        m = md[1]
        if not inspect.ismodule(m):
            if hasattr(m, "__module__"):
                m = sys.modules[m.__module__]
            else:
                continue
        if m in cts_dep:
            continue
        rootname = m.__name__.split(".")[0]
        if rootname == _CTS_ROOT:
            cts_dep.add(m)
            _get_dependencies(m, cts_dep, pip_dep)
        elif rootname not in sys.stdlib_module_names:
            pip_dep.add(sys.modules[rootname])
_get_dependencies(remote, _CTS_DEPENDENCIES, _PIP_DEPENDENCIES)


def _compute_sbatch_time_sec(total_bytes: int) -> int:
    """
    Compute the wall time, in seconds, to request for a `xfer` QOS Slurm job transferring
    total_bytes of data, capped at the QOS' 48 hour maximum.
    """
    estimate = max(_MIN_TIMEOUT_SEC, _SEC_PER_GB * total_bytes / 1_000_000_000)
    return int(min(_MAX_SBATCH_TIME_SEC, estimate * _TIME_BUFFER_MULTIPLIER))


def _seconds_to_slurm_time(seconds: int) -> str:
    """ Convert a number of seconds to a Slurm `--time` argument in HH:MM:SS format. """
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class TransferState(Enum):
    """ The state of the transfer. """
    
    SUCCESS = 1
    """ The transfer was successful. """

    FAIL = 2
    """ The transfer failed. """
    
    INCOMPLETE = 3
    """ The transfer is ongoing. """


class TransferResult(NamedTuple):
    """ Results of a file transfer between NERSC and another location. """
    
    state: TransferState = TransferState.SUCCESS
    """ The state of the transfer. """
    
    message: str | None = None
    """ If the transfer failed, contains an error message about the transfer. """
    
    traceback: str | None = None
    """ If the transfer failed, may contain a stack trace if available. """


class NERSCManager:
    """
    Manages interactions with the NERSC remote compute site.
    """
    
    @classmethod
    async def create(
        cls,
        client_provider: Callable[[], AsyncClient],
        nersc_paths: NERSCPaths,
        jaws_config: JAWSConfig,
        service_group: str = "dev",
    ) -> Self:
        """
        Create the NERSC manager.

        client_provider - a function that provides a valid SFAPI client. It is assumed that
            the user associated with the client does not change.
        nersc_paths - the set of paths for NERSC manager use.
        jaws_config - configuration for communicating with the JAWS job runner at NERSC. The JAWS
            username is expected to be a NERSC user and the same user as for the client provider.
            It is typically a collaboration account.
        service_group - The service group to which this instance of the manager belongs.
            This is used to separate files at NERSC so files from different S3 instances
            (say production and development) don't collide.
        """
        _not_falsy(jaws_config, "jaws_config")
        nm = NERSCManager(client_provider, nersc_paths, jaws_config.user, service_group)
        await nm._setup_remote_code(nersc_paths, jaws_config.token, jaws_config.group)
        return nm

    def __init__(
            self,
            client_provider: Callable[[], str],
            nersc_paths: NERSCPaths,
            nesrc_jaws_user: str,
            service_group: str,
        ):
        self._client_provider = _not_falsy(client_provider, "client_provider")
        self._nersc_code_path = _not_falsy(nersc_paths, "nersc_paths").code_path / service_group
        self._nersc_jaws_user = _require_string(nesrc_jaws_user, "nesrc_jaws_user")
        self._service_group = _require_string(service_group, "service_group")
        self._work_loc = Path("cdm_task_service") / service_group
        self._jawscfg = f"jaws_cts_{service_group}.conf"
        self._refdata_root = nersc_paths.jaws_refdata_root_dir

    async def _setup_remote_code(self, nersc_paths: NERSCPaths, jaws_token: str, jaws_group: str):
        # TODO RELIABILITY atomically write files. For these small ones probably doesn't matter?
        logr = logging.getLogger(__name__)
        cli = self._client_provider()
        perlmutter = await cli.compute(Machine.perlmutter)
        async with asyncio.TaskGroup() as tg:
            for mod in _CTS_DEPENDENCIES:
                target = self._nersc_code_path
                for module in mod.__name__.split("."):
                    target = target / module
                tg.create_task(self._upload_file_to_nersc(
                    perlmutter,
                    target.with_suffix(".py"),
                    file=mod.__file__)
                )
            tg.create_task(self._upload_file_to_nersc(
                perlmutter,
                self._nersc_code_path / _RUN_CTS_REMOTE_CODE_FILENAME,
                bio=io.BytesIO(_RUN_CTS_REMOTE_CODE.encode()),
                chmod="u+x",
            ))
            tg.create_task(self._upload_file_to_nersc(
                perlmutter,
                Path(self._jawscfg),  # No path puts it in the home dir
                bio=io.BytesIO(
                    _JAWS_CONF_TEMPLATE.format(token=jaws_token, group=jaws_group).encode()
                ),
                chmod = "600"
            ))
            pm_scratch = tg.create_task(self._set_up_perlmutter_scratch())
            if _PIP_DEPENDENCIES:
                tg.create_task(self._install_pip_dependencies(perlmutter))
        self._perlmutter_scratch = pm_scratch.result()
        self._nersc_perlmutter_file_cache_path = self._make_cache_path(
            nersc_paths.jaws_staging_dir_perlmutter)
        logr.info(
            "NERSC perlmutter JAWS staging cache path",
            extra={logfields.FILE: self._nersc_perlmutter_file_cache_path}
        )
    
    def _make_cache_path(self, jaws_staging_dir: Path):
        return (
            jaws_staging_dir
            / "inputs"
            # hard code site for now, it's the only one we can use
            / "kbase"
            # de-absolutize the scratch path
            / self._perlmutter_scratch.relative_to(self._perlmutter_scratch.anchor)
            / self._work_loc
            / "cache"
        )
    
    async def _install_pip_dependencies(self, compute: AsyncCompute):
        logr = logging.getLogger(__name__)
        deps = " ".join(
            # may need to do something else if module doesn't have __version__
            [f"{mod.__name__}=={mod.__version__}" for mod in _PIP_DEPENDENCIES])
        logr.info(f"Installing pip modules at NERSC: {deps}")
        command = (
            f"module load {_PYTHON_MODULE}; "
            # Unlikely, but this could cause problems if multiple versions
            # of the server are running at once. Don't worry about it for now
            + f"pip install {deps}"  # adding notapackage causes a failure
        )
        await compute.run(command)
        logr.info(f"Installed pip modules at NERSC")


    async def _set_up_perlmutter_scratch(self) -> Path:
        logr = logging.getLogger(__name__)
        logr.info("Getting Perlmutter scratch path from NERSC")
        cli = self._client_provider()
        compute = await cli.compute(Machine.perlmutter)
        scratch = (await compute.run("echo $SCRATCH")).strip()
        if not scratch:  # have had issues here previously
            raise ValueError("Unable to determine $SCRATCH variable for NERSC perlmutter")
        logr.info("NERSC perlmutter scratch path", extra={logfields.FILE: scratch})
        return Path(scratch)

    def _get_job_scratch(self, job_id) -> Path:
        return self._perlmutter_scratch / self._work_loc / _JOBS_DIR / job_id

    def _get_refdata_scratch(self, refdata_id) -> Path:
        return self._perlmutter_scratch / self._work_loc / _REFDATA_DIR / refdata_id

    def _get_refdata_staging_loc(self, refdata_id) -> Path:
        # Perlmutter login nodes (where `xfer` QOS jobs run) cannot write to the DTN-only
        # refdata root, so refdata is staged here first and copied to its final location by a
        # DTN-side step at the end of the job.
        return self._get_refdata_scratch(refdata_id) / _STAGING_DIR_NAME

    def _get_refdata_loc(self, refdata_id) -> Path:
        return self._refdata_root / self._get_relative_refdata_loc(refdata_id)

    def _get_relative_refdata_loc(self, refdata_id) -> Path:
        return self._work_loc / refdata_id
    
    def _get_refdata_file_change_path(self, refdata_id) -> Path:
        # the file path that JAWS looks for to trigger refdata transfers
        return (
            self._refdata_root /
            f"{self._nersc_jaws_user}_{self._service_group}_{refdata_id}_changes.txt"
        )
    
    def _get_refdata_file_complete_path(self, refdata_id, site: sites.Cluster) -> Path:
        # the file path that JAWS writes when refdata transfer to a site is complete
        site = get_jaws_site(site)
        return (
            self._refdata_root / "log" /
            (
                f"{self._nersc_jaws_user}_{self._service_group}_{refdata_id}_changes.txt"
                + f"_{site}.complete"
            )
        )
    
    async def _upload_file_to_nersc(
        self,
        compute: AsyncCompute,
        target: Path,
        file: Path = None,
        bio: io.BytesIO = None,
        chmod: str = None,
    ):  
        logr = logging.getLogger(__name__)
        logr.info("Uploading file to NERSC.", extra={logfields.FILE: target})
        if target.parent != Path("."):
            cmd = f"mkdir -p {target.parent}"
            await compute.run(cmd)
        asrp = self._get_async_path(compute, target)
        # TODO ERRORHANDLING throw custom errors
        if file:
            with open(file, "rb") as f:
                await asrp.upload(f)
        else:
            await asrp.upload(bio)
        logr.info("Upload of file to NERSC complete.", extra={logfields.FILE: target})
        if chmod:
            cmd = f"chmod {chmod} {target}"
            await compute.run(cmd)
            logr.info("chmod of uploaded file complete.", extra={logfields.FILE: target})

    async def _delete_paths(self, paths: list[Path]):
        logr = logging.getLogger(__name__)
        cli = self._client_provider()
        perlmutter = await cli.compute(Machine.perlmutter)
        async with asyncio.TaskGroup() as tg:
            for p in paths:
                logr.info("Deleting path at NERSC", extra={logfields.FILE: p})
                # May need to catch SFAPI client errors and wrap. YAGNI for now
                tg.create_task(perlmutter.run(_REMOVE_PATH_TEMPLATE.format(path=p)))

    def _get_async_path(self, compute: AsyncCompute, target: Path) -> AsyncRemotePath:
        # skip some API calls vs. the upload example in the NERSC docs
        # don't use a directory as the target or it makes an API call
        asrp = AsyncRemotePath(path=target, compute=compute)
        asrp.perms = "-"  # hack to prevent an unnecessary network call
        return asrp

    async def download_s3_files(
        self,
        download_id: str,
        objects: list[S3ObjectMeta],
        presigned_urls: list[str],
        callback_url: str,
        concurrency: int = 10,
        insecure_ssl: bool = False,
        refdata: bool = False,
        unpack: bool = False,
    ) -> str:
        """
        Download a set of files to NERSC from an S3 instance.
        
        download_id - the ID of the job or reference data for which the files are being
            transferred. This must be a unique ID, and no other transfers should be occurring
            for the id.
        objects - the S3 files to download.
        presigned_urls - the presigned download URLs for each object, in the same order as
            the objects.
        callback_url - the URL to GET as a callback for when the download is complete.
        concurrency - the number of simultaneous downloads to process.
        insecure_ssl - whether to skip the cert check for the S3 URL.
        refdata - whether this is a refdata download and files should be stored in the NERSC
            refdata location.
        unpack - whether to unpack *.gz, *.tar.gz, or *.tgz files.

        Returns the NERSC Slurm job ID for the download.
        """
        maniio = self._create_download_manifest(
            download_id, objects, presigned_urls, concurrency, insecure_ssl, refdata, unpack)
        total_bytes = sum(o.size for o in objects)
        return await self._process_manifest(
            maniio,
            download_id,
            callback_url,
            "download_manifest.json",
            "download",
            total_bytes,
            mode="refdata_manifest" if refdata else "manifest",
        )

    async def _upload_presigned_files(
        self,
        job_id: str,
        remote_files: list[Path],
        presigned_urls: list[PresignedPost],
        callback_url: str,
        total_bytes: int,
        concurrency: int = 10,
        insecure_ssl: bool = False,
    ) -> str:
        """
        Upload a set of files to presigned URLs from NERSC.

        job_id - the ID of the job for which the files are being transferred.
            This must be a unique ID, and no other transfers should be occurring for the job.
        remote_files - the files to upload.
        presigned_urls - the presigned upload URLs for each file, in the same order as
            the file.
        callback_url - the URL to GET as a callback for when the upload is complete.
        total_bytes - the total size of the files to upload, used to compute the Slurm job's
            wall time limit.
        concurrency - the number of simultaneous uploads to process.
        insecure_ssl - whether to skip the cert check for the S3 URL.

        Returns the NERSC Slurm job ID for the upload.
        """
        maniio = self._create_upload_manifest(
            remote_files, presigned_urls, concurrency, insecure_ssl)
        return await self._process_manifest(
            maniio,
            job_id,
            callback_url,
            "upload_manifest.json",
            "upload",
            total_bytes,
        )

    def _build_process_manifest_command(
        self,
        entity_id: str,
        callback_url: str,
        mode: str,
        manifestpath: Path,
        task_base_path: Path,
        error_json_file_location: str = None,
        container_logs_location: str = None,
    ) -> list[str]:
        refdata = mode == "refdata_manifest"
        command = [
            f"export CTS_MODE={mode}",
            f"export CTS_CODE_LOCATION={self._nersc_code_path}",
            f"export CTS_MANIFEST_LOCATION={manifestpath}",
            f"export CTS_RESULT_FILE_LOCATION={task_base_path}_result.json",
            f"export CTS_LOG_FILE_LOCATION={task_base_path}_log.txt",
            f"export CTS_CALLBACK_URL={callback_url}",
            f"export SCRATCH=$SCRATCH",
        ]
        if error_json_file_location:
            command.append(f"export CTS_ERRORS_JSON_LOCATION={error_json_file_location}")
            command.append(f"export CTS_CONTAINER_LOGS_LOCATION={container_logs_location}")
        if refdata:
            command.append(f"export CTS_STAGING_DIR={self._get_refdata_staging_loc(entity_id)}")
            command.append(f"export CTS_DTN_HOST={_DTN_HOST}")
            command.append(f"export CTS_REFDATA_DEST_DIR={self._get_refdata_loc(entity_id)}")
            # TODO CLEANUP need to delete this and the JAWS written completion file
            # see https://jaws-docs.jgi.doe.gov/en/latest/jaws/jaws_refdata.html#adding-data-to-refdata-directory
            command.append(
                "export CTS_COMPLETION_FILE_LOCATION="
                f"{self._get_refdata_file_change_path(entity_id)}"
            )
            command.append(
                f"export CTS_COMPLETION_FILE_CONTENTS={self._get_refdata_loc(entity_id)}"
            )
        command.append(f'"$CTS_CODE_LOCATION"/{_RUN_CTS_REMOTE_CODE_FILENAME}')
        return command

    async def _process_manifest(
        self,
        manifest: io.BytesIO,
        entity_id: str,
        callback_url: str,
        filename: str,
        task_type: str,
        total_bytes: int,
        mode: str = "manifest",
        error_json_file_location: str = None,
        container_logs_location: str = None,  # this is expected to be present if the above is
    ):
        refdata = mode == "refdata_manifest"
        if refdata:
            rootpath = self._get_refdata_scratch(entity_id)
        else:
            rootpath = self._get_job_scratch(entity_id)
        manifestpath = rootpath / filename
        cli = self._client_provider()
        perl = await cli.compute(Machine.perlmutter)
        # TODO CLEANUP manifests after some period of time
        await self._upload_file_to_nersc(perl, manifestpath, bio=manifest)
        command = self._build_process_manifest_command(
            entity_id,
            callback_url,
            mode,
            manifestpath,
            rootpath / task_type,
            error_json_file_location=error_json_file_location,
            container_logs_location=container_logs_location,
        )
        script = _SBATCH_SCRIPT_TEMPLATE.format(
            time=_seconds_to_slurm_time(_compute_sbatch_time_sec(total_bytes)),
            body="\n".join(command),
        )
        # upload script to make debugging easier
        scriptpath = rootpath / f"{task_type}_submit.sh"
        await self._upload_file_to_nersc(perl, scriptpath, bio=io.BytesIO(script.encode()))
        job = await perl.submit_job(str(scriptpath))
        job_id = str(job.jobid)
        logging.getLogger(__name__).info(
            f"Submitted {task_type} Slurm job for {'refdata' if refdata else 'job'}",
            extra={
                logfields.NERSC_JOB_ID: job_id,
                logfields.REFDATA_ID if refdata else logfields.JOB_ID: entity_id
            }
        )
        return job_id

    def _create_download_manifest(
        self,
        download_id: str,
        objects: list[S3ObjectMeta],
        presigned_urls: list[str],
        concurrency: int,
        insecure_ssl: bool,
        refdata: bool,
        unpack: bool
    ) -> io.BytesIO:
        _require_string(download_id, "download_id")
        _not_falsy(objects, "objects")
        _not_falsy(presigned_urls, "presigned_urls")
        if len(objects) != len(presigned_urls):
            raise ValueError("Must provide same number of paths and urls")
        if not all([bool(o.crc64nvme) for o in objects]):
            raise ValueError("All the S3 objects must have a CRC64/NVME checksum")
        manifest = self._base_manifest("download", concurrency, insecure_ssl)
        if refdata:
            # The final refdata location and completion file live under the DTN-only-writable
            # refdata root, so they can't be written directly by this manifest (it runs on a
            # Perlmutter login node). Files are staged here and moved into place, and the
            # completion file written, by a DTN-side step at the end of the Slurm job.
            sc = self._get_refdata_staging_loc(download_id)
        else:
            manifest["cache-dir"] = str(self._nersc_perlmutter_file_cache_path)
        manifest["files"] = []
        for url, meta in zip(presigned_urls, objects):
            fileman = {
                "url": url,
                "crc64nvme-b64": meta.crc64nvme,
                "size": meta.size,
            }
            if refdata:
                fileman["outputpath"] = str(sc / Path(meta.path).name)
                fileman["unpack"] = unpack
            manifest["files"].append(fileman)
        return io.BytesIO(json.dumps({"file-transfers": manifest}, indent=4).encode())
    
    def _create_upload_manifest(
        self,
        remote_files: list[Path],
        presigned_urls: list[PresignedPost],
        concurrency: int,
        insecure_ssl: bool,
    ) -> io.BytesIO:
        _not_falsy(remote_files, "remote_files")
        _not_falsy(presigned_urls, "presigned_urls")
        if len(remote_files) != len(presigned_urls):
            raise ValueError("Must provide same number of files and urls")
        manifest = self._base_manifest("upload", concurrency, insecure_ssl)
        manifest["files"] = [
            {
                "url": url.url,
                "fields": url.fields,
                "file": str(file),
            } for url, file in zip(presigned_urls, remote_files)
        ]
        return io.BytesIO(json.dumps({"file-transfers": manifest}, indent=4).encode())
    
    def _base_manifest(self, op: str, concurrency: int, insecure_ssl: bool):
        return {
            "op": op,
            "concurrency": _check_num(concurrency, "concurrency"),
            "insecure-ssl": insecure_ssl,
            "min-timeout-sec": _MIN_TIMEOUT_SEC,
            "sec-per-GB": _SEC_PER_GB,
        }
    
    async def _download_json_file_from_NERSC(
        self, machine: Machine, path: Path, no_exception_on_missing_file=False
    ) -> dict[str, Any]:
        cli = self._client_provider()
        dtns = await cli.compute(machine)
        try:
            result = await self._get_async_path(dtns, path).download()
        except SfApiError as e:
            if no_exception_on_missing_file and "no such file" in e.message.lower():
                    return None
            raise
        return json.load(result)
    
    async def get_s3_download_result(self, job: models.Job) -> TransferResult:
        """
        Get the results of downloading files to NERSC from s3 for a job.
        """
        return (await self._get_transfer_result(_not_falsy(job, "job").id, "download"))[0]
    
    async def get_s3_refdata_download_result(
        self, refdata: models.ReferenceData
    ) -> TransferResult:
        """
        Get the results of downloading files to NERSC from s3 for refdata.
        """
        return (await self._get_transfer_result(
            _not_falsy(refdata, "refdata").id, "download", refdata=True
        ))[0]

    async def get_presigned_upload_result(self, job: models.Job) -> TransferResult:
        """
        Get the results of uploading files from NERSC to presigned URLs for a job.
        """
        return (await self._get_transfer_result(_not_falsy(job, "job").id, "upload"))[0]
    
    # not thrilled about this api... probably needs a rethink
    async def get_presigned_error_log_upload_result(self, job: models.Job
    ) -> tuple[TransferResult, list[tuple[int, str]] | None]:
        """
        Get the results of uploading logs files from an errored job from
        NERSC to presigned URLs.
        
        Returns a tuple of
        * the result of the transfer
        * a list of tuples consisting of
          * The return code for each container
          * Any error message for each container. These are typically not useful to users.
          * This will be None if the upload failed.
        """
        return await self._get_transfer_result(_not_falsy(job, "job").id, "error_log")

    async def _get_transfer_result(
        self, entity_id: str, op: str, refdata: bool = False
    ) -> tuple[TransferResult, Any]:
        sc = self._get_refdata_scratch(entity_id) if refdata else self._get_job_scratch(entity_id)
        path = sc / f"{op}_result.json"
        res = await self._download_json_file_from_NERSC(
            Machine.perlmutter, path, no_exception_on_missing_file=True
        )
        if not res:
            return TransferResult(state=TransferState.INCOMPLETE), None
        if res["result"] == "success":
            return TransferResult(), res["data"]
        else:
            return TransferResult(
                state=TransferState.FAIL, message=res["job_msg"], traceback=res["job_trace"]
            ), None

    async def run_JAWS(self, job: models.Job, file_download_concurrency: int = 10) -> str:
        """
        Run a JAWS job at NERSC and return the job ID.
        
        job - the job to process
        file_download_concurrency - the number of files at one time to download to NERSC.
        """
        _check_num(file_download_concurrency, "file_download_concurrency")
        site = get_jaws_site(_not_falsy(job, "job").job_input.cluster)
        cli = self._client_provider()
        await self._generate_and_load_job_files_to_nersc(cli, job, file_download_concurrency)
        perl = await cli.compute(Machine.perlmutter)
        pre = self._get_job_scratch(job.id)
        try:
            res = await perl.run(_JAWS_COMMAND_TEMPLATE.format(
                job_id=job.id,
                wdlpath=pre / _JAWS_INPUT_WDL,
                inputjsonpath=pre / _JAWS_INPUT_JSON,
                site=site,
                conf_file=self._jawscfg,
            ))
        except SfApiError as e:
            # TODO ERRORHANDLING if jaws provides valid json parse it and return just the detail
            #try:
            #    j = json.loads(f"{e}")
            #    if "detail" in j:
            #        raise ValueError(f"JAWS error: {j['detail']}") from e
                raise ValueError(f"JAWS error: {e}") from e
            #except json.JSONDecodeError as je:
            #    raise ValueError(f"JAWS returned invalid JSON ({je}) in error: {e}") from e
        try:
            j = json.loads(res)
            if "run_id" not in j:
                raise ValueError(f"JAWS returned no run_id in JSON {res}")
            run_id = j["run_id"]
            logging.getLogger(__name__).info(
                "Submitted JAWS job",
                extra={logfields.JOB_ID: job.id, logfields.JAWS_RUN_ID: run_id}
            )
            return str(run_id)
        except json.JSONDecodeError as e:
            raise ValueError(f"JAWS returned invalid JSON: {e}\n{res}") from e

    async def _generate_and_load_job_files_to_nersc(
        self, cli: AsyncClient, job: models.Job, concurrency: int
    ):
        manifest_files = generate_manifest_files(job)
        manifest_file_paths = self._get_manifest_file_paths(len(manifest_files))
        fmap = {
            m: get_cache_path(self._nersc_perlmutter_file_cache_path, m.crc64nvme)
            for m in job.job_input.input_files
        }
        refpath = None
        if job.image.refdata_id:
            refpath = self._get_relative_refdata_loc(job.image.refdata_id)
        wdljson = wdl.generate_wdl(
            job, fmap, manifest_file_list=manifest_file_paths, relative_refdata_path=refpath
        )
        pre = self._get_job_scratch(job.id)
        downloads = {pre / fp: f for fp, f in zip(manifest_file_paths, manifest_files)}
        downloads[pre / _JAWS_INPUT_WDL] = wdljson.wdl
        downloads[pre / _JAWS_INPUT_JSON] = json.dumps(wdljson.input_json, indent=4)
        perl = await cli.compute(Machine.perlmutter)
        semaphore = asyncio.Semaphore(concurrency)
        async def sem_coro(coro):
            async with semaphore:
                return await coro
        coros = []
        try:
            async with asyncio.TaskGroup() as tg:
                for path, file in downloads.items():
                    coros.append(self._upload_file_to_nersc(
                        perl, path, bio=io.BytesIO(file.encode())
                    ))
                    tg.create_task(sem_coro(coros[-1]))
        except ExceptionGroup as eg:
            e = eg.exceptions[0]  # just pick one, essentially at random
            raise e from eg
        finally:
            # otherwise you can get coroutine never awaited warnings if a failure occurs
            for c in coros:
                c.close()

    def _get_manifest_file_paths(self, count: int) -> list[Path]:
        if count == 0:
            return []
        return [_JOB_MANIFESTS / f"{_MANIFEST_FILE_PREFIX}{c}" for c in range(1, count + 1)]

    async def upload_JAWS_job_files(
        self,
        job: models.Job,
        jaws_output_dir: Path,
        files_to_urls: Callable[[list[Path], list[str]], Awaitable[list[PresignedPost]]],
        callback_url: str,
        concurrency: int = 10,
        insecure_ssl: bool = False
    ) -> str:
        """
        Upload a set of output files from a JAWS run to presigned URLs.
        
        job - the job being processed. No other transfers should be occurring for the job.
        jaws_output_dir - the NERSC output directory of the JAWS job containing the output files,
            manifests, etc.
        files_to_urls - an async function that provides a list of presigned upload urls
            given a list of relative paths to files and a list of their corresponding CRC64/NVME
            checksums. The returned list must be in the same order as the input list.
        callback_url - the URL to GET as a callback for when the upload is complete.
        concurrency - the number of simultaneous uploads to process.
        insecure_ssl - whether to skip the cert check for the S3 URL.

        Returns the NERSC Slurm job ID for the upload.
        """
        _not_falsy(job, "job")
        _not_falsy(files_to_urls, "files_to_urls")
        jaws_output_dir = _require_string(jaws_output_dir, "jaws_output_dir")
        cburl = _require_string(callback_url, "callback_url")
        _check_num(concurrency, "concurrency")
        cli = self._client_provider()
        perl = await cli.compute(Machine.perlmutter)
        rootpath = self._get_job_scratch(job.id)
        checksum_file = _CRC64NVME_CHECKSUMS_JSON_FILE_NAME
        command = [  # similar to the command in _process_manifest
            f"export CTS_MODE=checksum; ",
            f"export CTS_CODE_LOCATION={self._nersc_code_path}; ",
            f"export CTS_RESULT_FILE_LOCATION={rootpath / 'upload_checksums_result.json'}; ",
            f"export CTS_LOG_FILE_LOCATION={rootpath / 'upload_checksums_log.txt'}; ",
            f"export CTS_JAWS_OUTPUT_DIR={jaws_output_dir}; ",
            f"export CTS_CHECKSUM_FILE_LOCATION={rootpath / checksum_file}; ",
            f"export SCRATCH=$SCRATCH; ",
            f'"$CTS_CODE_LOCATION"/{_RUN_CTS_REMOTE_CODE_FILENAME}',
        ]
        command = "".join(command)
        # May want to make this non-blocking if calculating checksums takes too long
        # Would require another set of job states and another callback URL so try to avoid
        await perl.run(command)

        checksumpath = rootpath / checksum_file
        checksums = await self._download_json_file_from_NERSC(Machine.perlmutter, checksumpath)
        s3_paths = []
        crc64nvmes = []
        nersc_rel_paths = []
        total_bytes = 0
        for c in checksums["files"]:
            s3_paths.append(Path(c["s3path"]))
            crc64nvmes.append(c["crc64nvme"])
            nersc_rel_paths.append(c["respath"])
            total_bytes += c["size"]
        presigns = await files_to_urls(s3_paths, crc64nvmes)
        return await self._upload_presigned_files(
            job.id,
            [os.path.join(jaws_output_dir, nrp) for nrp in nersc_rel_paths],
            presigns,
            cburl,
            total_bytes,
            concurrency,
            insecure_ssl,
        )

    async def get_uploaded_JAWS_files(self, job: models.Job) -> dict[str, str]:
        """
        Get the list of files that were uploaded to S3 as part of a successful JAWS job.
        
        Returns a dict of file paths relative to the output directory of a container to their
        CRC64/NVME checksums.
        
        Expects that the upload_JAWS_job_files function has been run, and will error otherwise.
        """
        _not_falsy(job, "job")
        path = self._get_job_scratch(job.id) / _CRC64NVME_CHECKSUMS_JSON_FILE_NAME
        # This uploads the same file from NERSC again. We could put the results in a temporary DB
        # collection if it turns out to be too expensive. YAGNI 
        checksums = await self._download_json_file_from_NERSC(Machine.perlmutter, path)
        return {c["s3path"]: c["crc64nvme"] for c in checksums["files"]}

    async def upload_JAWS_log_files_on_error(
        self,
        job: models.Job,
        jaws_output_dir: Path,
        files_to_urls: Callable[[list[Path]], Awaitable[list[PresignedPost]]],
        callback_url: str,
        concurrency: int = 10,
        insecure_ssl: bool = False
    ) -> str:
        """
        Upload the return code, stdout, and stderr files from a failed JAWS run to
        presigned URLs. The run must be completed with a failed result, otherwise unspecified
        errors may occur.
        
        job - the job being processed. No other transfers should be occurring for the job.
        jaws_output_dir - the NERSC output directory of the JAWS job containing the output files,
            manifests, etc.
        files_to_urls - an async function that provides a list of presigned upload urls
            given log file names. The returned list must be in the same order as the input
            list.
        callback_url - the URL to GET as a callback for when the upload is complete.
        concurrency - the number of simultaneous uploads to process.
        insecure_ssl - whether to skip the cert check for the S3 URL.
        
        Returns the NERSC Slurm job ID for the upload.
        """
        _not_falsy(job, "job")
        _not_falsy(files_to_urls, "files_to_urls")
        cburl = _require_string(callback_url, "callback_url")
        _check_num(concurrency, "concurrency")
        errfilepath = Path(
            _require_string(jaws_output_dir, "jaws_output_dir")) / ERRORS_JSON_FILE
        logs = []
        for i in range(job.job_input.num_containers):
            logs.extend(get_filenames_for_container(i))
        presigns = await files_to_urls(logs)
        
        rootpath = self._get_job_scratch(job.id)
        remotelogs = [rootpath / _JOB_LOGS / f for f in logs]
        # The extracted log files don't exist yet - they're written by the same Slurm job this
        # method submits below, so they can't be stat'd ahead of time to size that job's wall
        # time. Use the size of errors.json itself instead: it already exists (JAWS wrote it),
        # a stat is instant regardless of its size, and its on-disk size is a safe upper bound
        # for the stdout/stderr content that will be extracted from it (JSON string-escaping
        # only inflates size relative to the raw decoded content).
        total_bytes = await self._get_remote_file_sizes([errfilepath])

        manifest = self._create_upload_manifest(remotelogs, presigns, concurrency, insecure_ssl)
        return await self._process_manifest(
            manifest,
            job.id,
            cburl,
            "error_log_upload_manifest.json",
            "error_log",
            total_bytes,
            mode="errorsjson",
            error_json_file_location=errfilepath,
            container_logs_location=str(rootpath / _JOB_LOGS)
        )

    async def _get_remote_file_sizes(self, paths: list[Path]) -> int:
        """
        Get the total size in bytes of a list of files on NERSC. Raises an error if any file
        does not exist.
        """
        cli = self._client_provider()
        perl = await cli.compute(Machine.perlmutter)
        total = 0
        # TODO PERF parallelize the ls calls if this ever needs to handle more than a handful
        #           of paths
        for p in paths:
            entries = await perl.ls(str(p))
            if not entries:
                raise ValueError(f"File does not exist on NERSC: {p}")
            total += int(entries[0].size)
        return total

    async def setup_refdata_transfer_callback(
        self, refdata: models.ReferenceData, site: sites.Cluster, callback_url: str
    ):
        """
        Set up a callback to notify a service when transfer of refdata from NERSC to a remote
        site is complete. Expects that the reference data transfer has already been triggered
        for the remote site.
        
        refdata - the refdata being transferred.
        site - the remote site that's the target of the transfer
        callback_url - the url to GET when the transfer is complete.
        """
        # TODO HACKFIX SOON this is a total hack to deal with the callback endpoints being in
        #      beta. They should be coming out of beta Any Time Now (TM). When that happens,
        #      remove this hack and use the provided client (possibly using the bare POST method
        #      as below if there isn't a specific callback method).
        _not_falsy(refdata, "refdata")
        _not_falsy(site, "site")
        cb_url = _require_string(callback_url, "callback_url")
        token = await self._client_provider().token  # will expire in a few minutes
        sfcli = AsyncClient(api_base_url="https://api.nersc.gov/api/beta", access_token=token)
        payload = {
            "path_condition": {
                "path": str(self._get_refdata_file_complete_path(refdata.id, site)),
                # The completion file lives in the DTN-only-writable refdata area, but that
                # area is readable (just not writable) from Perlmutter login nodes, so the
                # watch itself can run from either machine.
                # TODO VERIFY confirm against a live NERSC callback (beta endpoint, no
                #      automated test coverage) before relying on this in production.
                "machine": Machine.perlmutter.value
            },
            "url": cb_url,
            # seconds. Assume that refdata transfers take less than a day. Make configurable?
            "timeout": 24 * 60 * 60,
        }
        logging.getLogger(__name__).info(
            "Setting up SFAPI callback", extra={logfields.PAYLOAD: payload}
        )
        await sfcli.post("callback", json=payload)
        
    async def get_refdata_transfer_result(
        self, refdata: models.ReferenceData, site: sites.Cluster
    ) -> TransferState:
        """
        Get the state of a refdata transfer from NERSC to a remote site.
        
        refdata - the refdata being transferred.
        site - the remote site that's the target of the transfer
        """
        _not_falsy(refdata, "refdata")
        _not_falsy(site, "site")
        # similar to _get_transfer_result, but not similar enough to warrant DRYing things up
        path = self._get_refdata_file_complete_path(refdata.id, site)
        res = await self._download_json_file_from_NERSC(
            Machine.perlmutter, path, no_exception_on_missing_file=True
        )
        if not res:
            return TransferResult(state=TransferState.INCOMPLETE)
        if res["result"] == "succeeded":
            return TransferResult()
        else:
            return TransferResult(
                state=TransferState.FAIL,
                message="JAWS indicates that the reference data transfer failed",
            )

    async def clean_job(self, job: models.Job, jaws_output_dirs: list[Path]):
        """
        Remove any files at NERSC associated with the job other than files managed by
        JAWS.
        
        If any files do not exist they are silently ignored.
        
        Note that running this method on a job that is not in a terminal state may result in
        undefined behavior.
        
        job - the job to clean up.
        jaws_output_dirs - the output directories of the jaws jobs, obtained from the jaws status
            command. Typically only a single directory.
        """
        # Passing in the JAWS output dirs seems a bit odd in this case but it's the way it's
        # done for all the other methods that deal with it.
        # The alternative would be passing in a JAWS client in the constructor and looking it
        #  up, in which case we should do the same elsewhere in this file, but that's an
        # extra JAWS call for nothing...
        to_delete = list(jaws_output_dirs or [])  # if jaws_output_dirs is None or empty
        to_delete.append(self._get_job_scratch(_not_falsy(job, "job").id))
        await self._delete_paths(to_delete)

    async def clean_refdata(self, refdata: models.ReferenceData):
        """
        Remove any files at NERSC associat4ed with staging the reference data. Does not remove
        the reference data itself or the NERSC -> LRC sync notification file read by JAWS.
        
        If any files do not exist they are silently ignored.
        
        Note that running this method on reference data where staging is not in a terminal
        state may result in undefined behavior.
        """
        await self._delete_paths([self._get_refdata_scratch(refdata.id)])
