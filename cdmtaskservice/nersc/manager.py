'''
Handler for data transfer between CDM sources and NERSC.
'''

import asyncio
from collections.abc import Callable
from enum import Enum
from httpx import HTTPStatusError
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
from cdmtaskservice.jaws.remote import get_filenames_for_container, ERRORS_JSON_FILE
from cdmtaskservice.manifest_files import generate_manifest_files
from cdmtaskservice.nersc import remote
from cdmtaskservice.s3.client import S3ObjectMeta, PresignedPost

# This is mostly tested manually to avoid modifying files at NERSC.

# TODO TEST add tests in test_manual
# TODO TEST add automated tests for stuff that doesn't contact nersc (arg checks etc.)
# TODO ERRORHANDLING wrap sfapi errors in server specific errors

# TODO CLEANUP clean up old code versions @NERSC somehow. Not particularly important
#              actually this is impossible to do safely - could be a different cluster of
#              servers on a different version. Note in admin docs that old unused installs
#              can be deleted. Server code is tiny anyway

_CLUSTER_TO_JAWS_SITE = {
    # It seems very unlikely, but we may need to add other JAWS sites for groups other than kbase
    # If we do that the perlmutter-jaws name is unfortunate since there are multiple virtual
    # sites there
    sites.Cluster.PERLMUTTER_JAWS: "kbase",  # what would they call a site actually at kbase...?
    sites.Cluster.LAWRENCIUM_JAWS: "jgi",  # historical name
}

_DT_TARGET = Machine.dtns
# TODO NERSCUPDATE delete the following line when DTN downloads work normally.
#       See https://nersc.servicenowservices.com/sp?sys_id=ad33e85f1b5a5610ac81a820f54bcba0&view=sp&id=ticket&table=incident
_DT_WORKAROUND = "source /etc/bashrc"

_COMMAND_PATH = "utilities/command"

_MIN_TIMEOUT_SEC = 300
_SEC_PER_GB = 2 * 60  # may want to make this configurable

_JOB_FILES = Path("files")
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
# TODO NERSCFEATURE if NERSC puts python 3.11 on the dtns revert to regular load 
_PYTHON_LOAD_HACK = "module use /global/common/software/nersc/pe/modulefiles/latest"
_RUN_CTS_REMOTE_CODE_FILENAME = "run_cts_remote_code.sh"
# Might want to make a shared constants module for all these env var names and update this
# file and remote.py
_RUN_CTS_REMOTE_CODE = f"""
#!/usr/bin/env bash

{_PYTHON_LOAD_HACK}
module load python

export PYTHONPATH=$CTS_CODE_LOCATION
export CTS_MODE=$CTS_MODE
export CTS_MANIFEST_LOCATION=$CTS_MANIFEST_LOCATION
export CTS_ERRORS_JSON_LOCATION=$CTS_ERRORS_JSON_LOCATION
export CTS_CONTAINER_LOGS_LOCATION=$CTS_CONTAINER_LOGS_LOCATION
export CTS_JAWS_OUTPUT_DIR=$CTS_JAWS_OUTPUT_DIR
export CTS_CHECKSUM_FILE_LOCATION=$CTS_CHECKSUM_FILE_LOCATION
export CTS_RESULT_FILE_LOCATION=$CTS_RESULT_FILE_LOCATION
export CTS_CALLBACK_URL=$CTS_CALLBACK_URL
export SCRATCH=$SCRATCH

echo "PYTHONPATH=[$PYTHONPATH]"
echo "CTS_MODE=[$CTS_MODE]"
echo "CTS_MANIFEST_LOCATION=[$CTS_MANIFEST_LOCATION]"
echo "CTS_ERRORS_JSON_LOCATION=[$CTS_ERRORS_JSON_LOCATION]"
echo "CTS_CONTAINER_LOGS_LOCATION=[$CTS_CONTAINER_LOGS_LOCATION]"
echo "CTS_JAWS_OUTPUT_DIR=[$CTS_JAWS_OUTPUT_DIR]
echo "CTS_CHECKSUM_FILE_LOCATION=[$CTS_CHECKSUM_FILE_LOCATION]
echo "CTS_RESULT_FILE_LOCATION=[$CTS_RESULT_FILE_LOCATION]"
echo "CTS_CALLBACK_URL=[$CTS_CALLBACK_URL]"
echo "SCRATCH=[$SCRATCH]"

python $CTS_CODE_LOCATION/{"/".join(remote.__name__.split("."))}.py
"""


# Note there's a race condition that theoretically could happen here if the path is removed
# after the existence check but before the rm. Since this is just for clean up not an issue. 
# Also note I wasted way too much time trying to make this fail cleanly if there was a write
# protected file in the path tree, which should never happen.
# It'll still fail if it really can't delete the path.
_REMOVE_DTN_PATH_TEMPLATE = f"""
{_DT_WORKAROUND}
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
        nersc_code_path: Path,
        nersc_jaws_user: str,
        jaws_token: str,
        jaws_group: str,
        jaws_refdata_root_dir: Path,
        service_group: str = "dev",
    ) -> Self:
        """
        Create the NERSC manager.
        
        client_provider - a function that provides a valid SFAPI client. It is assumed that
            the user associated with the client does not change.
        nersc_code_path - the path in which to store remote code at NERSC. It is advised to
            include version information in the path to avoid code conflicts. The path is appended
            by the service group to separate dev and prod environments.
        nersc_jaws_user - the NERSC username of the user associated with the jaws token.
            This is typically a collaboration account.
        jaws_token - a token for the JGI JAWS system.
        jaws_group - the group to use for running JAWS jobs.
        jaws_refdata_root_dir - the root directory for refdata configured for the JAWS instance.
        service_group - The service group to which this instance of the manager belongs.
            This is used to separate files at NERSC so files from different S3 instances
            (say production and development) don't collide.
        """
        nm = NERSCManager(
            client_provider, nersc_code_path, nersc_jaws_user, service_group, jaws_refdata_root_dir
        )
        await nm._setup_remote_code(
            _require_string(jaws_token, "jaws_token"),
            _require_string(jaws_group, "jaws_group"),
        )
        return nm
        
    def __init__(
            self,
            client_provider: Callable[[], str],
            nersc_code_path: Path,
            nesrc_jaws_user: str,
            service_group: str,
            jaws_refdata_root_dir: str,
        ):
        self._client_provider = _not_falsy(client_provider, "client_provider")
        self._nersc_code_path = self._check_path(
            nersc_code_path, "nersc_code_path"
        ) / service_group
        self._nersc_jaws_user = _require_string(nesrc_jaws_user, "nesrc_jaws_user")
        self._service_group = _require_string(service_group, "service_group")
        self._work_loc = Path("cdm_task_service") / service_group
        self._jawscfg = f"jaws_cts_{service_group}.conf"
        self._refdata_root = self._check_path(jaws_refdata_root_dir, "jaws_refdata_root_dir")

    def _check_path(self, path: Path, name: str):
        _not_falsy(path, name)
        # commands are ok with relative paths but file uploads are not
        if path.expanduser().absolute() != path:
            raise ValueError(f"{name} must be absolute to the NERSC root dir")
        return path

    async def _setup_remote_code(self, jaws_token: str, jaws_group: str):
        # TODO RELIABILITY atomically write files. For these small ones probably doesn't matter?
        logr = logging.getLogger(__name__)
        cli = self._client_provider()
        perlmutter = await cli.compute(Machine.perlmutter)
        dt = await cli.compute(_DT_TARGET)
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
            pm_scratch = tg.create_task(perlmutter.run("echo $SCRATCH"))
            dtn_scratch = tg.create_task(self._set_up_dtn_scratch(cli))
            if _PIP_DEPENDENCIES:
                deps = " ".join(
                    # may need to do something else if module doesn't have __version__
                    [f"{mod.__name__}=={mod.__version__}" for mod in _PIP_DEPENDENCIES])
                logr.info(f"Installing pip modules @ NERSC: {deps}")
                command = (
                    f"{_DT_WORKAROUND}; "
                    + f"{_PYTHON_LOAD_HACK}; "
                    + f"module load python; "
                    # Unlikely, but this could cause problems if multiple versions
                    # of the server are running at once. Don't worry about it for now 
                    + f"pip install {deps}"  # adding notapackage causes a failure
                )
                tg.create_task(dt.run(command))
        self._dtn_scratch = dtn_scratch.result()
        self._perlmutter_scratch = Path(pm_scratch.result().strip())
        logr.info(
            "NERSC perlmutter scratch path", extra={logfields.FILE: self._perlmutter_scratch}
        )
    
    async def _set_up_dtn_scratch(self, client: AsyncClient) -> Path:
        dt = await client.compute(_DT_TARGET)
        scratch = await dt.run(f"{_DT_WORKAROUND}; echo $SCRATCH")
        scratch = scratch.strip()
        if not scratch:
            raise ValueError("Unable to determine $SCRATCH variable for NERSC dtns")
        logging.getLogger(__name__).info(
            "NERSC DTN scratch path", extra={logfields.FILE: scratch}
        )
        return Path(scratch)
    
    def _get_job_scratch(self, job_id, perlmutter=False) -> Path:
        sc = self._perlmutter_scratch if perlmutter else self._dtn_scratch
        return sc / self._work_loc / _JOBS_DIR / job_id
    
    def _get_refdata_scratch(self, refdata_id) -> Path:
        return self._dtn_scratch / self._work_loc / _REFDATA_DIR / refdata_id
    
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
    
    async def _run_command(self, client: AsyncClient, machine: Machine, exe: str):
        # TODO ERRORHANDlING deal with errors 
        return (await client.post(f"{_COMMAND_PATH}/{machine}", data={"executable": exe})).json()
    
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
        dtw = f"{_DT_WORKAROUND}; " if compute.name == Machine.dtns else ""
        if target.parent != Path("."):
            cmd = f"{dtw}mkdir -p {target.parent}"
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
            cmd = f"{dtw}chmod {chmod} {target}"
            await compute.run(cmd)
            logr.info("chmod of uploaded file complete.", extra={logfields.FILE: target})

    async def _delete_dtn_paths(self, paths: list[Path]):
        logr = logging.getLogger(__name__)
        cli = self._client_provider()
        dtns = await cli.compute(Machine.dtns)
        async with asyncio.TaskGroup() as tg:
            for p in paths:
                logr.info("Deleting path at NERSC", extra={logfields.FILE: p})
                # May need to catch SFAPI client errors and wrap. YAGNI for now
                tg.create_task(dtns.run(_REMOVE_DTN_PATH_TEMPLATE.format(path=p)))

    def _get_async_path(self, compute: AsyncCompute, target: Path) -> AsyncRemotePath:
        # skip some API calls vs. the upload example in the NERSC docs
        # don't use a directory as the target or it makes an API call
        asrp = AsyncRemotePath(path=target, compute=compute)
        asrp.perms = "-"  # hack to prevent an unnecessary network call
        return asrp

    async def is_task_complete(self, task_id: str, max_retries=10) -> bool:
        """
        Returns true if the the task is complete, signified by:
        * The task data denoting it as complete, or
        * The task not being available in the NERSC SFAPI (e.g. HTTP 404), as tasks are removed
          10 minutes after completion.
        The task may be in a successful or errored state. Polls the task 1/s up to `max_retries`.
        
        Note that if a task ID never existed, this method will return that it is
        complete, as there is no way to tell the difference.
        """
        logr = logging.getLogger(__name__)
        cli = self._client_provider()
        retries = 1
        while True:
            try:
                task = await cli.get(f"tasks/{task_id}")
            except HTTPStatusError as e:
                if e.response.status_code == 404:
                    return True
                raise
            if task.json()["status"] == "completed":
                return True
            elif retries > max_retries:
                return False
            else:
                logr.info(f"Polling state of task {task_id} in 1s, retry attempt {retries}")
                retries += 1
                await asyncio.sleep(1)

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
        
        Returns the NERSC task ID for the download.
        """
        maniio = self._create_download_manifest(
            download_id, objects, presigned_urls, concurrency, insecure_ssl, refdata, unpack)
        return await self._process_manifest(
            maniio,
            download_id,
            callback_url,
            "download_manifest.json",
            "download",
            refdata=refdata,
        )
    
    async def upload_presigned_files(
        self,
        job_id: str,
        remote_files: list[Path],
        presigned_urls: list[PresignedPost],
        callback_url: str,
        concurrency: int = 10,
        insecure_ssl: bool = False
    ) -> str:
        """
        Upload a set of files to presigned URLs from NERSC.
        
        job_id - the ID of the job for which the files are being transferred. 
            This must be a unique ID, and no other transfers should be occurring for the job.
        remote_files - the files to upload.
        presigned_urls - the presigned upload URLs for each file, in the same order as
            the file.
        callback_url - the URL to GET as a callback for when the upload is complete.
        concurrency - the number of simultaneous uploads to process.
        insecure_ssl - whether to skip the cert check for the S3 URL.
        
        Returns the NERSC task ID for the upload.
        """
        maniio = self._create_upload_manifest(
            remote_files, presigned_urls, concurrency, insecure_ssl)
        return await self._process_manifest(
            maniio, job_id, callback_url, "upload_manifest.json", "upload"
        )
    
    async def _process_manifest(
        self,
        manifest: io.BytesIO,
        entity_id: str,
        callback_url: str,
        filename: str,
        task_type: str,
        mode="manifest",
        error_json_file_location: str = None,
        container_logs_location: str = None,  # this is expected to be present if the above is
        refdata: bool = False
    ):
        if refdata:
            rootpath = self._get_refdata_scratch(entity_id)
        else:
            rootpath = self._get_job_scratch(entity_id)
        manifestpath = rootpath / filename
        cli = self._client_provider()
        dt = await cli.compute(_DT_TARGET)
        # TODO CLEANUP manifests after some period of time
        await self._upload_file_to_nersc(dt, manifestpath, bio=manifest)
        command = [
            f"{_DT_WORKAROUND}; ",
            f"export CTS_MODE={mode}; ",
            f"export CTS_CODE_LOCATION={self._nersc_code_path}; ",
            f"export CTS_MANIFEST_LOCATION={manifestpath}; ",
            f"export CTS_RESULT_FILE_LOCATION={rootpath / task_type}_result.json; ",
            f"export CTS_CALLBACK_URL={callback_url}; ",
            f"export SCRATCH=$SCRATCH; ",
            f'"$CTS_CODE_LOCATION"/{_RUN_CTS_REMOTE_CODE_FILENAME}',
        ]
        if error_json_file_location:
            command.insert(2, f"export CTS_ERRORS_JSON_LOCATION={error_json_file_location}; ")
            command.insert(3, f"export CTS_CONTAINER_LOGS_LOCATION={container_logs_location}; ")
        command = "".join(command)
        task_id = (await self._run_command(cli, _DT_TARGET, command))["task_id"]
        logging.getLogger(__name__).info(
            f"Created {task_type} task for {'refdata' if refdata else 'job'}",
            extra={
                logfields.NERSC_TASK_ID: task_id,
                logfields.REFDATA_ID if refdata else logfields.JOB_ID: entity_id
            }
        )
        return task_id
    
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
            sc = self._get_refdata_loc(download_id)
            # TODO CLEANUP need to delete this and the JAWS written completion file
            # see https://jaws-docs.jgi.doe.gov/en/latest/jaws/jaws_refdata.html#adding-data-to-refdata-directory
            manifest["completion_file"] = str(self._get_refdata_file_change_path(download_id))
            manifest["completion_file_contents"] = str(sc)
        else:
            sc = self._get_job_scratch(download_id) / _JOB_FILES
        manifest["files"] = [
            {
                "url": url,
                # TODO CACHING have the remote code make a file cache - just give it the root,
                #              job ID and the minio path and have it handle the rest.
                #              This allows JAWS / Cromwell to cache the files if they have the
                #              same path, which they won't if there's a job ID in the mix
                "outputpath": str(sc / Path(meta.path).name if refdata else sc / meta.path),
                "crc64nvme": meta.crc64nvme,
                "size": meta.size,
                "unpack": unpack,
            } for url, meta in zip(presigned_urls, objects)
        ]
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
            Machine.dtns, path, no_exception_on_missing_file=True
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
        if not _not_falsy(job, "job").job_input.inputs_are_S3File():
            raise ValueError("Job files must be S3File objects")
        # getting a key error here is a programming error, but do it before pushing stuff to
        # NERSC just in case
        site = _CLUSTER_TO_JAWS_SITE[job.job_input.cluster]
        cli = self._client_provider()
        await self._generate_and_load_job_files_to_nersc(cli, job, file_download_concurrency)
        perl = await cli.compute(Machine.perlmutter)
        pre = self._get_job_scratch(job.id, perlmutter=True)
        try:
            # TODO PERF this copies all the files to the jaws staging area, and so could take
            #           a long time. Hardlink them in first to avoid the copy. Also look into
            #           caching between jobs, so multiple jobs on the same file don't DL it over
            #           and over
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
        fmap = {m: _JOB_FILES / m.file for m in job.job_input.input_files}
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
        dt = await cli.compute(_DT_TARGET)
        semaphore = asyncio.Semaphore(concurrency)
        async def sem_coro(coro):
            async with semaphore:
                return await coro
        coros = []
        try:
            async with asyncio.TaskGroup() as tg:
                for path, file in downloads.items():
                    coros.append(self._upload_file_to_nersc(
                        dt, path, bio=io.BytesIO(file.encode())
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
        
        Returns the NERSC task ID for the upload.
        """
        _not_falsy(job, "job")
        _not_falsy(files_to_urls, "files_to_urls")
        jaws_output_dir = _require_string(jaws_output_dir, "jaws_output_dir")
        cburl = _require_string(callback_url, "callback_url")
        _check_num(concurrency, "concurrency")
        cli = self._client_provider()
        dtns = await cli.compute(Machine.dtns)
        rootpath = self._get_job_scratch(job.id)
        checksum_file = _CRC64NVME_CHECKSUMS_JSON_FILE_NAME
        command = [  # similar to the command in _process_manifest
            f"{_DT_WORKAROUND}; ",
            f"export CTS_MODE=checksum; ",
            f"export CTS_CODE_LOCATION={self._nersc_code_path}; ",
            f"export CTS_RESULT_FILE_LOCATION={rootpath / 'upload_checksums_result.json'}; ",
            f"export CTS_JAWS_OUTPUT_DIR={jaws_output_dir}; ",
            f"export CTS_CHECKSUM_FILE_LOCATION={rootpath / checksum_file}; ",
            f"export SCRATCH=$SCRATCH; ",
            f'"$CTS_CODE_LOCATION"/{_RUN_CTS_REMOTE_CODE_FILENAME}',
        ]
        command = "".join(command)
        # May want to make this non-blocking if calculating checksums takes too long
        # Would require another set of job states and another callback URL so try to avoid
        await dtns.run(command)
        
        checksumpath = rootpath / checksum_file
        checksums = await self._download_json_file_from_NERSC(Machine.dtns, checksumpath)
        s3_paths = []
        crc64nvmes = []
        nersc_rel_paths = []
        for c in checksums["files"]:
            s3_paths.append(Path(c["s3path"]))
            crc64nvmes.append(c["crc64nvme"])
            nersc_rel_paths.append(c["respath"])
        presigns = await files_to_urls(s3_paths, crc64nvmes)
        return await self.upload_presigned_files(
            job.id,
            [os.path.join(jaws_output_dir, nrp) for nrp in nersc_rel_paths],
            presigns,
            cburl,
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
        checksums = await self._download_json_file_from_NERSC(Machine.dtns, path)
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
        
        Returns the NERSC task ID for the upload.
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
        
        manifest = self._create_upload_manifest(remotelogs, presigns, concurrency, insecure_ssl)
        return await self._process_manifest(
            manifest,
            job.id,
            cburl,
            "error_log_upload_manifest.json",
            "error_log",
            mode="errorsjson",
            error_json_file_location=errfilepath,
            container_logs_location=str(rootpath / _JOB_LOGS)
        )

    async def clean_job(self, job: models.Job, jaws_output_dirs: list[Path]):
        """
        Remove any files at NERSC associated with the job other than files managed by
        JAWS.
        
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
        await self._delete_dtn_paths(to_delete)

    async def clean_refdata(self, refdata: models.ReferenceData):
        """
        Remove any files at NERSC associat4ed with staging the reference data. Does not remove
        the reference data itself or the NERSC -> LRC sync notification file read by JAWS.
        
        Note that running this method on reference data where staging is not in a terminal
        state may result in undefined behavior.
        """
        await self._delete_dtn_paths([self._get_refdata_scratch(refdata.id)])
