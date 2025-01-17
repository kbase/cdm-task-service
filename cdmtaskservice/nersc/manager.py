'''
Handler for data transfer between CDM sources and NERSC.
'''

import asyncio
from collections.abc import Callable
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

from cdmtaskservice import models
from cdmtaskservice.arg_checkers import (
    not_falsy as _not_falsy,
    require_string as _require_string,
    check_num as _check_num,
)
from cdmtaskservice.jaws import (
    wdl,
    output as jaws_output
)
from cdmtaskservice.jaws.remote import get_filenames_for_container, ERRORS_JSON_FILE
from cdmtaskservice.manifest_files import generate_manifest_files
from cdmtaskservice.nersc import remote
from cdmtaskservice.s3.client import S3ObjectMeta, PresignedPost

# This is mostly tested manually to avoid modifying files at NERSC.

# TODO TEST add tests in test_manual
# TODO TEST add automated tests for stuff that doesn't contact nersc (arg checks etc.)
# TODO ERRORHANDLING wrap sfapi errors in server specific errors

# TODO CLEANUP clean up old code versions @NERSC somehow. Not particularly important
#              actually this is impoosible to do safely - could be a different cluster of
#              servers on a different version. Note in admin docs that old unused installs
#              can be deleted. Server code is tiny anyway

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
_MD5_JSON_FILE_NAME = "upload_md5s.json"
_JOB_LOGS = "logs"


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
_JAWS_SITE_PERLMUTTER = "kbase"  # add lawrencium later, maybe
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
export CTS_MD5_FILE_LOCATION=$CTS_MD5_FILE_LOCATION
export CTS_RESULT_FILE_LOCATION=$CTS_RESULT_FILE_LOCATION
export CTS_CALLBACK_URL=$CTS_CALLBACK_URL
export SCRATCH=$SCRATCH

echo "PYTHONPATH=[$PYTHONPATH]"
echo "CTS_MODE=[$CTS_MODE]"
echo "CTS_MANIFEST_LOCATION=[$CTS_MANIFEST_LOCATION]"
echo "CTS_ERRORS_JSON_LOCATION=[$CTS_ERRORS_JSON_LOCATION]"
echo "CTS_CONTAINER_LOGS_LOCATION=[$CTS_CONTAINER_LOGS_LOCATION]"
echo "CTS_MD5_FILE_LOCATION=[$CTS_MD5_FILE_LOCATION]"
echo "CTS_RESULT_FILE_LOCATION=[$CTS_RESULT_FILE_LOCATION]"
echo "CTS_CALLBACK_URL=[$CTS_CALLBACK_URL]"
echo "SCRATCH=[$SCRATCH]"

python $CTS_CODE_LOCATION/{"/".join(remote.__name__.split("."))}.py
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
            pip_dep.add(m)
_get_dependencies(remote, _CTS_DEPENDENCIES, _PIP_DEPENDENCIES)


class TransferResult(NamedTuple):
    """ Results of a file transfer between NERSC and another location. """
    
    success: bool = True
    """ True if the file transfer succeeded. """
    
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
        jaws_token: str,
        jaws_group: str,
        service_group: str = "dev",
    ) -> Self:
        """
        Create the NERSC manager.
        
        client_provider - a function that provides a valid SFAPI client. It is assumed that
            the user associated with the client does not change.
        nersc_code_path - the path in which to store remote code at NERSC. It is advised to
            include version information in the path to avoid code conflicts.
        jaws_token - a token for the JGI JAWS system.
        jaws_group - the group to use for running JAWS jobs.
        """
        nm = NERSCManager(client_provider, nersc_code_path, service_group)
        await nm._setup_remote_code(
            _require_string(jaws_token, "jaws_token"),
            _require_string(jaws_group, "jaws_group"),
        )
        return nm
        
    def __init__(
            self,
            client_provider: Callable[[], str],
            nersc_code_path: Path,
            service_group: str,
        ):
        self._client_provider = _not_falsy(client_provider, "client_provider")
        self._nersc_code_path = self._check_path(nersc_code_path, "nersc_code_path")
        self._scratchdir = Path("cdm_task_service") / service_group
        self._jawscfg = f"jaws_cts_{service_group}.conf"

    def _check_path(self, path: Path, name: str):
        _not_falsy(path, name)
        # commands are ok with relative paths but file uploads are not
        if path.expanduser().absolute() != path:
            raise ValueError(f"{name} must be absolute to the NERSC root dir")
        return path

    async def _setup_remote_code(self, jaws_token: str, jaws_group: str):
        # TODO RELIABILITY atomically write files. For these small ones probably doesn't matter?
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
        logging.getLogger(__name__).info(
            f"NERSC perlmutter scratch path: {self._perlmutter_scratch}"
        )
    
    async def _set_up_dtn_scratch(self, client: AsyncClient) -> Path:
        dt = await client.compute(_DT_TARGET)
        scratch = await dt.run(f"{_DT_WORKAROUND}; echo $SCRATCH")
        scratch = scratch.strip()
        if not scratch:
            raise ValueError("Unable to determine $SCRATCH variable for NERSC dtns")
        logging.getLogger(__name__).info(f"NERSC DTN scratch path: {scratch}")
        return Path(scratch)
    
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
        if chmod:
            cmd = f"{dtw}chmod {chmod} {target}"
            await compute.run(cmd)

    def _get_async_path(self, compute: AsyncCompute, target: Path) -> AsyncRemotePath:
        # skip some API calls vs. the upload example in the NERSC docs
        # don't use a directory as the target or it makes an API call
        asrp = AsyncRemotePath(path=target, compute=compute)
        asrp.perms = "-"  # hack to prevent an unnecessary network call
        return asrp

    async def is_task_complete(self, task_id: str) -> bool:
        """
        Returns true if the the task is complete, signified by:
        * The task data denoting it as complete, or
        * The task not being available in the NERSC SFAPI (e.g. HTTP 404), as tasks are removed
          10 minutes after completion.
        The task may be in a successful or errored state.
        
        Note that if a task ID never existed, this method will return that it is
        complete, as there is no way to tell the difference.
        """
        cli = self._client_provider()
        try:
            task = await cli.get(f"tasks/{task_id}")
        except HTTPStatusError as e:
            if e.response.status_code == 404:
                return True
            raise
        return task.json()["status"] == "completed"

    async def download_s3_files(
        self,
        job_id: str,
        objects: list[S3ObjectMeta],
        presigned_urls: list[str],
        callback_url: str,
        concurrency: int = 10,
        insecure_ssl: bool = False
    ) -> str:
        """
        Download a set of files to NERSC from an S3 instance.
        
        job_id - the ID of the job for which the files are being transferred. 
            This must be a unique ID, and no other transfers should be occurring for the job.
        objects - the S3 files to download.
        presigned_urls - the presigned download URLs for each object, in the same order as
            the objects.
        callback_url - the URL to GET as a callback for when the download is complete.
        concurrency - the number of simultaneous downloads to process.
        insecure_ssl - whether to skip the cert check for the S3 URL.
        
        Returns the NERSC task ID for the download.
        """
        maniio = self._create_download_manifest(
            job_id, objects, presigned_urls, concurrency, insecure_ssl)
        return await self._process_manifest(
            maniio, job_id, callback_url, "download_manifest.json", "download")
    
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
            maniio, job_id, callback_url, "upload_manifest.json", "upload", upload_md5s_file=True
        )
    
    async def _process_manifest(
        self,
        manifest: io.BytesIO,
        job_id: str,
        callback_url: str,
        filename: str,
        task_type: str,
        mode="manifest",
        error_json_file_location: str = None,
        container_logs_location: str = None,  # this is expected to be present if the above is
        upload_md5s_file: bool = False,
    ):
        rootpath = self._dtn_scratch / self._scratchdir / job_id
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
        if upload_md5s_file:
            command.insert(2, f"export CTS_MD5_FILE_LOCATION={rootpath / _MD5_JSON_FILE_NAME}; ")
        if error_json_file_location:
            command.insert(2, f"export CTS_ERRORS_JSON_LOCATION={error_json_file_location}; ")
            command.insert(3, f"export CTS_CONTAINER_LOGS_LOCATION={container_logs_location}; ")
        command = "".join(command)
        task_id = (await self._run_command(cli, _DT_TARGET, command))["task_id"]
        # TODO LOGGING figure out how to handle logging, see other logging todos
        logging.getLogger(__name__).info(
            f"Created {task_type} task with id {task_id} for job {job_id}")
        return task_id
    
    def _create_download_manifest(
        self,
        job_id: str,
        objects: list[S3ObjectMeta],
        presigned_urls: list[str],
        concurrency: int,
        insecure_ssl: bool,
    ) -> io.BytesIO:
        _require_string(job_id, "job_id")
        _not_falsy(objects, "objects")
        _not_falsy(presigned_urls, "presigned_urls")
        if len(objects) != len(presigned_urls):
            raise ValueError("Must provide same number of paths and urls")
        manifest = self._base_manifest("download", concurrency, insecure_ssl)
        manifest["files"] = [
            {
                "url": url,
                # TODO CACHING have the remote code make a file cache - just give it the root,
                #              job ID and the minio path and have it handle the rest.
                #              This allows JAWS / Cromwell to cache the files if they have the
                #              same path, which they won't if there's a job ID in the mix
                "outputpath": self._localize_s3_path(job_id, meta.path),
                "etag": meta.e_tag,
                "partsize": meta.effective_part_size,
                "size": meta.size,
            } for url, meta in zip(presigned_urls, objects)
        ]
        return io.BytesIO(json.dumps({"file-transfers": manifest}).encode())
    
    def _localize_s3_path(self, job_id: str, s3path: str) -> str:
        return str(self._dtn_scratch / self._scratchdir/ job_id / _JOB_FILES / s3path)
    
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
        return io.BytesIO(json.dumps({"file-transfers": manifest}).encode())
    
    def _base_manifest(self, op: str, concurrency: int, insecure_ssl: bool):
        return {
            "op": op,
            "concurrency": _check_num(concurrency, "concurrency"),
            "insecure-ssl": insecure_ssl,
            "min-timeout-sec": _MIN_TIMEOUT_SEC,
            "sec-per-GB": _SEC_PER_GB,
        }
    
    async def _download_json_file_from_NERSC(self, machine: Machine, path: Path) -> dict[str, Any]:
        cli = self._client_provider()
        dtns = await cli.compute(machine)
        result = await self._get_async_path(dtns, path).download()
        return json.load(result)
    
    async def get_s3_download_result(self, job: models.Job) -> TransferResult:
        """
        Get the results of downloading files to NERSC from s3 for a job.
        """
        return (await self._get_transfer_result(job, "download"))[0]

    async def get_presigned_upload_result(self, job: models.Job) -> TransferResult:
        """
        Get the results of uploading files from NERSC to presigned URLs for a job.
        """
        return (await self._get_transfer_result(job, "upload"))[0]
    
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
        return await self._get_transfer_result(job, "error_log")

    async def _get_transfer_result(self, job: models.Job, op: str) -> tuple[str, str] | None:
        _not_falsy(job, "job")
        path = self._dtn_scratch / self._scratchdir / job.id / f"{op}_result.json"
        res = await self._download_json_file_from_NERSC(Machine.dtns, path)
        if res["result"] == "success":
            return TransferResult(), res["data"]
        else:
            return TransferResult(
                success=False, message=res["job_msg"], traceback=res["job_trace"]
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
        cli = self._client_provider()
        await self._generate_and_load_job_files_to_nersc(cli, job, file_download_concurrency)
        perl = await cli.compute(Machine.perlmutter)
        pre = self._perlmutter_scratch / self._scratchdir / job.id
        try:
            # TODO PERF this copies all the files to the jaws staging area, and so could take
            #           a long time. Hardlink them in first to avoid the copy. Also look into
            #           caching between jobs, so multiple jobs on the same file don't DL it over
            #           and over
            res = await perl.run(_JAWS_COMMAND_TEMPLATE.format(
                job_id=job.id,
                wdlpath=pre / _JAWS_INPUT_WDL,
                inputjsonpath=pre / _JAWS_INPUT_JSON,
                site=_JAWS_SITE_PERLMUTTER,
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
                f"Submitted JAWS job with run id {run_id} for job {job.id}"
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
        wdljson = wdl.generate_wdl(job, fmap, manifest_file_paths)
        pre = self._dtn_scratch / self._scratchdir / job.id
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
        files_to_urls: Callable[[list[Path]], Awaitable[list[PresignedPost]]],
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
            given relative paths to files. The returned list must be in the same order as the input
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
        outfilepath = Path(
            _require_string(jaws_output_dir, "jaws_output_dir")) / jaws_output.OUTPUTS_JSON_FILE
        cli = self._client_provider()
        dtns = await cli.compute(Machine.dtns)
        outputs_json = await self._get_async_path(dtns, outfilepath).download()
        outputs = jaws_output.parse_outputs_json(outputs_json)
        container_files = list(outputs.output_files.keys())
        presigns = await files_to_urls(container_files)
        return await self.upload_presigned_files(
            job.id,
            [os.path.join(jaws_output_dir, outputs.output_files[f]) for f in container_files],
            presigns,
            cburl,
            concurrency,
            insecure_ssl,
        )

    async def get_uploaded_JAWS_files(self, job: models.Job) -> dict[str, str]:
        """
        Get the list of files that were uploaded to S3 as part of a JAWS job.
        
        Returns a dict of file paths relative to the output directory of a container to their
        MD5s.
        
        Expects that the upload_JAWS_job_files function has been run, and will error otherwise.
        """
        _not_falsy(job, "job")
        path = self._dtn_scratch / self._scratchdir / job.id / _MD5_JSON_FILE_NAME
        file_to_md5 = await self._download_json_file_from_NERSC(Machine.dtns, path)
        return {jaws_output.get_relative_file_path(f): md5 for f, md5 in file_to_md5.items()}

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
        
        rootpath = self._dtn_scratch / self._scratchdir / job.id
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
