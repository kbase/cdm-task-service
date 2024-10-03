'''
Handler for data transfer between CDM sources and NERSC.
'''

import asyncio
from collections.abc import Awaitable
import io
import inspect
import json
import logging
from pathlib import Path
from sfapi_client import AsyncClient
from sfapi_client.paths import AsyncRemotePath
from sfapi_client.compute import Machine, AsyncCompute
import sys
from types import ModuleType
from typing import Self

from cdmtaskservice.arg_checkers import not_falsy, require_string, check_int
from cdmtaskservice.nersc import remote
from cdmtaskservice.s3.client import S3ObjectMeta, S3PresignedPost

# This is mostly tested manually to avoid modifying files at NERSC.

# TODO TEST add tests in test_manual
# TODO TEST add automated tests for stuff that doesn't contact nersc (arg checks etc.)
# TODO ERRORHANDLING wrap sfapi errors in server specific errors

# TODO CLEANUP clean up old code versions @NERSC somehow. Not particularly important

_COMMAND_PATH = "utilities/command"

_MIN_TIMEOUT_SEC = 300
_SEC_PER_GB = 2 * 60  # may want to make this configurable

_CTS_SCRATCH_ROOT_DIR = Path("cdm_task_service")

# TODO PROD add start and end time to task output and record
# TODO NERSCFEATURE if NERSC puts python 3.11 on the dtns revert to regular load 
_PYTHON_LOAD_HACK = "module use /global/common/software/nersc/pe/modulefiles/latest"
_PROCESS_DATA_XFER_MANIFEST_FILENAME = "process_data_transfer_manifest.sh"
_PROCESS_DATA_XFER_MANIFEST = f"""
#!/usr/bin/env bash

{_PYTHON_LOAD_HACK}
module load python

export PYTHONPATH=$CTS_CODE_LOCATION
export CTS_MANIFEST_LOCATION=$CTS_MANIFEST_LOCATION
export CTS_CALLBACK_URL=$CTS_CALLBACK_URL
export SCRATCH=$SCRATCH

echo "PYTHONPATH=[$PYTHONPATH]"
echo "CTS_MANIFEST_LOCATION=[$CTS_MANIFEST_LOCATION]"
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


class NERSCManager:
    """
    Manages interactions with the NERSC remote compute site.
    """
    
    @classmethod
    async def create(
        cls,
        client_provider: Awaitable[[], AsyncClient],
        nersc_code_path: Path,
    ) -> Self:
        """
        Create the NERSC manager.
        
        client_provider - a function that provides a valid SFAPI client. It is assumed that
            the user associated with the client does not change.
        nersc_code_path - the path in which to store remote code at NERSC. It is advised to
            include version information in the path to avoid code conflicts.
        """
        nm = NERSCManager(client_provider, nersc_code_path)
        await nm._setup_remote_code()
        return nm
        
    def __init__(
            self,
            client_provider: Awaitable[[], str],
            nersc_code_path: Path,
        ):
        self._client_provider = not_falsy(client_provider, "client_provider")
        self._nersc_code_path = self._check_path(nersc_code_path, "nersc_code_path")

    def _check_path(self, path: Path, name: str):
        not_falsy(path, name)
        # commands are ok with relative paths but file uploads are not
        if path.expanduser().absolute() != path:
            raise ValueError(f"{name} must be absolute to the NERSC root dir")
        return path

    async def _setup_remote_code(self):
        cli = self._client_provider()
        perlmutter = await cli.compute(Machine.perlmutter)
        dtns = await cli.compute(Machine.dtns)
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
                self._nersc_code_path / _PROCESS_DATA_XFER_MANIFEST_FILENAME,
                bio=io.BytesIO(_PROCESS_DATA_XFER_MANIFEST.encode()),
                make_exe=True,
            ))
            res = tg.create_task(dtns.run('bash -c "echo $SCRATCH"'))
            if _PIP_DEPENDENCIES:
                deps = " ".join(
                    # may need to do something else if module doesn't have __version__
                    [f"{mod.__name__}=={mod.__version__}" for mod in _PIP_DEPENDENCIES])
                command = (
                    'bash -c "'
                         + f"{_PYTHON_LOAD_HACK}; "
                         + f"module load python; "
                         # Unlikely, but this could cause problems if multiple versions
                         # of the server are running at once. Don't worry about it for now 
                         + f"pip install {deps}"  # adding notapackage causes a failure
                     + '"')
                tg.create_task(perlmutter.run(command))
        self._dtn_scratch = Path(res.result().strip())
    
    async def _run_command(self, client: AsyncClient, machine: Machine, exe: str):
        # TODO ERRORHANDlING deal with errors 
        return (await client.post(f"{_COMMAND_PATH}/{machine}", data={"executable": exe})).json()
    
    async def _upload_file_to_nersc(
        self,
        compute: AsyncCompute,
        target: Path,
        file: Path = None,
        bio: io.BytesIO = None,
        make_exe: bool = False,
    ):
        cmd = f'bash -c "mkdir -p {target.parent}"'
        await compute.run(cmd)
        # skip some API calls vs. the upload example in the NERSC docs
        # don't use a directory as the target or it makes an API call
        asrp = AsyncRemotePath(path=target, compute=compute)
        asrp.perms = "-"  # hack to prevent an unnecessary network call
        # TODO ERRORHANDLING throw custom errors
        if file:
            with open(file, "rb") as f:
                await asrp.upload(f)
        else:
            await asrp.upload(bio)
        if make_exe:
            cmd = f'bash -c "chmod u+x {target}"'
            await compute.run(cmd)

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
        callback_url - the URL to provide to NERSC as a callback for when the download is
            complete.
        concurrency - the number of simultaneous downloads to process.
        insecure_ssl - whether to skip the cert check for the S3 URL.
        
        Returns the NERSC task ID for the download.
        """
        maniio = self._create_download_manifest(
            job_id, objects, presigned_urls, concurrency, insecure_ssl)
        return await self._process_manifest(
            maniio, job_id, callback_url, "download_manifest.json", "download")
    
    async def upload_s3_files(
        self,
        job_id: str,
        remote_files: list[Path],
        presigned_urls: list[S3PresignedPost],
        callback_url: str,
        concurrency: int = 10,
        insecure_ssl: bool = False
    ) -> str:
        """
        Upload a set of files to an S3 instance from NERSC.
        
        job_id - the ID of the job for which the files are being transferred. 
            This must be a unique ID, and no other transfers should be occurring for the job.
        remote_files - the files to upload.
        presigned_urls - the presigned upload URLs for each file, in the same order as
            the file.
        callback_url - the URL to provide to NERSC as a callback for when the upload is
            complete.
        concurrency - the number of simultaneous uploads to process.
        insecure_ssl - whether to skip the cert check for the S3 URL.
        
        Returns the NERSC task ID for the upload.
        """
        maniio = self._create_upload_manifest(
            remote_files, presigned_urls, concurrency, insecure_ssl)
        return await self._process_manifest(
            maniio, job_id, callback_url, "upload_manifest.json", "upload")
    
    async def _process_manifest(
        self, manifest: io.BytesIO, job_id: str, callback_url: str, filename: str, task_type: str
    ):
        path = self._dtn_scratch / _CTS_SCRATCH_ROOT_DIR / job_id / filename
        cli = self._client_provider()
        dtn = await cli.compute(Machine.dtns)
        # TODO CLEANUP manifests after some period of time
        await self._upload_file_to_nersc(dtn, path, bio=manifest)
        command = (
            "bash -c '"
                + f"export CTS_CODE_LOCATION={self._nersc_code_path}; "
                + f"export CTS_MANIFEST_LOCATION={path}; "
                + f"export CTS_CALLBACK_URL={callback_url}; "
                + f"export SCRATCH=$SCRATCH; "
                + f'"$CTS_CODE_LOCATION"/{_PROCESS_DATA_XFER_MANIFEST_FILENAME}'
            + "'"
        )
        task_id  = (await self._run_command(cli, Machine.dtns, command))["task_id"]
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
        require_string(job_id, "job_id")
        not_falsy(objects, "objects")
        not_falsy(presigned_urls, "presigned_urls")
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
                "outputpath": str(self._dtn_scratch / 
                                  _CTS_SCRATCH_ROOT_DIR/ job_id / "files" / meta.path),
                "etag": meta.e_tag,
                "partsize": meta.effective_part_size,
                "size": meta.size,
            } for url, meta in zip(presigned_urls, objects)
        ]
        return io.BytesIO(json.dumps({"file-transfers": manifest}).encode())
    
    def _create_upload_manifest(
        self,
        remote_files: list[Path],
        presigned_urls: list[S3PresignedPost],
        concurrency: int,
        insecure_ssl: bool,
    ) -> io.BytesIO:
        not_falsy(remote_files, "remote_files")
        not_falsy(presigned_urls, "presigned_urls")
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
            "concurrency": check_int(concurrency, "concurrency"),
            "insecure-ssl": insecure_ssl,
            "min-timeout-sec": _MIN_TIMEOUT_SEC,
            "sec-per-GB": _SEC_PER_GB,
        }
