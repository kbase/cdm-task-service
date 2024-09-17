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
from cdmtaskservice.s3.client import S3ObjectMeta

# This is mostly tested manually to avoid modifying files at NERSC.

# TODO TEST add tests in test_manual
# TODO ERRORHANDLING wrap sfapi errors in server specific errors

# TODO CLEANUP clean up old code versions @NERSC somehow. Not particularly important

# hard code the API version 
_URL_API = "https://api.nersc.gov/api/v1.2"
_URL_API_BETA = "https://api.nersc.gov/api/beta"
_COMMAND_PATH = "utilities/command"

_MAX_CALLBACK_TIMEOUT = 3 * 24 * 3600

_MANIFEST_ROOT_DIR = Path("cdm_task_service")

# TODO PROD add start and end time to task output and record
# TODO TICKET ask NERSC for start & completion times for tasks
# TODO NERSCFEATURE if NERSC puts python 3.11 on the dtns revert to regular load 
_PYTHON_LOAD_HACK = "module use /global/common/software/nersc/pe/modulefiles/latest"
_PROCESS_DATA_XFER_MANIFEST_FILENAME = "process_data_transfer_manifest.sh"
_PROCESS_DATA_XFER_MANIFEST = f"""
#!/usr/bin/env bash

{_PYTHON_LOAD_HACK}
module load python

export PYTHONPATH=$CTS_CODE_LOCATION
export CTS_MANIFEST_LOCATION=$CTS_MANIFEST_LOCATION
export CTS_TOUCH_ON_COMPLETE=$CTS_TOUCH_ON_COMPLETE
export SCRATCH=$SCRATCH

echo "PYTHONPATH=[$PYTHONPATH]"
echo "CTS_MANIFEST_LOCATION=[$CTS_MANIFEST_LOCATION]"
echo "CTS_TOUCH_ON_COMPLETE=[$CTS_TOUCH_ON_COMPLETE]"
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

# TODO AUTH use authlib's AsyncOAuth2Client ensure_active_token to get tokens, otherwise
# there's a new token call every time the client is created


class NERSCManager:
    """
    Manages interactions with the NERSC remote compute site.
    """
    
    @classmethod
    async def create(
        cls,
        client_token_provider: Awaitable[[], str],
        nersc_code_path: Path,
        jaws_root_path: Path,
    ) -> Self:
        """
        Create the NERSC manager.
        
        client_token_provider - a function that provides a valid client token.
        nersc_code_path - the path in which to store remote code at NERSC. It is advised to
            include version information in the path to avoid code conflicts.
        jaws_root_path - the path under which input files should be stored such that they're
            available to JAWS.
        """
        nm = NERSCManager(client_token_provider, nersc_code_path, jaws_root_path)
        await nm._setup_remote_code()
        return nm
        
    def __init__(
            self,
            client_token_provider: Awaitable[[], str],
            nersc_code_path: Path,
            jaws_root_path: Path,
        ):
        self._cl_tkn_provider = not_falsy(client_token_provider, "client_token_provider")
        self._nersc_code_path = self._check_path(nersc_code_path, "nersc_code_path")
        self._jaws_root_path = self._check_path(jaws_root_path, "jaws_root_path")

    def _check_path(self, path: Path, name: str):
        not_falsy(path, name)
        # commands are ok with relative paths but file uploads are not
        if path.expanduser().absolute() != path:
            raise ValueError(f"{name} must be absolute to the NERSC root dir")
        return path

    async def _setup_remote_code(self):
        async with AsyncClient(
            api_base_url=_URL_API, access_token=await self._cl_tkn_provider()
        ) as cli:
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
                    self._nersc_code_path / _PROCESS_DATA_XFER_MANIFEST_FILENAME,
                    bio=io.BytesIO(_PROCESS_DATA_XFER_MANIFEST.encode()),
                    make_exe=True,
                ))
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
            cmd = f'bash -c "chmod a+x {target}"'
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
        path = Path("$SCRATCH") / _MANIFEST_ROOT_DIR / job_id / f"download_manifest.json"
        async with AsyncClient(
            api_base_url=_URL_API, access_token=await self._cl_tkn_provider()
        ) as cli:
            dtn = await cli.compute(Machine.dtns)
            # TODO CLEANUP manifests after some period of time
            await self._upload_file_to_nersc(dtn, path, bio=maniio)
            command = (
                "bash -c '"
                    + f"export CTS_CODE_LOCATION={self._nersc_code_path}; "
                    + f"export CTS_MANIFEST_LOCATION={path}; "
                    + f"export CTS_TOUCH_ON_COMPLETE={str(path) + '.complete'}; "
                    + f"export SCRATCH=$SCRATCH; "
                    + f'"$CTS_CODE_LOCATION"/{_PROCESS_DATA_XFER_MANIFEST_FILENAME}'
                + "'"
            )
            # TODO NERSCFEATURE send call back with command when NERSC supports
            task_id  = (await self._run_command(cli, Machine.dtns, command))["task_id"]
            # TODO LOGGING figure out how to handle logging, see other logging todos
            # log task ID in case the next step fails
            # Could maybe pass in a task ID receiver or something?
            logging.getLogger(__name__).info(f"Created upload task with id {task_id}")
        callback = {
            # TODO PROD what happens in a timeout? Nothing? the callback triggers?
            "timeout": _MAX_CALLBACK_TIMEOUT,
            "url": callback_url,
            "path_condition": {
                "path": str(path) + ".complete",
                "machine": Machine.dtns,
            }
        }
        # TODO NERSCFEATURE use callback specific methods when client supports and remove beta
        async with AsyncClient(
            api_base_url=_URL_API_BETA, access_token=await self._cl_tkn_provider()
        ) as cli:
            await cli.post("callback/", json=callback)
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
        manifest = {
            "file-transfers": {
                "op": "download",
                "concurrency": check_int(concurrency, "concurrency"),
                "insecure-ssl": insecure_ssl,
                "files": [
                    {
                        "url": url,
                        "outputpath": str(self._jaws_root_path / job_id / meta.path),
                        "etag": meta.e_tag,
                        "partsize": meta.effective_part_size,
                    } for url, meta in zip(presigned_urls, objects)
                ]
            }
        }
        return io.BytesIO(json.dumps(manifest).encode())
