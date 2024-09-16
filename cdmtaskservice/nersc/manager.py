'''
Handler for data transfer between CDM sources and NERSC.
'''

import asyncio
from collections.abc import Awaitable
import io
import inspect
from pathlib import Path
from sfapi_client import AsyncClient
from sfapi_client.paths import AsyncRemotePath
from sfapi_client.compute import Machine, AsyncCompute
import sys
from types import ModuleType
from typing import Self

from cdmtaskservice.arg_checkers import not_falsy
from cdmtaskservice.nersc import remote

# This is mostly tested manually to avoid modifying files at NERSC.

# TODO TEST add tests in test_manual
# TODO ERRORHANDLING wrap sfapi errors in server specific errors

# TODO CLEANUP clean up old code versions @NERSC somehow. Not particularly important

# hard code the API version 
_URL_API = "https://api.nersc.gov/api/v1.2"

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
