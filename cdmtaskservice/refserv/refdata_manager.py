""" Manager for staging reference data based on CTS records. """

import asyncio
from concurrent.futures.process import ProcessPoolExecutor
import json
import hashlib
import logging
from pathlib import Path
import traceback

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import InvalidReferenceDataStateError
from cdmtaskservice import models
from cdmtaskservice.refserv.cts_client import CTSRefdataClient
from cdmtaskservice import sites
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.s3.remote import unpack_archive


_MD5_FILE = "md5.json"
_MD5_FILE_TMP = _MD5_FILE + ".tmp"


class RefdataManager:
    """ Manages CTS refdata locally. """
    
    def __init__(
        self,
        ctsrefcli: CTSRefdataClient,
        s3cli: S3Client,
        coman: CoroutineWrangler,
        refdata_path: Path,
        refdata_meta_path: Path,
        
    ):
        """
        Create the manager.
        
        ctsrefcli - the CTS reference data client.
        s3cli - the S3 client. Must be able to read all locations where CTS refdata is stored.
        coman - a coroutine manager.
        refdata_path - where refdata will be stored locally.
        refdata_meta_path - where refdata metadata will be stored locally.
        """
        self._cli = _not_falsy(ctsrefcli, "ctsrefcli")
        self._s3cli = _not_falsy(s3cli, "s3cli")
        self._coman = _not_falsy(coman, "coman")
        self._refpath = _not_falsy(refdata_path, "refdata_path")
        self._metapath = _not_falsy(refdata_meta_path, "refdata_meta_path")
        self._logr = logging.getLogger(__name__)
        # TODO CODE update to an InterpreterPoolExecutor when upgrading to Python 3.14
        #           is possible. Currently some dependencies aren't compatible.
        # 10 workers seems like plenty, refdata staging should be rare.
        self._exe = ProcessPoolExecutor(max_workers=10)

    def close(self):
        """
        Close any resources associated with the manager.
        """
        self._exe.shutdown(cancel_futures=True)

    async def stage_refdata(self, refdata_id: str, cluster: sites.Cluster):
        """
        Stage refdata from S3 to the local repository. Starts the staging and returns immediately
        
        refdata_id - the ID of the refdata to stage.
        cluster - the cluster where the refdata is being staged.
        """
        _require_string(refdata_id, "refdata_id")
        _not_falsy(cluster, "cluster")
        # check refdata exists, update state and throw errors back to CTS
        # before starting a coroutine and returning
        # If there's another process trying to do the same thing then one of them will cause
        # the staging to error out in the CTS, but that should never happen
        refdata = await self._cli.get_refdata(refdata_id)
        refstate = refdata.get_status_for_cluster(cluster)
        if refstate.state != models.ReferenceDataState.CREATED:
            raise InvalidReferenceDataStateError(
                f"Reference data must be in the created state for cluster {refstate.cluster.value}"
            )
        await self._cli.update_refdata_state(
            refdata.id, cluster, models.ReferenceDataState.DOWNLOAD_SUBMITTED
        )
        # Now this server coroutine has claimed the staging process, any other requests to
        # the server will fail for this refdata ID so we're safe to proceed and send errors
        # directly to the CTS vs. the caller, whoever that might be (although it should
        # only be the CTS under normal circumstances)
        await self._coman.run_coroutine(self._stage_refdata(refdata, cluster))
    
    async def _stage_refdata(self, refdata: models.ReferenceData, cluster: sites.Cluster):
        try:
            arcpath = self._refpath / refdata.id / Path(refdata.file).name
            arcpath.parent.mkdir(parents=True, exist_ok=True)
            self._logr.info(
                f"Downloading refdata {refdata.id} from S3 {refdata.file} to {arcpath}"
            )
            await self._s3cli.download_objects_to_file(S3Paths([refdata.file]), [arcpath])
            await asyncio.get_running_loop().run_in_executor(
                self._exe, _unpack_refdata, refdata.id, arcpath, refdata.unpack, self._metapath
            )
            await self._cli.update_refdata_state(
                refdata.id, cluster, models.ReferenceDataState.COMPLETE
            )
        except Exception as e:
            self._logr.exception(
                f"Failed to stage refdata {refdata.id} for cluster {cluster.value}"
            )
            # if this fails there's not much else we can do
            await self._cli.update_refdata_state(
                refdata.id,
                cluster,
                models.ReferenceDataState.ERROR,
                admin_error=str(e),
                traceback=traceback.format_exc()
            )


# Expected to be run in a new process or interpreter
# Don't block the event loop with all this sync stuff 
def _unpack_refdata(refdata_id: str, arcpath: Path, unpack: bool, metadata_dir: Path):
    logging.basicConfig(level=logging.INFO)
    logr = logging.getLogger(__name__)
    if unpack:
        logr.info(
            f"Unpacking refdata {refdata_id} archive {arcpath}"
        )
        unpack_archive(arcpath)
    refparent = arcpath.parent
    logr.info(f"Generating md5s for refdata {refdata_id}")
    files = [file for file in refparent.rglob('*') if file.is_file()]
    file_md5s = []
    for f in files:
        with open(f, "rb") as fo:
            file_md5s.append({
                "file": str(f.relative_to(refparent)),
                "md5": hashlib.file_digest(fo, "md5").hexdigest(),
                "size": f.stat().st_size
            })
    tmpfile = metadata_dir / refdata_id / _MD5_FILE_TMP
    tmpfile.parent.mkdir(parents=True, exist_ok=True)
    with open(tmpfile, "w") as f:
        f.write(json.dumps({"file_md5s": file_md5s}, indent=4))
    # Ensure no reading of partially written files
    finalfile = metadata_dir / refdata_id / _MD5_FILE
    tmpfile.rename(finalfile)
    logr.info(f"Wrote md5s for refdata {refdata_id} to {finalfile}")
