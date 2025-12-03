""" Manager for staging reference data based on CTS records. """

import logging

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import InvalidReferenceDataStateError
from cdmtaskservice import models
from cdmtaskservice.refserv.cts_client import CTSRefdataClient
from cdmtaskservice import sites
from cdmtaskservice.s3.client import S3Client


class RefdataManager:
    """ Manages CTS refdata locally. """
    
    def __init__(self, ctsrefcli: CTSRefdataClient, s3cli: S3Client, coman: CoroutineWrangler):
        """ Create the manager. """
        self._cli = _not_falsy(ctsrefcli, "ctsrefcli")
        self._s3cli = _not_falsy(s3cli, "s3cli")
        self._coman = _not_falsy(coman, "coman")
        self._logr = logging.getLogger(__name__)

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
        # TODO NEXT switch state to DOWNLOAD_SUBMITTED
        await self._coman.run_coroutine(self._stage_refdata(refdata, cluster))
    
    async def _stage_refdata(self, refdata: models.ReferenceData, cluster: sites.Cluster):
        try:
            self._logr.info(f"Running staging coroutine on {refdata}, {cluster}")  # TODO REFDATA remove
        except Exception as e:
            self._logr.exception(
                f"Failed to stage refdata {refdata.id} for cluster {cluster.value}"
            )
            # TODO REFDATASERV set refdata to error in CTS if possible
            raise  # TODO REFDATASERV remove
