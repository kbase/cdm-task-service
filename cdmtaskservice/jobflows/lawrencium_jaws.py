"""
Manages running jobs at Lawrenium using the JAWS system.

Note that data is still staged at NERSC and the job is started from NERSC.
"""

from cdmtaskservice import models
from cdmtaskservice import sites
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.callback_url_paths import get_refdata_download_complete_callback
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import InvalidReferenceDataStateError
from cdmtaskservice.jaws import client as jaws_client
from cdmtaskservice.jobflows.nersc_jaws import NERSCJAWSRunner
from cdmtaskservice.jobflows.s3config import S3Config
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.nersc.manager import NERSCManager
from cdmtaskservice.s3.client import S3ObjectMeta
from cdmtaskservice.update_state import submitted_refdata_download, refdata_complete


class LawrenciumJAWSRunner(NERSCJAWSRunner):
    """
    Runs jobs at Lawrencium using JAWS.
    """
    
    CLUSTER = sites.Cluster.LAWRENCIUM_JAWS
    """ The cluster on which this runner operates. """
    
    def __init__(
        self,
        nersc_manager: NERSCManager,
        jaws_client: jaws_client.JAWSClient,
        mongodao: MongoDAO,
        s3config: S3Config,
        kafka: KafkaNotifier, 
        coro_manager: CoroutineWrangler,
        service_root_url: str,
    ):
        """
        Create the runner.
        
        nersc_manager - the NERSC manager.
        jaws_client - a JAWS Central client.
        mongodao - the Mongo DAO object.
        s3config - the S3 configuration.
        kafka - a kafka notifier.
        coro_manager - a coroutine manager.
        service_root_url - the URL of the service root, used for constructing service callbacks.
        """
        super().__init__(
            nersc_manager,
            jaws_client,
            mongodao,
            s3config,
            kafka,
            coro_manager,
            service_root_url,
        )
    
    async def stage_refdata(self, refdata: models.ReferenceData, objmeta: S3ObjectMeta):
        """
        Do nothing. This method is overidden as LRC refdata staging depends on the completion
        of NERSC refdata staging first.
        """
        pass  # intentionally do nothing

    async def nersc_refdata_complete(self, refdata: models.ReferenceData):
        """
        Notify the flow runner that refdata has been staged at NERSC and transfer to
        LRC has been requested.
        
        Any exceptions cause the refdata state to be set to error for LRC, as it is expected
        only internal methods call this method, not API methods.
        """
        try:
            refstate = _not_falsy(refdata, "refdata").get_status_for_cluster(self.CLUSTER)
            if refstate.state != models.ReferenceDataState.CREATED:
                raise InvalidReferenceDataStateError(
                    f"Reference data must be in the created state for cluster {self.CLUSTER}"
                )
            callback_url = get_refdata_download_complete_callback(
                self._callback_root, refdata.id, self.CLUSTER
            )
            await self._nman.setup_refdata_transfer_callback(refdata, self.CLUSTER, callback_url)
            await self._update_refdata_state(refdata.id, submitted_refdata_download())
            # Note on eventual retries - JAWS writes a file to note that the transfer is
            # done, whether it succeeded or failed. The file path is based on the file that the
            # s3 remote code writes when refdata is staged. Therefore on a retry, these files
            # will already exist and so setting up a SFAPI callback for them will trigger the
            # callback immediately. Either the files need to be deleted or a file with a new
            # filename needs to be written (maybe insert _attempt_# or something into the name).
        except Exception as e:
            await self._handle_exception(e, refdata.id, "setting up callbacks for", refdata=True)

    async def refdata_complete(self, refdata_id: str):
        """
        Complete a refdata download task. The refdata is expected to be in the download
        submitted state for the cluster.
        """
        refdata = await self._mongo.get_refdata_by_id(_require_string(refdata_id, "refdata_id"))
        refstate = refdata.get_status_for_cluster(self.CLUSTER)
        if refstate.state != models.ReferenceDataState.DOWNLOAD_SUBMITTED:
            raise InvalidReferenceDataStateError(
                "Reference data must be in the download submitted state for "
                + f"cluster {refstate.cluster.value}"
            )
        async def tfunc():
            return await self._nman.get_refdata_transfer_result(refdata, self.CLUSTER), None
        await self._get_transfer_result(  # check for errors
            tfunc, refdata.id, "Transfer", "transferring", refdata=True
        )
        await self._update_refdata_state(refdata.id, refdata_complete())

    async def clean_refdata(self, refdata: models.ReferenceData, force: bool = False):
        """
        Do nothing. Refdata is staged at NERSC; there is nothing to clean up at LRC.
        """
        pass # Intentionally do nothing 
