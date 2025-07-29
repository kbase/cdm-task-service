"""
Manages running jobs at Lawrenium using the JAWS system.

Note that data is still staged at NERSC and the job is started from NERSC.
"""

from cdmtaskservice import models
from cdmtaskservice import sites
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import InvalidReferenceDataStateError
from cdmtaskservice.jaws import client as jaws_client
from cdmtaskservice.jobflows.nersc_jaws import NERSCJAWSRunner
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.nersc.manager import NERSCManager
from cdmtaskservice.s3.client import S3Client, S3ObjectMeta


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
        s3_client: S3Client,
        s3_external_client: S3Client,
        container_s3_log_dir: str,
        kafka: KafkaNotifier, 
        coro_manager: CoroutineWrangler,
        service_root_url: str,
        s3_insecure_ssl: bool = False,
    ):
        """
        Create the runner.
        
        nersc_manager - the NERSC manager.
        jaws_client - a JAWS Central client.
        mongodao - the Mongo DAO object.
        s3_client - an S3 client pointed to the data stores.
        s3_external_client - an S3 client pointing to an external URL for the S3 data stores
            that may not be accessible from the current process, but is accessible to remote
            processes at NERSC.
        container_s3_log_dir - where to store container logs in S3.
        kafka - a kafka notifier.
        coro_manager - a coroutine manager.
        service_root_url - the URL of the service root, used for constructing service callbacks.
        s3_insecure_url - whether to skip checking the SSL certificate for the S3 instance,
            leaving the service open to MITM attacks.
        """
        super().__init__(
            nersc_manager,
            jaws_client,
            mongodao,
            s3_client,
            s3_external_client,
            container_s3_log_dir,
            kafka,
            coro_manager,
            service_root_url,
            s3_insecure_ssl=s3_insecure_ssl
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
                raise InvalidReferenceDataStateError("Reference data must be in the created state")
            import logging  # TODO LRC REFDATA remove
            logging.getLogger(__name__).info(
                "waiting for JAWS to complete refdata handling stuff"
            )
            # Note on eventual retries - JAWS writes a file to note that the transfer is
            # done, whether it succeeded or failed. The file path is based on the file that the
            # s3 remote code writes when refdata is staged. Therefore on a retry, these files
            # will already exist and so setting up a SFAPI callback for them will trigger the
            # callback immediately. Either the files need to be deleted or a file with a new
            # filename needs to be written (maybe insert _attempt_# or something into the name).
            
            # BLOCKED waiting for JAWS features / debugging
            # TODO LRC REFDATA set up a callback @ NERSC for the refata complete file marker.that
            #        pings the service to tell it the refdata transfer to LRC is complete
            #        Update the refdata state
        except Exception as e:
            await self._handle_exception(e, refdata.id, "setting up callbacks for", refdata=True)

    async def refdata_complete(self, refdata_id: str):
        """
        Complete a refdata download task. The refdata is expected to be in the download
        submitted state for the cluster.
        """
        raise ValueError("unimplemented")
        # TODO LRC REFDATA implement.
        #       * check refdata state like the NERSC job flow
        #       * check the refdata completion file exists at NERSCD (e.g. don't trust the 
        #          http ping, verify)
        #       * update state

    async def clean_refdata(self, refdata: models.ReferenceData, force: bool = False):
        """
        Do nothing. Refdata is staged at NERSC; there is nothing to clean up at LRC.
        """
        pass # Intentionally do nothing 
