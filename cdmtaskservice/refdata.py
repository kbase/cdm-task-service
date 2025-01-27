"""
Manages refdata at remote sites.
"""

import uuid

from cdmtaskservice import kb_auth
from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import ETagMismatchError
from cdmtaskservice.jobflows.flowmanager import JobFlowManager
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.timestamp import utcdatetime


class Refdata:
    """
    A manager for reference data.
    """
    
    def __init__(
        self,
        mongo: MongoDAO,
        s3client: S3Client,
        coro_manager: CoroutineWrangler,
        flow_manager: JobFlowManager,
    ):
        """ 
        Create the refdata manager.
        
        mongo - a MongoDB DAO object.
        s3Client - an S3Client pointed at the S3 storage system to use.
        coro_manager - a coroutine manager.
        flow_manager- the job flow manager.
        """
        self._mongo = _not_falsy(mongo, "mongo")
        self._s3 = _not_falsy(s3client, "s3client")
        self._coman = _not_falsy(coro_manager, "coro_manager")
        self._flowman = _not_falsy(flow_manager, "flow_manager")

    async def create_refdata(
        self, refdata_input: models.ReferenceDataInput, user: kb_auth.KBaseUser
    ) -> models.ReferenceData:
        """
        Start the refdata creation process.
        
        refdata_input - the input data needed to create the refdata.
        user - the username of the user submitting the request.
        
        Returns the refdata state.
        """
        _not_falsy(refdata_input, "refdata_input")
        _not_falsy(user, "user")
        # TODO PERF this checks the file path syntax again, consider some way to avoid
        meta = (await self._s3.get_object_meta(S3Paths([refdata_input.file])))[0]
        if refdata_input.etag and refdata_input.etag != meta.e_tag:
            raise ETagMismatchError(
                f"The expected ETag '{refdata_input.etag} for the path "
                + f"'{refdata_input.file}' does not match the actual ETag '{meta.e_tag}'"
            )
        statuses = []
        for c in self._flowman.list_clusters():
            # TODO LAWRENCIUM REFDATA will need to do something special here since refdata is
            #                         xferred from NERSC
            statuses.append(models.ReferenceDataStatus(
                cluster=c,
                state=models.ReferenceDataState.CREATED,
                transition_times=[(models.ReferenceDataState.CREATED, utcdatetime())]
            ))
        if not statuses:
            # TODO REFDATA add a way to restart refdata staging and just save to mongo
            #              to be restarted later
            raise ValueError("No job flows are available")
        rd = models.ReferenceData(
            id=str(uuid.uuid4()),  # TODO TEST for testing we'll need to set up a mock for this
            file=refdata_input.file,
            etag=meta.e_tag,
            unpack=refdata_input.unpack,
            registered_by=user.user,
            registered_on=utcdatetime(),
            statuses=statuses,
        )
        # TODO REFDATA save to mongo
        # TODO REFDATA start a coroutine to stage the data
        return rd
