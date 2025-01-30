"""
Manages refdata at remote sites.
"""

import uuid

from cdmtaskservice import kb_auth
from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import ETagMismatchError
from cdmtaskservice.jobflows.flowmanager import JobFlowManager
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths, validate_path
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
        self,
        s3_path: str,
        user: kb_auth.KBaseUser,
        etag: str = None,
        unpack: bool = False
    ) -> models.ReferenceData:
        """
        Start the refdata creation process.
        
        s3_path - the path to the refdata file in S3.
        user - the user submitting the request.
        etag - the etag of the refdata file. If provided and non-matching, an error is thrown.
        unpack - whether to unpack the reference data file at the compute location. Supports
            *.gz, *.tar.gz, and *.tgz files.
        
        Returns the refdata state.
        """
        # validate before S3Paths beacuse that returns an error mentioning an index
        # TODO CODE add a toggle to S3Paths to not include index info in error
        validate_path(s3_path)
        _not_falsy(user, "user")
        meta = (await self._s3.get_object_meta(S3Paths([s3_path])))[0]
        if etag and etag != meta.e_tag:
            raise ETagMismatchError(
                f"The expected ETag '{etag} for the path "
                + f"'{s3_path}' does not match the actual ETag '{meta.e_tag}'"
            )
        statuses = []
        clusters = self._flowman.list_clusters()
        for c in  clusters:
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
            file=s3_path,
            etag=meta.e_tag,
            unpack=unpack,
            registered_by=user.user,
            registered_on=utcdatetime(),
            statuses=statuses,
        )
        await self._mongo.save_refdata(rd)
        for c in clusters:
            # theoretically a race condition here but not much we can do about it
            flow = self._flowman.get_flow(c)
            # Pass in the meta to avoid potential race conditions w/ etag changes
            await self._coman.run_coroutine(flow.stage_refdata(rd, meta))
        return rd

    async def get_refdata(
        self,
        refdata_id: str,
        as_admin: bool = False,
    ) -> models.ReferenceData | models.AdminReferenceData:
        """
        Get reference data based on its ID.
        
        refdata_id - the reference data ID
        as_admin - True if the user should be able to access additional details about the data.
        """
        return await self._mongo.get_refdata(
            _require_string(refdata_id, "refdata_id"), as_admin=as_admin
        )
