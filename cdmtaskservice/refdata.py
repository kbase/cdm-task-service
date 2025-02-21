"""
Manages refdata at remote sites.
"""

import uuid

from cdmtaskservice import kb_auth
from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.coroutine_manager import CoroutineWrangler
from cdmtaskservice.exceptions import ChecksumMismatchError, IllegalParameterError
from cdmtaskservice.jobflows.flowmanager import JobFlowManager
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths, validate_path
from cdmtaskservice.s3.remote import UNPACK_FILE_EXTENSIONS
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
        crc64nvme: str = None,
        unpack: bool = False
    ) -> models.ReferenceData:
        """
        Start the refdata creation process.
        
        s3_path - the path to the refdata file in S3.
        user - the user submitting the request.
        crc64nvme - the base64 encoded CRC64/NVME checksum of the refdata file.
            If provided and non-matching, an error is thrown.
        unpack - whether to unpack the reference data file at the compute location. Supports
            *.gz, *.tar.gz, and *.tgz files.
        
        Returns the refdata state.
        """
        # validate before S3Paths because that returns an error mentioning an index
        # TODO CODE add a toggle to S3Paths to not include index info in error
        validate_path(s3_path)
        _not_falsy(user, "user")
        if unpack:
            if not any([s3_path.endswith(ex) for ex in UNPACK_FILE_EXTENSIONS]):
                raise IllegalParameterError(
                    "Invalid file extension for unpack, only "
                    + f"{', '.join(UNPACK_FILE_EXTENSIONS)} are supported")
        meta = (await self._s3.get_object_meta(S3Paths([s3_path])))[0]
        if not meta.crc64nvme:
            raise IllegalParameterError(
                f"The S3 path '{s3_path}' does not have a CRC64/NVME checksum"
            )
        if crc64nvme and crc64nvme != meta.crc64nvme:
            raise ChecksumMismatchError(
                f"The expected CRC64/NMVE checksum '{crc64nvme}' for the path "
                + f"'{s3_path}' does not match the actual checksum '{meta.crc64nvme}'"
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
            crc64nvme=meta.crc64nvme,
            etag=meta.e_tag,  # TODO CHECKSUM remove
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

    async def get_refdata(self) -> list[models.ReferenceData]:
        """
        Get reference data in the system in no particular order. Returns at most 1000 records.
        """
        # pass through method, don't want the routes talking directly to mongo
        return await self._mongo.get_refdata()

    async def get_refdata_by_id(
        self,
        refdata_id: str,
        as_admin: bool = False,
    ) -> models.ReferenceData | models.AdminReferenceData:
        """
        Get reference data based on its ID.
        
        refdata_id - the reference data ID
        as_admin - True if the user should be able to access additional details about the data.
        """
        return await self._mongo.get_refdata_by_id(
            _require_string(refdata_id, "refdata_id"), as_admin=as_admin
        )
        
    async def get_refdata_by_path(self, s3_path: str) -> list[models.ReferenceData]:
        """
        Get reference data based on the S3 file path. Returns at most 1000 records.
        """
        # pass through method, don't want the routes talking directly to mongo
        return await self._mongo.get_refdata_by_path(_require_string(s3_path, "s3_path"))
