"""
DAO for MongoDB.
"""
# Could make an interface here so that mongo can be swapped out, but the chance that happens
# low enough to YAGNI the idea.

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import IndexModel, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice import models


_INDEX_TAG = "UNIQUE_IMAGE_TAG_INDEX"
_INDEX_DIGEST = "UNIQUE_IMAGE_DIGEST_INDEX"


class MongoDAO:
    """
    DAO for MongoDB.
    """
    
    @classmethod
    async def create(cls, db: AsyncIOMotorDatabase):
        """
        Create the DAO.
        
        db - the MongoDB database.
        """
        mdao = MongoDAO(db)
        await mdao._create_indexes()
        return mdao
    
    def __init__(self, db: AsyncIOMotorDatabase):
        if db is None:  # dbs don't work as a bool
            raise ValueError("db is required")
        self._db = db
        self._col_images = self._db.images
        self._col_jobs = self._db.jobs
        
    async def _create_indexes(self):
        # TODO DATAINTEGRITY test unique index builds throw an error if there are non-unique
        #                    docs in the DB. Want the server to not start until the indexes
        #                    are confirmed correct. See
        #                    https://www.mongodb.com/docs/manual/core/index-creation/#constraint-violations-during-index-build
        #                    Otherwise, create a polling loop to wait until indexes exist
        
        # NOTE that since there's two unique indexes means this collection can't be sharded
        # but it's likely to be very small so shouldn't be an issue
        await self._col_images.create_indexes([
            IndexModel(
                [(models.FLD_IMAGE_NAME, ASCENDING), (models.FLD_IMAGE_DIGEST, ASCENDING)],
                 unique=True,
                 name=_INDEX_DIGEST
            ),
            # Only allow one instance of a tag per image name to avoid confusion
            IndexModel(
                [(models.FLD_IMAGE_NAME, ASCENDING), (models.FLD_IMAGE_TAG, ASCENDING)],
                unique=True,
                sparse=True,  # tags are optional
                name=_INDEX_TAG
            ),
        ])
        # Only need and want a single unique index for jobs so they can be sharded
        await self._col_jobs.create_indexes([
            IndexModel([(models.FLD_JOB_ID, ASCENDING)], unique=True)
        ])

    async def save_image(self, image: models.Image):
        """
        Save details about a Docker image.
        
        Only one record per digest or tag is allowed per image
        """
        _not_falsy(image, "image")
        try:
            await self._col_images.insert_one(image.model_dump())
        except DuplicateKeyError as e:
            if _INDEX_TAG in e.args[0]:
                raise ImageTagExistsError(f"The tag {image.tag} for image {image.name} "
                                          + "already exists in the system")
            elif _INDEX_DIGEST in e.args[0]:
                raise ImageDigestExistsError(f"The digest {image.digest} for image {image.name} "
                                             + "already exists in the system")
            else:
                # no way to test this, but just in case
                raise ValueError(f"Unexpected duplicate key collision for image {image}") from e

    async def get_image(self, name: str, digest: str | None = None, tag: str | None = None
    ) -> models.Image:
        """
        Get an image via a name and tag or digest. Either a tag or digest is required. If both
        are provided, the tag is ignored.
        """
        name = _require_string(name, "name")
        digest = digest.strip() if digest else None
        tag = tag.strip() if tag else None
        query = {models.FLD_IMAGE_NAME: name}
        if digest:
            query[models.FLD_IMAGE_DIGEST] = digest
            err = f"with digest {digest}"
        elif tag:
            query[models.FLD_IMAGE_TAG] = tag
            err = f"with tag '{tag}'"
        else:
            raise ValueError("A digest or tag is required")
        doc = await self._col_images.find_one(query)
        if not doc:
            raise NoSuchImageError(f"No image {name} {err} exists in the system.")
        return models.Image.model_construct(**doc)

    async def save_job(self, job: models.Job):
        """ Save a job. Job IDs are expected to be unique."""
        _not_falsy(job, "job")
        jobd = job.model_dump()
        jobi = jobd[models.FLD_JOB_JOB_INPUT]
        jobi[models.FLD_JOB_INPUT_RUNTIME] = jobi[models.FLD_JOB_INPUT_RUNTIME].total_seconds()
        # don't bother checking for duplicate key exceptions since the service is supposed
        # to ensure unique IDs
        await self._col_jobs.insert_one(jobd)

    async def get_job(self, job_id: str):
        """ Get a job by its ID. """
        job_id = _require_string(job_id, "job_id")
        doc = await self._col_jobs.find_one({models.FLD_JOB_ID: job_id})
        if not doc:
            raise NoSuchJobError(f"No job with ID '{job_id}' exists")
        # TODO PERF build up the job piece by piece to skip S3 path validation
        return models.Job(**doc)


class NoSuchImageError(Exception):
    """ The image does not exist in the system. """


class NoSuchJobError(Exception):
    """ The job does not exist in the system. """


class ImageTagExistsError(Exception):
    """ The tag for the image already exists in the system. """


class ImageDigestExistsError(Exception):
    """ The digest for the image already exists in the system. """
