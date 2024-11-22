"""
DAO for MongoDB.
"""
# Could make an interface here so that mongo can be swapped out, but the chance that happens
# low enough to YAGNI the idea.

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo import IndexModel, ASCENDING, DESCENDING

_FLD_IMAGE_NAME = "name"
_FLD_IMAGE_HASH = "hash"
_FLD_IMAGE_TAG = "tag"
_FLD_IMAGE_ENTRYPOINT = "entrypoint"


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
        
    async def _create_indexes(self):
        # TODO DATAINTEGRITY test unique index builds throw an error if there are non-unique
        #                    docs in the DB. Want the server to not start until the indexes
        #                    are confirmed correct. See
        #                    https://www.mongodb.com/docs/manual/core/index-creation/#constraint-violations-during-index-build
        #                    Otherwise, create a polling loop to wait until indexes exist
        
        await self._col_images.create_indexes([
            IndexModel([(_FLD_IMAGE_NAME, ASCENDING), (_FLD_IMAGE_HASH, ASCENDING)], unique=True),
            # Only allow one instance of a tag per image name to avoid confusion
            IndexModel(
                [(_FLD_IMAGE_NAME, ASCENDING), (_FLD_IMAGE_TAG, ASCENDING)],
                unique=True,
                sparse=True  # tags are optional
            ),
        ])
