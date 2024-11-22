"""
Methods for registering, deleting, and listing images.
"""

from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.image_remote_lookup import DockerImageInfo
from cdmtaskservice.mongo import MongoDAO


class Images:
    """
    Registers, deletes, and lists images.
    """
    
    def __init__(self, mongo: MongoDAO, imageinfo: DockerImageInfo):
        """
        Create the docker image manager.
        
        mongo - the Mongo DAO.
        imageinfo - an image information instance.
        """
        self._mongo = _not_falsy(mongo, "mongo")
        self._iminfo = _not_falsy(imageinfo, "imageinfo")
        
    async def register(self, imagename: str):
        normedname = await self._iminfo.normalize_image_name(imagename)
        # Just use the sha for entrypoint lookup to ensure we get the right image
        entrypoint = await self._iminfo.get_entrypoint_from_name(normedname.name_with_digest)
        if not entrypoint:
            raise NoEntrypointError(f"Image {imagename} does not have an entrypoint")
        # TODO IMAGEREG save image to mongo
        #       need unique index on image name + tag for lookups and uniqueness
        #       need unique index on image name + sha for the same
        # TODO DATAINTEG add username and date created
        # TODO REFDATA allow specifying refdata for image
        return models.Image(
            name=normedname.name,
            digest = normedname.digest,
            tag=normedname.tag,
            entrypoint=entrypoint
        )


class NoEntrypointError(Exception):
    """ Thrown when an image does not have an entrypoint. """
