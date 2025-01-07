"""
Methods for registering, deleting, and listing images.
"""

from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.image_remote_lookup import DockerImageInfo, parse_image_name
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.timestamp import utcdatetime


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
        
    async def register(self, imagename: str, username: str) -> models.Image:
        """
        Register an image to the service.
        
        imagename - the name of the docker image, e.g. gchr.io/kbase/checkm2:6.7.1
        username - the name of the user registering the image.
        """
        normedname = await self._iminfo.normalize_image_name(imagename)
        # Just use the sha for entrypoint lookup to ensure we get the right image
        entrypoint = await self._iminfo.get_entrypoint_from_name(normedname.name_with_digest)
        if not entrypoint:
            raise NoEntrypointError(f"Image {imagename} does not have an entrypoint")
        # TODO REFDATA allow specifying refdata for image
        img = models.Image(
            name=normedname.name,
            digest = normedname.digest,
            tag=normedname.tag,
            entrypoint=entrypoint,
            registered_by=username,
            registered_on=utcdatetime()
        )
        await self._mongo.save_image(img)
        return img
    
    async def get_image(self, imagename) -> models.Image:
        """
        Get an image.
        """
        parsedimage = parse_image_name(imagename)
        tag = parsedimage.tag
        if not parsedimage.tag and not parsedimage.digest:
            tag = "latest"
        return await self._mongo.get_image(parsedimage.name, digest=parsedimage.digest, tag=tag)


class NoEntrypointError(Exception):
    """ Thrown when an image does not have an entrypoint. """
