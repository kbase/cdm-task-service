"""
Methods for registering, deleting, and listing images.
"""

import re
from typing import Awaitable

from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.exceptions import IllegalParameterError
from cdmtaskservice.image_remote_lookup import DockerImageInfo, parse_image_name
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.refdata import Refdata
from cdmtaskservice.timestamp import utcdatetime


_ABSPATH_REGEX = re.compile(models.ABSOLUTE_PATH_REGEX)


class Images:
    """
    Registers, deletes, and lists images.
    """
    
    def __init__(self, mongo: MongoDAO, imageinfo: DockerImageInfo, refdata: Refdata):
        """
        Create the docker image manager.
        
        mongo - the Mongo DAO.
        imageinfo - an image information instance.
        """
        self._mongo = _not_falsy(mongo, "mongo")
        self._iminfo = _not_falsy(imageinfo, "imageinfo")
        self._ref = _not_falsy(refdata, "refdata")
        
    async def register(
        self,
        imagename: str,
        username: str,
        image_usage: models.ImageUsage = None,
        refdata_id: str = None,
        default_refdata_mount_point: str = None
    ) -> models.Image:
        """
        Register an image to the service.
        
        imagename - the name of the docker image, e.g. gchr.io/kbase/checkm2:6.7.1
        username - the name of the user registering the image.
        image_usage - usage information about the image.
        refdata_id - the ID of reference data to associate with the image.
        default_refdata_mount_point - the default mount point for refdata in an image container.
            Must be am absolute path with at least one path element.
        """
        if default_refdata_mount_point:
            if not refdata_id:
                raise IllegalParameterError(
                    "If a refdata mount point is provided a refdata ID must be provided."
                )
            if not _ABSPATH_REGEX.fullmatch(default_refdata_mount_point):
                raise IllegalParameterError(
                    "Refdata mount points must be absolute paths with at least one path element."
                )
        if refdata_id:  # ensure refdata exists
            await self._ref.get_refdata_by_id(refdata_id)
        normedname = await self._iminfo.normalize_image_name(imagename)
        # Just use the sha for entrypoint lookup to ensure we get the right image
        entrypoint = await self._iminfo.get_entrypoint_from_name(normedname.name_with_digest)
        if not entrypoint:
            raise NoEntrypointError(f"Image {imagename} does not have an entrypoint")
        image_usage = image_usage or models.ImageUsage()
        img = models.Image(
            name=normedname.name,
            digest = normedname.digest,
            tag=normedname.tag,
            entrypoint=entrypoint,
            registered_by=username,
            registered_on=utcdatetime(),
            refdata_id=refdata_id or None,  # ensure not empty string, etc.
            default_refdata_mount_point=default_refdata_mount_point,
            usage_notes=image_usage.usage_notes,
            urls=image_usage.urls,
        )
        await self._mongo.save_image(img)
        return img
    
    async def get_image(self, imagename: str) -> models.Image:
        """
        Get an image.
        """
        return await self._process_image(imagename, self._mongo.get_image)
    
    async def get_images(self) -> list[models.Image]:
        """
        Get images in the service in no particluar order. Returns at most 1000 images.
        """
        # Pass through method
        return await self._mongo.get_images()

    async def delete_image(self, imagename: str):
        """
        Delete an image.
        """
        await self._process_image(imagename, self._mongo.delete_image)
    
    async def _process_image(self, imagename: str, fn: Awaitable):
        parsedimage = parse_image_name(imagename)
        tag = parsedimage.tag
        if not parsedimage.tag and not parsedimage.digest:
            tag = "latest"
        return await fn(parsedimage.name, digest=parsedimage.digest, tag=tag)


class NoEntrypointError(Exception):
    """ Thrown when an image does not have an entrypoint. """
