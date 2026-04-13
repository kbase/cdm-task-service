"""
Methods for registering, deleting, and listing images.
"""

import re
from typing import Awaitable, NamedTuple

from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.exceptions import IllegalParameterError
from cdmtaskservice.image_remote_lookup import DockerImageInfo, parse_image_name
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.refdata import Refdata
from cdmtaskservice.timestamp import utcdatetime


_ABSPATH_REGEX = re.compile(models.ABSOLUTE_PATH_REGEX)


class _ImageFields(NamedTuple):
    urls: list | None
    usage_notes: str | None
    refdata_id: str | None
    default_refdata_mount_point: str | None


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
        image_reg: models.ImageRegistration = None,
        refdata_id: str = None,
        default_refdata_mount_point: str = None
    ) -> models.Image:
        """
        Register an image to the service.

        imagename - the name of the docker image, e.g. gchr.io/kbase/checkm2:6.7.1
        username - the name of the user registering the image.
        image_reg - the image registration request, optionally including usage info
            and a from_image reference to an existing registered image from which to copy
            user-supplied fields.
        refdata_id - the ID of reference data to associate with the image.
        default_refdata_mount_point - the default mount point for refdata in an image container.
            Must be an absolute path with at least one path element.
        """
        image_reg = image_reg or models.ImageRegistration()
        fields = await self._resolve_image_fields(image_reg, refdata_id, default_refdata_mount_point)
        if fields.default_refdata_mount_point:
            if not fields.refdata_id:
                raise IllegalParameterError(
                    "If a refdata mount point is provided a refdata ID must be provided."
                )
            if not _ABSPATH_REGEX.fullmatch(fields.default_refdata_mount_point):
                raise IllegalParameterError(
                    "Refdata mount points must be absolute paths with at least one path element."
                )
        if fields.refdata_id:  # ensure refdata exists
            await self._ref.get_refdata_by_id(fields.refdata_id)
        normedname = await self._iminfo.normalize_image_name(imagename)
        # Just use the sha for entrypoint lookup to ensure we get the right image
        entrypoint = await self._iminfo.get_entrypoint_from_name(normedname.name_with_digest)
        if not entrypoint:
            raise NoEntrypointError(f"Image {imagename} does not have an entrypoint")
        img = models.Image(
            name=normedname.name,
            digest=normedname.digest,
            tag=normedname.tag,
            entrypoint=entrypoint,
            registered_by=username,
            registered_on=utcdatetime(),
            refdata_id=fields.refdata_id,
            default_refdata_mount_point=fields.default_refdata_mount_point,
            usage_notes=fields.usage_notes,
            urls=fields.urls,
        )
        await self._mongo.save_image(img)
        return img

    async def _resolve_image_fields(
        self,
        image_reg: models.ImageRegistration,
        refdata_id: str,
        default_refdata_mount_point: str,
    ) -> _ImageFields:
        """
        Resolve the final user-supplied field values for image registration from the two
        potential sources: the current request and the from_image reference. Values in the
        current request take precedence.
        """
        urls = image_reg.urls
        usage_notes = image_reg.usage_notes
        if image_reg.from_image:
            from_img = await self._process_image(image_reg.from_image, self._mongo.get_image)
            if urls is None:
                urls = from_img.urls
            if usage_notes is None:
                usage_notes = from_img.usage_notes
            if refdata_id is None:
                refdata_id = from_img.refdata_id
            if default_refdata_mount_point is None:
                default_refdata_mount_point = from_img.default_refdata_mount_point
        return _ImageFields(
            urls=urls,
            usage_notes=usage_notes,
            refdata_id=refdata_id or None,  # ensure not empty string, etc.
            default_refdata_mount_point=default_refdata_mount_point,
        )
    
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
