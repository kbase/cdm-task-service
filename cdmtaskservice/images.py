"""
Methods for registering, deleting, and listing images.
"""

from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy
from cdmtaskservice.image_remote_lookup import DockerImageInfo


class Images:
    """
    Registers, deletes, and lists images.
    """
    
    def __init__(self, imageinfo: DockerImageInfo):
        """
        Create the docker image manager.
        
        imageinfo = an image information instance.
        """
        self._iminfo = _not_falsy(imageinfo, "imageinfo")
        
    async def register(self, imagename: str):
        # TODO PERF use a task group to get the name and entrypoint simultaneously
        normedname = await self._iminfo.normalize_image_name(imagename)
        od = normedname.olddigest
        if od and od != normedname.normedname.split("@")[-1]:
            # Not sure how this could happen, unless it's a sha256 vs 512 or something?
            # leave it just to be safe
            raise ValueError("somehow the provided digest doesn't match with the returned digest")
        entrypoint = await self._iminfo.get_entrypoint_from_name(normedname.normedname)
        if not entrypoint:
            raise NoEntrypointError(f"Image {imagename} does not have an entrypoint")
        # TODO IMAGEREG save image to mongo
        #       need unique index on image name + tag for lookups and uniqueness
        #       need unique index on image name + sha for the same
        # TODO DATAINTEG add username and date created
        # TODO REFDATA allow specifying refdata for image
        return models.Image(
            normed_name=normedname.normedname, tag=normedname.tag, entrypoint=entrypoint
        )


class NoEntrypointError(Exception):
    """ Thrown when an image does not have an entrypoint. """
