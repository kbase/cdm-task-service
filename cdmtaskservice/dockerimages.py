"""
Contains code for querying container information without access to a docker server
"""

# Trying to find a simple way to handle this was surprisingly frustrating. I spent a half
# day or so looking at alternatives and the crane CLI seems like the least bad option. I tried
# a number of things, most of which I don't remember, including http requests (had to be tailored
# to each repository somewhat) and https://github.com/davedoesdev/dxf (couldn't get anonymous
# access to work). If there's a python library that can take crane's place that'd be great but
# I didn't find it.

import asyncio
from collections import namedtuple
import json
import logging
import os
from pathlib import Path
import re
from typing import Self

from docker_image import reference


_DISALLOWED_CHARS=re.compile(r"([^a-zA-Z0-9@:_.\-\/])")


NormedImageName = namedtuple(
    "NormedImageName",
    ("normedname", "tag", "olddigest"),
    defaults=(None, None)
)


class DockerImageInfo:
    """
    Provides information about a remote docker image
    """
    
    @classmethod
    async def create(cls, crane_absolute_path: Path) -> Self:
        """
        Create the image info provider.
        
        crane_absolute_path - the absolute path to the crane executable.
        """
        dii = DockerImageInfo(crane_absolute_path)
        retcode, _, stde = await dii._run_crane("version")
        if retcode > 0:
            raise ValueError(
                f"crane executable version call failed, retcode: {retcode} stderr:\n{stde}")
        return dii
    
    def __init__(self, crane_absolute_path: Path):
        if not crane_absolute_path:
            raise CranePathError("crane_absolute_path cannot be None")
        crane = Path(os.path.normpath(crane_absolute_path))
        if not crane.is_absolute():
            # Require an absolute path to avoid various malicious attacks
            raise CranePathError("crane_absolute_path must be absolute")
        self._crane = crane_absolute_path

    async def normalize_image_name(self, image_name: str) -> NormedImageName:
        """
        Given an image name, returns a normalized version of the name in the form
        `host/namespace/path@sha`. This form always refers to the same image and is compatible
        with Docker, Shifter, and Apptainer. 
        
        If the input image name contains a tag, it is provided in the returned tuple. Note that
        the tag may or may not refer to the image referenced by the SHA in the future.
        
        The SHA is sourced from the remote repository digest; if a sha is provided in the input
        image name it is included in the returned tuple.
        """
        ref = _parse_image_name(image_name)
        digest = await self._run_crane_command(_assemble_name(ref), "digest")
        return NormedImageName(
            normedname=f'{ref["name"]}@{digest}',
            tag=ref["tag"],
            olddigest=ref["digest"])
    
    async def _run_crane_command(self, image_name: str, command: str) -> str:
        retcode, stdo, stde = await self._run_crane(command, image_name)
        if retcode > 0:
            # TODO LOGGING figure out how this is going to work, want to associate logs with
            #              user names, ips, etc.
            # TODO TEST logging add tests for logging before exiting prototype stage
            #           manual testing for now
            logging.getLogger(__name__).error(
                f"crane lookup of image {image_name} failed, retcode: {retcode}, stderr:\n{stde}")
            stdel = stde.lower()
            # special cases when crane spits out html and the regular error parsing doesn't work
            if "unauthorized" in stdel:  # config command doesn't include 401
                raise ImageInfoFetchError(
                    f"Failed to access information for image {image_name}. "
                    + "Unauthorized to access image")
            if "404 not found" in stdel:
                raise ImageInfoFetchError(
                    f"Failed to access information for image {image_name}. "
                    + "Image was not found on the host")
            # This is pretty fragile for non-docker repository hosts.
            # Not really sure what else to do here
            # Allowlist for hosts?
            errcode = stde.split("\n")[-1].split(':')[-1].strip()
            raise ImageInfoFetchError(
                f"Failed to access information for image {image_name}. Error code was: {errcode}")
        return stdo

    async def get_entrypoint_from_name(self, image_name: str) -> list[str] | None:
        """
        Get the image entrypoint given the image name.
        
        Assumes that a namespace is always present (so names like `image:tag` will be rejected)
        and the path always has 2 components, the namespace and the image name.
        
        Returns None if there is no entrypoint.
        """
        ret = await self._run_crane_command(
            _assemble_name(_parse_image_name(image_name)),
           "config")
        try:
            cfg = json.loads(ret)
        except json.JSONDecodeError as e:
            # Can't think of a good way to test this
            raise ImageInfoFetchError(
                f"Unable to parse response for remote image {image_name} into JSON: {e}") from e
        return cfg["config"].get("Entrypoint")

    async def _run_crane(self, *args) -> (int, str, str):
        # crane has very small output so just buffer in memory
        try:
            proc = await asyncio.create_subprocess_exec(
                self._crane, *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
        except FileNotFoundError as e:
            raise CranePathError(
                f"Configured crane executable path {self._crane} is invalid") from e
        stdo, stde = await proc.communicate()
        return proc.returncode, stdo.decode().strip(), stde.decode().strip()


def _parse_image_name(image_name: str) -> reference.Reference:
    image_name = image_name.strip() if image_name is not None else None
    # Image name rules: https://docs.docker.com/reference/cli/docker/image/tag/
    # Don't do an exhaustive check here, but enough that we're reasonably confident
    # it's a real name and there's no shell injection risk
    if not image_name:
        raise ImageNameParseError("No image name provided")
    # check for illegal chars first or bad things can happen:
    # https://github.com/realityone/docker-image-py/issues/12
    match = _DISALLOWED_CHARS.search(image_name)
    if match:
        raise ImageNameParseError(
            f"Illegal character in image name '{image_name}': '{match.group(1)}'")
    try:
        return reference.Reference.parse_normalized_named(image_name)
    except reference.InvalidReference as e:
        # error messages aren't super great but it's a good chunk of code to make it better,
        # and a user should be able to figure out the error by examination or trying to pull the
        # image themselves
        raise ImageNameParseError(f"Unable to parse image name '{image_name}': {e}") from e


def _assemble_name(ref: reference.Reference) -> str:
    name = ref["name"]
    if ref["tag"]:
        name += f':{ref["tag"]}'
    if ref["digest"]:
        name += f'@{ref["digest"]}'
    return name


class ImageInfoError(Exception):
    """ Base class for image info exceptions. """


class CranePathError(ImageInfoError):
    """ Thrown when the crane executable cannot be found. """


class ImageNameParseError(ImageInfoError):
    """ Thrown when an image name couldn't be parsed. """


class ImageInfoFetchError(ImageInfoError):
    """ Thrown when an error occurs fetching information about an image. """
