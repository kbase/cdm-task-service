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
import logging
import os
from pathlib import Path
from typing import Self

from fqdn import FQDN


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

    async def get_digest_from_name(self, image_name: str) -> str:
        """
        Get an image digest given the image name.
        """
        image_name = _validate_image_name(image_name)
        retcode, stdo, stde = await self._run_crane("digest", image_name)
        if retcode > 0:
            # TODO LOGGING figure out how this is going to work, want to associate logs with
            #              user names, ips, etc.
            # TDOO TEST logging add tests for logging before exiting prototype stage
            #           manual testing for now
            logging.getLogger(__name__).error(
                f"crane lookup of image {image_name} failed, retcode: {retcode}, stderr:\n{stde}")
            # special case when crane spits out html and the regular error parsing doesn't work
            if "404 not found" in stde.lower():
                raise DigestFetchError(
                    f"Failed to get digest for image {image_name}. "
                    + "Image was not found on the host")
            # This is pretty fragile for non-docker repository hosts.
            # Not really sure what else to do here
            # Allowlist for hosts?
            errcode = stde.split("\n")[-1].split(':')[-1].strip()
            raise DigestFetchError(
                f"Failed to get digest for image {image_name}. Error code was: {errcode}")
        return stdo

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


def _validate_image_name(image_name: str) -> str:
    image_name = image_name.strip() if image_name is not None else None
    # Image name rules: https://docs.docker.com/reference/cli/docker/image/tag/
    # Don't do an exhaustive check here, but enough that we're reasonably confident
    # it's a real name and there's no shell injection risk
    # Also allows us to provide more specific error messages
    if not image_name:
        raise ImageNameParseError("No image name provided")
    parts = image_name.split("/")
    # assume a namespace is always present
    if len(parts) == 2:  # docker
        host, repo, image_and_tag = None, parts[0], parts[1]
    elif len(parts) == 3:
        host, repo, image_and_tag = parts
    else:
        # theoretically can have > 2 slashes, but don't bother with that case for now.
        raise ImageNameParseError(f"Expected 1 or 2 '/' symbols in image name '{image_name}'")
    if host and not FQDN(host).is_valid:
        raise ImageNameParseError(f"Illegal host '{host}' in image name '{image_name}'")
    _check_path(image_name, repo)
    _check_path(image_name, image_and_tag)
    return image_name


# legal non alphanum characters in docker image name path / tag
_TRANS_TABLE = str.maketrans({
    '-': None,
    '.': None,
    '_': None,
    ':': None,
    '@': None,  # for digests
    
})


def _check_path(image_name, part):
    part = part.translate(_TRANS_TABLE)
    if not part.isalnum():
        raise ImageNameParseError(
            "path or tag contains non-alphanumeric characters other than '_-:.@' "
            + f"in image name '{image_name}'")
    if not part.isascii():
        raise ImageNameParseError(
            f"path or tag contains non-ascii characters in image name '{image_name}'")
    if not part.islower(): 
        raise ImageNameParseError(
            f"path or tag contains upper case characters in image name '{image_name}'")


class ImageInfoError(Exception):
    """ Base class for image info exceptions. """


class CranePathError(ImageInfoError):
    """ Thrown when the crane executable cannot be found. """


class ImageNameParseError(ImageInfoError):
    """ Thrown when an image name couldn't be parsed. """


class DigestFetchError(ImageInfoError):
    """ Thrown when an error occurs fetching a digest for an image. """
