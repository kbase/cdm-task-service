"""
S3 Path related classes and functions.
"""

from collections.abc import Sequence
from typing import Generator

from .exceptions import S3PathError


class S3Paths:
    """
    A container of format validated S3 paths. The paths may not exist in the s3 instance.
    
    Instance variables:
    
    paths - a tuple of the input paths, stripped of surrounding whitespace.
    """

    def __init__(self, paths: Sequence[str]):
        """
        Create the paths.
        
        paths - a sequence of S3 paths, all starting with the bucket.
        
        throws S3Path error if a path is not formatted correctly.
        """
        if not paths:
            raise ValueError("At least one path must be supplied.")
        newpaths = []
        for i, p in enumerate(paths):
            newpaths.append(_validate_path(i, p))
        self.paths = tuple(newpaths)


    def split_paths(self, include_full_path=False) -> Generator[list[str, ...], None, None]:
        """
        Returns a generator over the paths, split into [bucket, key] lists.
        
        include_full_path - append the full path to the returned list.
        """
        for p in self.paths:
            parts = p.split("/", 1)
            if include_full_path:
                parts.append(p)
            yield parts


def _validate_path(i: int, path: str) -> str:
    if not path or not path.strip():
        raise S3PathError(f"The s3 path at index {i} cannot be null or a whitespace string")
    parts = [s.strip() for s in path.split("/", 1) if s.strip()]
    if len(parts) != 2:
        raise S3PathError(
            f"path '{path.strip()}' at index {i} must start with the s3 bucket and include a key")
    _validate_bucket_name(i, parts[0])
    return path.strip()


def _validate_bucket_name(i: int, bucket_name: str):
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    bn = bucket_name.strip()
    if len(bn) < 3 or len(bn) > 63:
        raise S3PathError(f"Bucket name at index {i} must be > 2 and < 64 characters: {bn}")
    if "." in bn:
        raise S3PathError(f"Bucket at index {i} has `.` in the name which is unsupported: {bn}")
    if bn.startswith("-") or bn.endswith("-"):
        raise S3PathError(f"Bucket name at index {i} cannot start or end with '-': {bn}")
    if not bn.replace("-", "").isalnum() or not bn.isascii() or not bn.islower():
        raise S3PathError(
            f"Bucket name at index {i} may only contain '-' and lowercase ascii "
            + f"alphanumerics: {bn}")
