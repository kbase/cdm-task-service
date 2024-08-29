"""
S3 Path related classes and functions.
"""

from collections.abc import Sequence
from typing import Generator
import unicodedata

from .exceptions import S3PathError


class S3Paths:
    """
    A container of format validated S3 paths. The paths may not necessarily exist in the
    s3 instance.
    
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

    def __len__(self):
        return len(self.paths)

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
    # keys can have spaces and / but not //, except for the first characters
    if not path or not path.strip():
        raise S3PathError(f"The s3 path at index {i} cannot be null or a whitespace string")
    parts = path.lstrip().lstrip("/").split("/", 1)  # ignore leading /s in the bucket, be nice
    if len(parts) != 2:
        raise S3PathError(
            f"Path '{path}' at index {i} must start with the s3 bucket and include a key")
    bucket = parts[0].strip()  # be nice to users and clean up the name a bit
    _validate_bucket_name(i, bucket)
    key = parts[1].lstrip("/")  # Leading /s are ignored by s3, but spaces and trailing /s count
    if "//" in key:
        raise S3PathError(f"Path '{path}' at index {i} contains illegal "
                          + "character string '//' in the key")
    # See https://stackoverflow.com/questions/4324790/removing-control-characters-from-a-string-in-python
    for ci, c in enumerate(key):
        if unicodedata.category(c)[0] == 'C':
            raise S3PathError(
                f"Key {key} at index {i} contains a control character at position {ci}")
    return f"{bucket}/{key}"


def _validate_bucket_name(i: int, bucket_name: str):
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    bn = bucket_name
    if not bn:
        raise S3PathError(f"Bucket name at index {i} cannot be whitespace only")
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
