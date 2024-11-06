"""
S3 Path related classes and functions.
"""

from collections.abc import Sequence
import re
from typing import Generator
import unicodedata

from .exceptions import S3PathError

# https://code.jgi.doe.gov/advanced-analysis/jaws-docs/-/issues/86
# These characters are obnoxious in filenames anyway
# May need to add more characters as problems are discovered
# Double forward slash is disallowed by S3
_DISALLOWED_CHARACTERS = re.compile(r"([';]|[/]{2})")


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
            newpaths.append(validate_path(p, index=i))
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


# TDOO TEST add tests for public method
# TODO S3PATHS allow accepting model paths to the constructor here so they aren't validated 2x 
def validate_path(path: str, index: int = None) -> str:
    """
    Validate an S3 path.
    
    path - the path to validate.
    index - the index of the path in some external data structure. The index will be added to
        error messages.
    
    Returns a normalized path.
    """
    # keys can have spaces and / but not //, except for the first characters
    i = f" at index {index}" if index is not None else ""
    if not path or not path.strip():
        raise S3PathError(f"The s3 path{i} cannot be null or a whitespace string")
    parts = path.lstrip().lstrip("/").split("/", 1)  # ignore leading /s in the bucket, be nice
    if len(parts) != 2:
        raise S3PathError(
            f"Path '{path}'{i} must start with the s3 bucket and include a key")
    bucket = validate_bucket_name(parts[0], index=index)
    key = parts[1].lstrip("/")  # Leading /s are ignored by s3, but spaces and trailing /s count
    if len(key.encode("UTF-8")) > 1024:
        raise S3PathError(f"Path '{path}'{i}'s key is longer than 1024 bytes in UTF-8")
    match_ = _DISALLOWED_CHARACTERS.search(key)
    if match_:
        raise S3PathError(f"Path '{path}'{i} contains illegal character(s) '{match_.group(1)}' "
                          + f"at key index {match_.start()}")
    # See https://stackoverflow.com/questions/4324790/removing-control-characters-from-a-string-in-python
    # Don't want to put control chars in a returned error
    for ci, c in enumerate(key):
        if unicodedata.category(c)[0] == 'C':
            raise S3PathError(
                f"Path {path}{i} contains a control character in the key at position {ci}")
    return f"{bucket}/{key}"


# TDOO TEST add tests for public method
def validate_bucket_name(bucket_name: str, index: int = None):
    """
    Validate an S3 bucket name.
    
    bucket_name - the bucket name to validate.
    index - the index of the bucket name in some external data structure.
        The index will be added to error messages.
    
    Returns a bucket name stripped of whitespace..
    """
    # https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    i = f" at index {index}" if index is not None else ""
    bn = bucket_name.strip()
    if not bn:
        raise S3PathError(f"Bucket name{i} cannot be whitespace only")
    if len(bn) < 3 or len(bn) > 63:
        raise S3PathError(f"Bucket name{i} must be > 2 and < 64 characters: {bn}")
    if "." in bn:
        raise S3PathError(f"Bucket{i} has `.` in the name which is unsupported: {bn}")
    if bn.startswith("-") or bn.endswith("-"):
        raise S3PathError(f"Bucket name{i} cannot start or end with '-': {bn}")
    if not bn.replace("-", "").isalnum() or not bn.isascii() or not bn.islower():
        raise S3PathError(
            f"Bucket name{i} may only contain '-' and lowercase ascii alphanumerics: {bn}")
    return bn
