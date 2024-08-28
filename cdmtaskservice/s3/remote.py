"""
Code for interacting with s3 based storage that is expected to run on a remote cluster.

In particular, non-standard lib dependency imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
"""

import aiohttp
from hashlib import md5
from pathlib import Path
from typing import Any
import zlib

# Probably not necessary, but coould look into aiofiles for some of these methods
# Potential future (minor) performance improvement, but means more installs on remote clusters

_CHUNK_SIZE_64KB = 2 ** 16


def calculate_etag(infile: Path, partsize: int) -> str:
    """
    Calculate the s3 e-tag for a file.
    
    The e-tag will not match if the file is encrypted with customer supplied keys or with the
    AWS key management service.
    See https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html for
    more information.
    
    infile - the file to process
    partsize - the size of the file parts. It is assumed that all parts have the same size except
    for the last part.
    
    Returns - the e-tag
    """
    # Adapted from
    # https://teppen.io/2018/10/23/aws_s3_verify_etags/#calculating-an-s3-etag-using-python
    
    # Alternatives:
    # https://github.com/awnimo/compETAG/blob/master/src/comp_etag/core.py#L36
    # Makes you choose between an md5 and an e-tag with parts rather than just returning the
    # e-tag with or without parts
    # https://github.com/DataDog/s3id/
    # Looks like comparison methods vs. calculation methods

    # Manually tested by uploading a 419MB file and a 86MB file to Minio and checking the
    # e-tags matched this function given the part size reported by Minio. The 1st file had 4 parts
    # and the 2nd none.

    # Not really a good way to test the expanduser calls
    _check_file(infile)
    if partsize < 1:
        raise ValueError("partsize must be > 0")
    md5_digests = []
    with open(infile.expanduser(), 'rb') as f:
        # this could theoretically be a 5GB read. May need to read smaller chunks?
        while chunk := f.read(partsize):
            md5_digests.append(md5(chunk).digest())
    if len(md5_digests) == 0:
        raise ValueError("file is empty")
    if len(md5_digests) == 1:
        return md5_digests[0].hex()
    return md5(b''.join(md5_digests)).hexdigest() +  '-' + str(len(md5_digests))


def crc32(infile: Path) -> bytes:
    """Compute the CRC-32 checksum of the contents of the given file"""
    # adapted from https://stackoverflow.com/a/59974585/643675
    # Not really a good way to test the expanduser calls
    _check_file(infile)
    with open(infile.expanduser(), "rb") as f:
        checksum = 0
        while chunk := f.read(_CHUNK_SIZE_64KB):
            checksum = zlib.crc32(chunk, checksum)
    return checksum.to_bytes(4)

def _check_file(infile: Path):
    if not infile or not infile.expanduser().is_file():
        raise ValueError("infile must be exist and be a file")


async def download_presigned_url(
    session: aiohttp.ClientSession,
    url: str,
    etag: str,
    partsize: int,
    outputpath: Path):
    """
    Download a presigned url from S3 and verify the E-tag.
    
    session - the http session.
    url - the presigned url.
    etag - the etag to check the download against.
    partsize - the partsize used when uploading the file to S3
    path - where to store the file. If the file exists, it will be overwritten
    """
    _not_falsy(session, "session")
    _require_string(url, "url")
    _require_string(etag, "etag")
    _not_falsy(outputpath, "outputpath")
    async with session.get(url) as resp:
        if resp.status > 199 and resp.status < 300:  # redirects are handled automatically
            with open(outputpath, "wb") as f:
                async for chunk in resp.content.iter_chunked(_CHUNK_SIZE_64KB):
                    f.write(chunk)
        else:
            # assume the error output isn't too huge
            err = await resp.read()
            raise TransferError(f"GET URL: {url.split('?')[0]} {resp.status}\nError:\n{err}")
    try:
        got_etag = calculate_etag(outputpath, partsize)
    except ValueError:
        outputpath.unlink(missing_ok=True)
        raise
    if etag != got_etag:
        outputpath.unlink(missing_ok=True)
        raise FileCorruptionError(
            f"Etag check failed for url {url.split('?')[0]}. Expected {etag}, got {got_etag}")


# These arg checkers are duplicated in other places, but we want to minimize the number of files
# we have to transfer to the remote cluster and they're simple enough that duplication isn't
# a huge problem


def _require_string(string: str, name: str):
    if not string or not string.strip():
        raise ValueError(f"{name} is required")
    return string.strip()


def _not_falsy(obj: Any, name: str):
    if not obj:
        raise ValueError(f"{name} is required")


class TransferError(Exception):
    """ Thrown when a S3 transfer fails. """


class FileCorruptionError(Exception):
    """ Thrown when a file transfer results in a corrupt file """
