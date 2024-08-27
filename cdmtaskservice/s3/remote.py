"""
Code for interacting with s3 based storage that is expected to run on a remote cluster.

In particular, non-standard lib dependency imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
"""

from hashlib import md5
from pathlib import Path
import zlib


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
        while chunk := f.read(65536):
            checksum = zlib.crc32(chunk, checksum)
    return checksum.to_bytes(4)

def _check_file(infile: Path):
    if not infile or not infile.expanduser().is_file():
        raise ValueError("infile must be exist and be a file")
