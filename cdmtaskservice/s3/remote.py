"""
Code for interacting with s3 based storage that is expected to run on a remote cluster.

In particular, non-standard lib dependency imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
"""

import aiohttp
import asyncio
import base64
from hashlib import md5
import os
from pathlib import Path
from typing import Any, Awaitable
import zlib

# Probably not necessary, but could look into aiofiles for some of these methods
# Potential future (minor) performance improvement, but means more installs on remote clusters

_CHUNK_SIZE_64KB = 2 ** 16
_MT_FILE_MD5 = md5(b"").hexdigest()


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
        return _MT_FILE_MD5
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
        raise ValueError("infile must exist and be a file")


async def download_presigned_url(
    session: aiohttp.ClientSession,
    url: str,
    partsize: int,
    outputpath: Path,
    etag: str = None,
    insecure_ssl: bool = False,
    timeout_sec: int = 600,
):
    """
    Download a presigned url from S3 and verify the E-tag.
    
    session - the http session.
    url - the presigned url.
    partsize - the partsize used when uploading the file to S3
    outputpath - where to store the file. If the file exists, it will be overwritten
    etag - if provided,
         a) checks that the server Etag matches this Etag. The downloaded file is
            always checked against the server provided Etag as an integrity check, but providing
            an Etag can ensure the file hasn't changed on the server side since the last access.
        b) if a file already exists at outputpath and matches the etag, the download is skipped.
    insecure_ssl - skip the ssl certificate check.
    timeout_sec - the time, in seconds, before the download times out.
    
    Returns True if the download occurred or False if the file already exists.
    """
    _not_falsy(session, "session")
    _require_string(url, "url")
    _not_falsy(outputpath, "outputpath")
    if partsize < 1:
        raise ValueError("partsize must be > 0")
    if etag and outputpath.is_file():
        got_etag = calculate_etag(outputpath, partsize)
        if got_etag == etag:
            return False
    tout = aiohttp.ClientTimeout(total=timeout_sec)
    try:
        async with session.get(url, ssl=not insecure_ssl, timeout=tout) as resp:
            if 199 < resp.status < 300:  # redirects are handled automatically
                serv_etag = resp.headers["Etag"].strip('"')
                if etag and etag != serv_etag:
                    raise FileChangeError(
                        f"Etag check failed for url {url.split('?')[0]}. "
                        + f"Server provided {serv_etag}, user provided Etag requirement is {etag}")
                outputpath.parent.mkdir(exist_ok=True, parents=True)
                with open(outputpath, "wb") as f:
                    async for chunk in resp.content.iter_chunked(_CHUNK_SIZE_64KB):
                        f.write(chunk)
            else:
                # assume the error output isn't too huge
                err = await resp.read()
                raise TransferError(f"GET URL: {url.split('?')[0]} {resp.status}\nError:\n{err}")
    except asyncio.TimeoutError as e:
        raise RemoteTimeoutError(f"Timeout downloading to file {outputpath} with timeout {timeout_sec}s"
                           ) from e
    got_etag = calculate_etag(outputpath, partsize)
    if got_etag != serv_etag:
        outputpath.unlink(missing_ok=True)
        raise FileCorruptionError(
            f"Etag check failed for url {url.split('?')[0]}. "
            + f"Server provided {serv_etag}, file Etag is {got_etag}")
    return True


_UPLOAD_REQUIRED_FIELDS = ["key", "AWSAccessKeyId", "signature", "policy"]


async def upload_presigned_url(
    session: aiohttp.ClientSession,
    url: str,
    fields: dict[str, str],
    infile: Path,
    insecure_ssl: bool = False,
    timeout_sec: int = 600,
):
    """
    Upload a file to S3 via a presigned url. If the object already exists in S3, it will be
    overwritten.
    
    session - the http session.
    url - the presigned url.
    fields - the fields associated with the presigned url returned by the S3 client.
    infile - the file to upload.
    insecure_ssl - skip the ssl certificate check.
    timeout_sec - the time, in seconds, before the upload times out.
    """
    _not_falsy(session, "session")
    _require_string(url, "url")
    _not_falsy(fields, "fields")
    for f in _UPLOAD_REQUIRED_FIELDS:
        if fields.get(f) is None or not fields.get(f).strip():
            raise ValueError(f"fields missing required '{f}' field")
    _check_file(infile)
    data = aiohttp.FormData(fields)
    with open(infile, "rb") as f:
        data.add_field("file", f)
        tout = aiohttp.ClientTimeout(total=timeout_sec)
        try:
            async with session.post(url, ssl=not insecure_ssl, data=data, timeout=tout) as resp:
                # Returns 204 no content on success
                if resp.status < 200 or resp.status > 299:  # redirects are handled automatically
                    # assume the error output isn't too huge
                    err = await resp.read()
                    raise TransferError(
                        f"POST URL: {url} Key: {fields['key']} {resp.status}\nError:\n{err}")
        except asyncio.TimeoutError as e:
            raise RemoteTimeoutError(f"Timeout uploading from file {infile} with timeout {timeout_sec}s"
                               ) from e


# could merge this with the above method and add a toggle... eh
async def upload_presigned_url_with_crc32(
    session: aiohttp.ClientSession,
    url: str,
    fields: dict[str, str],
    infile: Path,
    insecure_ssl: bool = False,
    timeout_sec: int = 600,
):
    """
    As upload_presigned_url but calculates the crc32 of the input file and sends it to S3 as
    an integrity check.
    """
    _not_falsy(fields, "fields")
    fields = dict(fields)  # don't mutate the input
    fields["x-amz-checksum-crc32"] = base64.b64encode(crc32(infile)).decode()
    await upload_presigned_url(
        session, url, fields, infile, insecure_ssl=insecure_ssl, timeout_sec=timeout_sec)


async def _process_uploads(
    sess: aiohttp.ClientSession,
    root: Path,
    files: list[dict[str, Any]],
    concurrency: int,
    insecure_ssl: bool,
    min_timeout_sec: int,
    sec_per_GB: float,
):
    tasks = [upload_presigned_url_with_crc32(
        sess,
        fil["url"],
        fil["fields"],
        root / fil["file"],
        insecure_ssl=insecure_ssl,
        timeout_sec=_timeout(min_timeout_sec, (root / fil["file"]).stat().st_size, sec_per_GB),
    ) for fil in files]
    await _run_tasks(tasks, concurrency)


async def _process_downloads(
    sess: aiohttp.ClientSession,
    root: Path,
    files: list[dict[str, Any]],
    concurrency: int,
    insecure_ssl: bool,
    min_timeout_sec: int,
    sec_per_GB: float,
):
    tasks = [download_presigned_url(
        sess,
        fil["url"],
        fil["partsize"],
        root / fil["outputpath"],
        etag=fil["etag"],
        insecure_ssl=insecure_ssl,
        timeout_sec=_timeout(min_timeout_sec, fil["size"], sec_per_GB),
    ) for fil in files]
    await _run_tasks(tasks, concurrency)


_OP_TO_FUNC = {
    "upload": _process_uploads,
    "download": _process_downloads,
}


async def process_data_transfer_manifest(manifest: dict[str, Any]):
    """
    Process a manifest specifying data to upload or download to or from S3.
    """
    # The manifest should be only used by the CDM task service and so we don't document
    # its structure.
    # Similarly, it should only be produced and consumed by the service, and so we don't
    # stress error checking too much.
    # Potential performance improvements:
    # * aiofiles
    # * Add multiprocessing; not clear if helpful given low CPU load expected 
    # * See if multipart uploads are possible with presigned urls
    #   * Presumably only helpful if disk reads are the bottleneck
    # TODO TEST add tests for this and its dependency functions.
    _not_falsy(manifest, "manifest")
    func = _OP_TO_FUNC.get(manifest["op"])
    if not func:
        raise ValueError(f"unknown operation: {manifest['op']}")
    root = Path("/")
    if manifest.get("env-root"):
        root = os.environ.get(manifest["env-root"])
        if not root or not root.strip():
            raise ValueError(f"Value of the environment variable {manifest['env-root']} "
                             + "from the manifest env-root field is missing or the empty string")
        root = Path(root)
    async with aiohttp.ClientSession() as sess:
        await func(
            sess,
            root,
            manifest["files"],
            manifest["concurrency"],
            manifest.get("insecure-ssl", False),
            manifest['min-timeout-sec'],
            manifest["sec-per-GB"],
        )


def _timeout(min_timeout_sec: int, filesize: int, sec_per_GB: float) -> float:
    return max(min_timeout_sec, sec_per_GB * filesize / 1_000_000_000)


async def _run_tasks(
    tasks: list[Awaitable],
    concurrency: int
):
    semaphore = asyncio.Semaphore(concurrency)
    async def sem_coro(coro):
        async with semaphore:
            return await coro
    try: 
        async with asyncio.TaskGroup() as tg:
            for t in tasks:
                tg.create_task(sem_coro(t))
                # just throw any ExceptionGroups as is
    finally:
        # otherwise you can get coroutine never awaited warnings if a failure occurs
        for t in tasks:
            t.close()


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


class RemoteTimeoutError(TransferError):
    """ Thrown when a S3 transfer timesout. """


class FileCorruptionError(TransferError):
    """ Thrown when a file transfer results in a corrupt file """


class FileChangeError(Exception):
    """ Thrown when a file has changed since the last access """
