"""
Code for interacting with s3 based storage that is expected to run on a remote cluster.

In particular, non-standard lib dependency imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
"""

import aiohttp
import asyncio
from awscrt import checksums as awschecksums
import base64
import gzip
from hashlib import md5
import os
from pathlib import Path
import shutil
import tarfile
from typing import Any, Awaitable
import uuid

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string

# Probably not necessary, but could look into aiofiles for some of these methods
# Potential future (minor) performance improvement, but means more installs on remote clusters


_EXT_GZ = ".gz"
_EXT_TARGZ = ".tar.gz"
_EXT_TGZ = ".tgz"
UNPACK_FILE_EXTENSIONS = [_EXT_GZ, _EXT_TARGZ, _EXT_TGZ]

_CHUNK_SIZE_64KB = 2 ** 16
_MT_FILE_MD5 = md5(b"").hexdigest()


def calculate_etag(infile: Path, partsize: int) -> str:
    """
    Calculate the s3 e-tag for a file.
    
    NOTE: The calculation assumes that
    * the part size is constant except for the last part
    * single part etags are just the md5 of the file
    
    Neither of these assumptions are always true.
    
    The e-tag will not match if the file is encrypted with customer supplied keys or with the
    AWS key management service.
    See https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html for
    more information.
    
    infile - the file to process
    partsize - the size of the file parts.
    
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


def crc64nvme(infile: Path) -> bytes:
    """Compute the CRC-64/NVME checksum of the contents of the given file"""
    # adapted from https://stackoverflow.com/a/59974585/643675
    # Not really a good way to test the expanduser calls
    _check_file(infile)
    with open(infile.expanduser(), "rb") as f:
        checksum = 0
        while chunk := f.read(_CHUNK_SIZE_64KB):
            checksum = awschecksums.crc64nvme(chunk, checksum)
    return checksum.to_bytes(8)


def crc64nvme_b64(infile: Path) -> str:
    """Compute the base64 endoded CRC-64/NVME checksum of the contents of the given file"""
    return base64.b64encode(crc64nvme(infile)).decode()


def _check_file(infile: Path):
    if not infile or not infile.expanduser().is_file():
        raise ValueError("infile must exist and be a file")


async def download_presigned_url(
    session: aiohttp.ClientSession,
    url: str,
    outputpath: Path,
    crc64nvme_expected: str = None,
    insecure_ssl: bool = False,
    timeout_sec: int = 600,
):
    """
    Download a presigned url from S3.
    
    session - the http session.
    url - the presigned url.
    outputpath - where to store the file. If the file exists, it will be overwritten
    crc64nvme_expected - the expected CRC64/NVME checksum for the file. This can ensure
        the file hasn't changed on the server since the last access.
    insecure_ssl - skip the ssl certificate check.
    timeout_sec - the time, in seconds, before the download times out.
    """
    _not_falsy(session, "session")
    _require_string(url, "url")
    _not_falsy(outputpath, "outputpath")
    tout = aiohttp.ClientTimeout(total=timeout_sec)
    try:
        async with session.get(url, ssl=not insecure_ssl, timeout=tout) as resp:
            if 199 < resp.status < 300:  # redirects are handled automatically
                # unfortunately the crc64nvme is not included in the headers
                outputpath.parent.mkdir(exist_ok=True, parents=True)
                with open(outputpath, "wb") as f:
                    async for chunk in resp.content.iter_chunked(_CHUNK_SIZE_64KB):
                        f.write(chunk)
            else:
                # assume the error output isn't too huge
                err = await resp.read()
                raise TransferError(f"GET URL: {url.split('?')[0]} {resp.status}\nError:\n{err}")
    except asyncio.TimeoutError as e:
        raise RemoteTimeoutError(
            f"Timeout downloading to file {outputpath} with timeout {timeout_sec}s"
        ) from e
    got_crc = crc64nvme_b64(outputpath)
    if crc64nvme_expected and crc64nvme_expected != got_crc:
        outputpath.unlink(missing_ok=True)
        raise FileCorruptionError(
            f"CRC64/NVME check failed for url {url.split('?')[0]}. "
            + f"Expected {crc64nvme_expected}, file checksum is {got_crc}")


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
            raise RemoteTimeoutError(
                f"Timeout uploading from file {infile} with timeout {timeout_sec}s"
            ) from e


# Might want to separate the manifest processing stuff into a different module


async def _process_uploads(
    sess: aiohttp.ClientSession,
    files: list[dict[str, Any]],
    concurrency: int,
    insecure_ssl: bool,
    min_timeout_sec: int,
    sec_per_GB: float,
):
    tasks = [upload_presigned_url(
        sess,
        fil["url"],
        fil["fields"],
        Path(fil["file"]),
        insecure_ssl=insecure_ssl,
        timeout_sec=_timeout(min_timeout_sec, Path(fil["file"]).stat().st_size, sec_per_GB),
    ) for fil in files]
    await _run_tasks(tasks, concurrency)


def _ensure_safe_tar_path(parent_dir: Path, member_name: str, tar_file: str):
    """Ensure that the target path is within the base directory."""
    # TODO TEST need to figure out how to make a bad tar file to test this
    abs_base = parent_dir.absolute()
    abs_target = (parent_dir / member_name).absolute()
    if not abs_base.parts == abs_target.parts[:len(abs_base.parts)]:
        raise ValueError(
            f"Unsafe path detected for tarfile {tar_file}: {member_name}"
        )


def _extract_gz(file_path: Path):
    """Extract a .gz file."""
    output_path = file_path.parent / file_path.stem
    with gzip.open(file_path, 'rb') as f_in, open(output_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


def _extract_tar(file_path: Path):
    """Extract a .tar.gz or .tgz file safely."""
    parent_dir = file_path.parent
    with tarfile.open(file_path, 'r:*') as tar:
        for member in tar.getmembers():
            _ensure_safe_tar_path(parent_dir, member.name, file_path)
        tar.extractall(parent_dir)


def get_cache_path(cache_dir: Path, crc64nvme_b64: str) -> Path:
    """
    Get the path for a cached file based on its Base64 encoded CRC64NVME checksum.
    
    cache_dir - the root directory of the file cache.
    crc64nvme_b64 - the Base64 encoded CRC64NVME checksum
    
    Returns a path that looks like:
    <cache_dir>/<first 2 chars of the hex encoded checksum>/<hex encoded checksum>
    """
    # Security note - CRC64s are not cryptographically secure, and so it's possible for a
    # malicious user to pollute the cache with a file intended to block caching of a target
    # file with the same CRC64. The CDM / BERDL team has chosen to go ahead with the current
    # implementation anyway.
    # Could check that this is 8 bytes... YAGNI
    # Necessary since B64 has chars in it that aren't fun in paths
    crchex = base64.b64decode(_require_string(crc64nvme_b64, "crc64nvme_b64")).hex()
    # reduce number of files in top level dir
    return _not_falsy(cache_dir, "cache_dir") / crchex[:2] / crchex


# Re cache expiration, it's expected that the cache lives in the JAWS staging dir,
# so JAWS is expected to handle that

def _download_is_cached(cache_path: Path) -> bool:
    if cache_path.exists():
        os.utime(cache_path)  # touch to update modtime
        return True
    return False


def _add_to_cache(outputpath: Path, cache_path: Path):
    try:
        os.rename(outputpath, cache_path)  # will overwrite if exists on linux
        os.utime(cache_path)  # ensure the modtime is up to date
    finally:
        outputpath.unlink(missing_ok=True)


async def _process_download(
    session: aiohttp.ClientSession,
    url: str,
    cache_dir: str | None,  # Present if std file download
    outputpath: str | None,  # Present if refdata
    crc64nvme_b64: str,
    insecure_ssl: bool,
    timeout_sec: int,
    unpack: bool,
):
    if cache_dir:  # Std file download
        cache_dir = Path(cache_dir)
        cache_path = get_cache_path(cache_dir, crc64nvme_b64)
        if _download_is_cached(cache_path):
            return
        else:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            outputpath = cache_path.with_name(
                cache_path.name + f"_{os.getpid()}.{uuid.uuid4()}.tmp"
            )
    else:  # refdata download
        outputpath = Path(outputpath)
    await download_presigned_url(
        session,
        url,
        outputpath,
        crc64nvme_expected=crc64nvme_b64,
        insecure_ssl=insecure_ssl,
        timeout_sec=timeout_sec,
    )
    if cache_dir:
        _add_to_cache(outputpath, cache_path)
    if unpack:  # never used with a cache
        unpack_archive(outputpath)


def unpack_archive(archive: Path):
    """ Unpack a *.tgz, *.tar.gz, or *.gz file. """
    try:
        op = str(archive).lower()
        if op.endswith(_EXT_TGZ) or op.endswith(_EXT_TARGZ):
            return _extract_tar(archive)
        elif op.endswith(_EXT_GZ):
            return _extract_gz(archive)
        else:
            raise ValueError(
                f"Unsupported unpack file type for file {archive}. " +
                f"Only {', '.join(UNPACK_FILE_EXTENSIONS)} are supported."
            )
    finally:
        archive.unlink(missing_ok=True)


async def _process_downloads(
    sess: aiohttp.ClientSession,
    cache_dir: str | None,
    files: list[dict[str, Any]],
    concurrency: int,
    insecure_ssl: bool,
    min_timeout_sec: int,
    sec_per_GB: float,
):
    tasks = [_process_download(
        sess,
        fil["url"],
        cache_dir,
        fil.get("outputpath"),
        fil["crc64nvme-b64"],
        insecure_ssl,
        _timeout(min_timeout_sec, fil["size"], sec_per_GB),
        fil.get("unpack", False),
    ) for fil in files]
    await _run_tasks(tasks, concurrency)


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
    operation = manifest["op"]
    async with aiohttp.ClientSession() as sess:
        if operation == "download":
            await _process_downloads(
                sess,
                manifest.get("cache-dir"),
                manifest["files"],
                manifest["concurrency"],
                manifest.get("insecure-ssl", False),
                manifest['min-timeout-sec'],
                manifest["sec-per-GB"],
            )
        elif operation == "upload":
            await _process_uploads(
                sess,
                manifest["files"],
                manifest["concurrency"],
                manifest.get("insecure-ssl", False),
                manifest['min-timeout-sec'],
                manifest["sec-per-GB"],
            )
        else:
            raise ValueError(f"unknown operation: {operation}")
    if "completion-file" in manifest:
        with open(manifest["completion-file"], "w") as f:
            # just raise a keyerror if it's not there
            f.write(manifest["completion-file-contents"] + "\n")


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


class TransferError(Exception):
    """ Thrown when a S3 transfer fails. """


class RemoteTimeoutError(TransferError):
    """ Thrown when a S3 transfer timesout. """


class FileCorruptionError(TransferError):
    """ Thrown when a file transfer results in a corrupt file """
