import aiohttp
import os
from pathlib import Path
import pytest
import shutil
import tempfile

from conftest import assert_exception_correct, minio  # @UnusedImport
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.s3.remote import (
    calculate_etag,
    crc32,
    download_presigned_url,
    FileCorruptionError,
    TransferError,
)
import config

TESTDATA = Path(os.path.normpath((Path(__file__) / ".." / ".." / "testdata")))


@pytest.fixture(scope="module")
def temp_dir():
    
    td = Path(tempfile.mkdtemp(prefix="remote_test", dir=config.TEMP_DIR))
    
    yield td
    
    if not config.TEMP_DIR_KEEP:
        shutil.rmtree(td)


def test_calculate_etag():
    testset = [
        (TESTDATA / "random_bytes_1kB", 1024, "b10278db14633f102103c5e9d75c0af0"),
        (TESTDATA / "random_bytes_1kB", 10000, "b10278db14633f102103c5e9d75c0af0"),
        (TESTDATA / "random_bytes_10kB", 1024, "b4b7898bf290001d169572b777efd34f-10"),
        (TESTDATA / "random_bytes_10kB", 5000, "a70a4d1732484e75434df2c08570e1b2-3"),
        (TESTDATA / "random_bytes_10kB", 10240, "3291fbb392f6fad06dbf331dfb74da81"),
        (TESTDATA / "random_bytes_10kB", 100000, "3291fbb392f6fad06dbf331dfb74da81"),
    ]
    for infile, size, etag in testset:
        gottag = calculate_etag(infile, size)
        assert gottag == etag


def test_calculate_etag_fail():
    testset = [
        (None, 1, ValueError("infile must be exist and be a file")),
        (TESTDATA, 1, ValueError("infile must be exist and be a file")),
        (TESTDATA / "empty_file", 1, ValueError("file is empty")),
        (TESTDATA / "random_bytes_1kB", 0, ValueError("partsize must be > 0")),
        (TESTDATA / "random_bytes_1kB", -10000, ValueError("partsize must be > 0")),
    ]
    for infile, size, expected in testset:
        with pytest.raises(Exception) as got:
            calculate_etag(infile, size)
        assert_exception_correct(got.value, expected)


def test_crc32():
    # checked these with the linux crc32 program
    testset = [
        (TESTDATA / "empty_file", "00000000"),
        (TESTDATA / "random_bytes_1kB", "ed9a6eb3"),
        (TESTDATA / "random_bytes_10kB", "4ffc5208"),
    ]
    for infile, crc in testset:
        gotcrc = crc32(infile)
        assert gotcrc.hex() == crc


def test_crc32_fail():
    testset = [
        (None, ValueError("infile must be exist and be a file")),
        (TESTDATA, ValueError("infile must be exist and be a file")),
    ]
    for infile, expected in testset:
        with pytest.raises(Exception) as got:
            crc32(infile)
        assert_exception_correct(got.value, expected)


@pytest.mark.asyncio
async def test_download_presigned_url(minio, temp_dir):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    await minio.upload_file("test-bucket/myfile", b"abcdefghij")
    
    s3c = await _client(minio)
    url = (await s3c.presign_get_urls(S3Paths(["test-bucket/myfile"])))[0]
    output = temp_dir / "temp1.txt"
    async with aiohttp.ClientSession() as sess:
        await download_presigned_url(sess, url, "a925576942e94b2ef57a066101b48876", 10, output)
    with open(output) as f:
        assert f.read() == "abcdefghij"


@pytest.mark.asyncio
async def test_download_presigned_url_multipart(minio, temp_dir):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("nice-bucket")
    res = await minio.upload_file(
        "nice-bucket/big_test_file", b"abcdefghij" * 600000, 4, b"bigolfile")
    
    s3c = await _client(minio)
    url = (await s3c.presign_get_urls(S3Paths(["nice-bucket/big_test_file"])))[0]
    output = temp_dir / "temp2.txt"
    async with aiohttp.ClientSession() as sess:
        await download_presigned_url(
            sess, url, "9728af2f2c566b2b944b96203769175d-5", 6000000, output)
    with open(output) as f:
        assert f.read() == "abcdefghij" * 600000 * 4 + "bigolfile"


@pytest.mark.asyncio
async def test_download_presigned_url_fail_no_session(minio, temp_dir):
    output = temp_dir / "fail.txt"
    et = "a925576942e94b2ef57a066101b48876"
    s3c = await _client(minio)
    url = (await s3c.presign_get_urls(S3Paths(["test-bucket/myfile"])))[0]
    with pytest.raises(Exception) as got:
        await download_presigned_url(None, url, et, 10, output)
    assert_exception_correct(got.value, ValueError("session is required"))
    assert not output.exists()


@pytest.mark.asyncio
async def test_download_presigned_url_fail_bad_args(minio, temp_dir):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    await minio.upload_file("test-bucket/myfile", b"abcdefghij")
    et = "a925576942e94b2ef57a066101b48876"
    ps = 10
    o = temp_dir / "fail.txt"
    
    s3c = await _client(minio)
    url = (await s3c.presign_get_urls(S3Paths(["test-bucket/myfile"])))[0]
    await _download_presigned_url_fail(None, et, ps, o, ValueError("url is required"))
    await _download_presigned_url_fail("  \t   ", et, ps, o, ValueError("url is required"))
    await _download_presigned_url_fail(url, None, ps, o, ValueError("etag is required"))
    await _download_presigned_url_fail(url, "   \t  ", ps, o, ValueError("etag is required"))
    await _download_presigned_url_fail(url, "foo", ps, o, FileCorruptionError(
        f"Etag check failed for url http://localhost:{minio.port}/test-bucket/myfile. "
        + "Expected foo, got a925576942e94b2ef57a066101b48876"))
    await _download_presigned_url_fail(url, et, 0, o, ValueError("partsize must be > 0"))
    await _download_presigned_url_fail(url, et, 3, o, FileCorruptionError(
        f"Etag check failed for url http://localhost:{minio.port}/test-bucket/myfile. "
        + "Expected a925576942e94b2ef57a066101b48876, got 1543089f5b20740cc5713f0437fcea8c-4"))
    await _download_presigned_url_fail(url, et, ps, None, ValueError("outputpath is required"))
    
    
async def _download_presigned_url_fail(url, etag, partsize, output, expected):
    with pytest.raises(Exception) as got:
        async with aiohttp.ClientSession() as sess:
            await download_presigned_url(sess, url, etag, partsize, output)
    assert_exception_correct(got.value, expected)
    if output:
        assert not output.exists()


@pytest.mark.asyncio
async def test_download_presigned_url_fail_bad_sig(minio, temp_dir):
    starts_with = f"GET URL: http://localhost:{minio.port}/test-bucket/myfilex 403\nError:\n"
    contains = ("<Error><Code>SignatureDoesNotMatch</Code><Message>The request signature we "
            + "calculated does not match the signature you provided. Check your key and "
            + "signing method.</Message><Key>myfilex</Key><BucketName>test-bucket</BucketName>"
            + "<Resource>/test-bucket/myfilex</Resource>"
    )
    s3c = await _client(minio)
    url = (await s3c.presign_get_urls(S3Paths(["test-bucket/myfile"]))
           )[0].replace("myfile", "myfilex")
    await _download_presigned_url_fail_s3_error(minio, temp_dir, url, starts_with, contains)


@pytest.mark.asyncio
async def test_download_presigned_url_fail_nofile(minio, temp_dir):
    starts_with = f"GET URL: http://localhost:{minio.port}/test-bucket/myfilex 404\nError:\n"
    contains = ("<Error><Code>NoSuchKey</Code><Message>The specified key does not exist."
                + "</Message><Key>myfilex</Key><BucketName>test-bucket</BucketName>"
                +"<Resource>/test-bucket/myfilex</Resource>"
    )
    s3c = await _client(minio)
    url = (await s3c.presign_get_urls(S3Paths(["test-bucket/myfilex"])))[0]
    await _download_presigned_url_fail_s3_error(minio, temp_dir, url, starts_with, contains)


async def _download_presigned_url_fail_s3_error(minio, temp_dir, url, starts_with, contains):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    await minio.upload_file("test-bucket/myfile", b"abcdefghij")
    et = "a925576942e94b2ef57a066101b48876"
    output = temp_dir / "fail_s3.txt"
    with pytest.raises(Exception) as got:
        async with aiohttp.ClientSession() as sess:
            await download_presigned_url(sess, url, et, 10, output)
    errmsg = str(got.value)
    assert errmsg.startswith(starts_with)
    assert contains in errmsg
    assert type(got.value) == TransferError
    assert not output.exists()


async def _client(minio):
    return await S3Client.create(minio.host,  minio.access_key, minio.secret_key)
