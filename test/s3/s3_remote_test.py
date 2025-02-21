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
    crc64nvme,
    download_presigned_url,
    upload_presigned_url,
    FileCorruptionError,
    TransferError,
    RemoteTimeoutError,
)
from test_common import config

TESTDATA = Path(os.path.normpath((Path(__file__) / ".." / ".." / "testdata")))
TEST_MT = TESTDATA / "empty_file"
TEST_RAND1KB = TESTDATA / "random_bytes_1kB"
TEST_RAND10KB = TESTDATA / "random_bytes_10kB"


@pytest.fixture(scope="module")
def temp_dir():
    
    td = Path(tempfile.mkdtemp(prefix="remote_test", dir=config.TEMP_DIR))
    
    yield td
    
    if not config.TEMP_DIR_KEEP:
        shutil.rmtree(td)


def test_calculate_etag():
    testset = [
        (TEST_MT, 1024, "d41d8cd98f00b204e9800998ecf8427e"),
        (TEST_RAND1KB, 1024, "b10278db14633f102103c5e9d75c0af0"),
        (TEST_RAND1KB, 10000, "b10278db14633f102103c5e9d75c0af0"),
        (TEST_RAND10KB, 1024, "b4b7898bf290001d169572b777efd34f-10"),
        (TEST_RAND10KB, 5000, "a70a4d1732484e75434df2c08570e1b2-3"),
        (TEST_RAND10KB, 10240, "3291fbb392f6fad06dbf331dfb74da81"),
        (TEST_RAND10KB, 100000, "3291fbb392f6fad06dbf331dfb74da81"),
    ]
    for infile, size, etag in testset:
        gottag = calculate_etag(infile, size)
        assert gottag == etag


def test_calculate_etag_fail():
    testset = [
        (None, 1, ValueError("infile must exist and be a file")),
        (TESTDATA, 1, ValueError("infile must exist and be a file")),
        (TEST_RAND1KB, 0, ValueError("partsize must be > 0")),
        (TEST_RAND1KB, -10000, ValueError("partsize must be > 0")),
    ]
    for infile, size, expected in testset:
        with pytest.raises(Exception) as got:
            calculate_etag(infile, size)
        assert_exception_correct(got.value, expected)


def test_crc64nvme():
    # checked these with the linux crc32 program
    testset = [
        (TEST_MT, "0000000000000000"),
        (TEST_RAND1KB, "e1e92dd9607528ee"),
        (TEST_RAND10KB, "b8813a5a204f469b"),
    ]
    for infile, crc in testset:
        gotcrc = crc64nvme(infile)
        assert gotcrc.hex() == crc


def test_crc64nvme_fail():
    testset = [
        (None, ValueError("infile must exist and be a file")),
        (TESTDATA, ValueError("infile must exist and be a file")),
    ]
    for infile, expected in testset:
        with pytest.raises(Exception) as got:
            crc64nvme(infile)
        assert_exception_correct(got.value, expected)


@pytest.mark.asyncio
async def test_download_presigned_url(minio, temp_dir):
    await _test_download_presigned_url(minio, temp_dir, None)
    await _test_download_presigned_url(minio, temp_dir, "e/Vz6rUQ/+o=")

async def _test_download_presigned_url(minio, temp_dir, crc):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    await minio.upload_file("test-bucket/myfile", b"abcdefghij", crc64nvme="e/Vz6rUQ/+o=")
    
    s3c = await _client(minio)
    url = (await s3c.presign_get_urls(S3Paths(["test-bucket/myfile"])))[0]
    output = temp_dir / "somedir" / "temp.txt"  # test directory creation for the first case
    async with aiohttp.ClientSession() as sess:
        await download_presigned_url(
            sess, url, output, crc64nvme_expected=crc, timeout_sec=5
        )
    with open(output) as f:
        assert f.read() == "abcdefghij"


@pytest.mark.asyncio
async def test_download_presigned_url_multipart_and_insecure_ssl(minio, temp_dir):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("nice-bucket")
    await minio.upload_file(
        "nice-bucket/big_test_file",
        b"abcdefghij" * 600000,
        4,
        b"bigolfile",
        main_part_crc64nvme="GtvBv8sO0DA=",
        last_part_crc64nvme="LSWN4IrdjXs=",
        crc64nvme="B/mN7A/vO4c=",
    )

    s3c = await _client(minio)
    url = (await s3c.presign_get_urls(S3Paths(["nice-bucket/big_test_file"])))[0]
    output = temp_dir / "temp2.txt"
    async with aiohttp.ClientSession() as sess:
        # There's not a lot to test with insecure ssl other than it doesn't break things
        # Unless we want to get really crazy and set up Minio with a SSC in the tests. We don't
        await download_presigned_url(
            sess, url, output, crc64nvme_expected="B/mN7A/vO4c=", insecure_ssl=True
        )
    with open(output) as f:
        assert f.read() == "abcdefghij" * 600000 * 4 + "bigolfile"


@pytest.mark.asyncio
async def test_download_presigned_url_fail_bad_args(minio, temp_dir):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    await minio.upload_file("test-bucket/myfile", b"abcdefghij")
    o = temp_dir / "fail.txt"
    
    s3c = await _client(minio)
    url = (await s3c.presign_get_urls(S3Paths(["test-bucket/myfile"])))[0]
    async with aiohttp.ClientSession() as s:
        await _download_presigned_url_fail(None, url, o, ValueError("session is required"))
        await _download_presigned_url_fail(s, None, o, ValueError("url is required"))
        await _download_presigned_url_fail(s, "  \t   ", o, ValueError("url is required"))
        await _download_presigned_url_fail(s, url, o, FileCorruptionError(
            f"CRC64/NVME check failed for url http://localhost:{minio.port}/test-bucket/myfile. "
            + "Expected foo, file checksum is e/Vz6rUQ/+o="),
            crc64nvme="foo"
        )
        await _download_presigned_url_fail(
            s, url, None, ValueError("outputpath is required"))
        await _download_presigned_url_fail(
            s, url, o, RemoteTimeoutError(f"Timeout downloading to file {o} with timeout 1e-05s"),
            timeout_sec=0.00001
        )
    
    
async def _download_presigned_url_fail(
    sess, url, output, expected, crc64nvme=None, timeout_sec=600
):
    with pytest.raises(Exception) as got:
        await download_presigned_url(
            sess, url, output, crc64nvme, timeout_sec=timeout_sec)
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
    output = temp_dir / "fail_s3.txt"
    with pytest.raises(Exception) as got:
        async with aiohttp.ClientSession() as sess:
            await download_presigned_url(sess, url, 10, output)
    errmsg = str(got.value)
    assert errmsg.startswith(starts_with)
    assert contains in errmsg
    assert type(got.value) == TransferError
    assert not output.exists()


@pytest.mark.asyncio
async def test_upload_presigned_url(minio):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    
    s3c = await _client(minio)
    url = (await s3c.presign_post_urls(S3Paths(["test-bucket/foo/myfile"])))[0]
    async with aiohttp.ClientSession() as sess:
        await upload_presigned_url(sess, url.url, url.fields, TEST_RAND10KB, timeout_sec=5)
    with open(TEST_RAND10KB, "rb") as f:
        expectedfile = f.read()
    objdata = await minio.get_object("test-bucket", "foo/myfile")
    body = await objdata["Body"].read()
    assert body == expectedfile
    assert objdata["ETag"] == '"3291fbb392f6fad06dbf331dfb74da81"'


@pytest.mark.asyncio
async def test_upload_presigned_url_with_crc_and_insecure_ssl(minio):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    
    s3c = await _client(minio)
    url = (await s3c.presign_post_urls(S3Paths(["test-bucket/foo/myfile"]), ["4ekt2WB1KO4="]))[0]
    async with aiohttp.ClientSession() as sess:
        # There's not a lot to test with insecure ssl other than it doesn't break things
        # Unless we want to get really crazy and set up Minio with a SSC in the tests. We don't
        await upload_presigned_url(
            sess, url.url, url.fields, TEST_RAND1KB, insecure_ssl=True, timeout_sec=5)
    with open(TEST_RAND1KB, "rb") as f:
        expectedfile = f.read()
    objdata = await minio.get_object("test-bucket", "foo/myfile")
    
    body = await objdata["Body"].read()
    assert body == expectedfile
    assert objdata["ETag"] == '"b10278db14633f102103c5e9d75c0af0"'
    assert objdata["ChecksumCRC64NVME"] == "4ekt2WB1KO4="


@pytest.mark.asyncio
async def test_upload_presigned_url_fail(minio):
    s3c = await _client(minio)
    r = (await s3c.presign_post_urls(S3Paths(["test-bucket/foo/myfile"])))[0]
    url, flds = r.url, r.fields
    fl = TEST_RAND10KB
    
    async with aiohttp.ClientSession() as s:
        await _upload_presigned_url_fail(None, url, flds, fl, ValueError("session is required"))
        await _upload_presigned_url_fail(s, None, flds, fl, ValueError("url is required"))
        await _upload_presigned_url_fail(s, "  \t   ", flds, fl, ValueError("url is required"))
        await _upload_presigned_url_fail(s, url, None, fl, ValueError("fields is required"))
        await _upload_presigned_url_fail(s, url, {}, fl, ValueError("fields is required"))
        await _upload_presigned_url_fail(s, url, dctp(flds, "key"), fl, ValueError(
            "fields missing required 'key' field"))
        await _upload_presigned_url_fail(s, url, dctp(flds, "signature"), fl, ValueError(
            "fields missing required 'signature' field"))
        await _upload_presigned_url_fail(s, url, dctp(flds, "policy"), fl, ValueError(
            "fields missing required 'policy' field"))
        await _upload_presigned_url_fail(s, url, dctp(flds, "AWSAccessKeyId"), fl, ValueError(
            "fields missing required 'AWSAccessKeyId' field"))
        await _upload_presigned_url_fail(s, url, dctws(flds, "key"), fl, ValueError(
            "fields missing required 'key' field"))
        await _upload_presigned_url_fail(s, url, dctws(flds, "signature"), fl, ValueError(
            "fields missing required 'signature' field"))
        await _upload_presigned_url_fail(s, url, dctws(flds, "policy"), fl, ValueError(
            "fields missing required 'policy' field"))
        await _upload_presigned_url_fail(s, url, dctws(flds, "AWSAccessKeyId"), fl, ValueError(
            "fields missing required 'AWSAccessKeyId' field"))
        await _upload_presigned_url_fail(s, url, flds, None, ValueError(
            "infile must exist and be a file"))
        await _upload_presigned_url_fail(s, url, flds, TESTDATA, ValueError(
            "infile must exist and be a file"))
        await _upload_presigned_url_fail(
            s, url, flds, fl,
            RemoteTimeoutError(f"Timeout uploading from file {fl} with timeout 0.001s"),
            timeout_sec=0.001)


def dctp(dic, key):
    return {k: v for k, v in dic.items() if k != key}


def dctws(dic, key):
    return {k: v if not k == key else "  \t   " for k, v in dic.items()}


async def _upload_presigned_url_fail(sess, url, fields, infile, expected, timeout_sec=600):
    with pytest.raises(Exception) as got:
        await upload_presigned_url(sess, url, fields, infile, timeout_sec=timeout_sec)
    assert_exception_correct(got.value, expected)


@pytest.mark.asyncio
async def test_upload_presigned_url_fail_bad_crc(minio):
    starts_with = (f"POST URL: http://localhost:{minio.port}/test-bucket Key: bar/myfilecrc 400"
                   + "\nError:\n")
    contains = ("<Error><Code>XAmzContentChecksumMismatch</Code><Message>The provided "
                + "&#39;x-amz-checksum&#39; header does not match what was computed.</Message>"
                + "<BucketName>test-bucket</BucketName><Resource>/test-bucket</Resource>"
                + "<RequestId>"
    )
    
    s3c = await _client(minio)
    url = (await s3c.presign_post_urls(
        S3Paths(["test-bucket/bar/myfilecrc"]), ["uIE6WiBPRpX="] # Actual checksum: uIE6WiBPRps=
    ))[0]
    
    await _upload_presigned_url_fail_s3_error(minio, url.url, url.fields, starts_with, contains)


@pytest.mark.asyncio
async def test_upload_presigned_url_fail_no_bucket(minio):
    starts_with = (f"POST URL: http://localhost:{minio.port}/fake-bucket Key: bar/myfilecrc 404"
                   + "\nError:\n")
    contains = ("<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist"
                + "</Message><BucketName>fake-bucket</BucketName><Resource>/fake-bucket"
                + "</Resource><RequestId>"
    )
    
    s3c = await _client(minio)
    url = (await s3c.presign_post_urls(S3Paths(["fake-bucket/bar/myfilecrc"])))[0]
    
    await _upload_presigned_url_fail_s3_error(minio, url.url, url.fields, starts_with, contains)


async def _upload_presigned_url_fail_s3_error(minio, url, fields, starts_with, contains):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    with pytest.raises(Exception) as got:
        async with aiohttp.ClientSession() as sess:
            await upload_presigned_url(sess, url, fields, TEST_RAND10KB)
    errmsg = str(got.value)
    assert errmsg.startswith(starts_with)
    assert contains in errmsg
    assert type(got.value) == TransferError


async def _client(minio):
    return await S3Client.create(minio.host,  minio.access_key, minio.secret_key)
