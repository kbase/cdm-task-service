import aiohttp
import os
from pathlib import Path
import pytest
import shutil
import tempfile
from unittest import mock

from conftest import assert_exception_correct, minio  # @UnusedImport
from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.s3.remote import (
    calculate_etag,
    crc32,
    download_presigned_url,
    upload_presigned_url,
    upload_presigned_url_with_crc32,
    FileCorruptionError,
    TransferError,
    FileChangeError,
    TimeoutError,
)
import config

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


def test_crc32():
    # checked these with the linux crc32 program
    testset = [
        (TEST_MT, "00000000"),
        (TEST_RAND1KB, "ed9a6eb3"),
        (TEST_RAND10KB, "4ffc5208"),
    ]
    for infile, crc in testset:
        gotcrc = crc32(infile)
        assert gotcrc.hex() == crc


def test_crc32_fail():
    testset = [
        (None, ValueError("infile must exist and be a file")),
        (TESTDATA, ValueError("infile must exist and be a file")),
    ]
    for infile, expected in testset:
        with pytest.raises(Exception) as got:
            crc32(infile)
        assert_exception_correct(got.value, expected)


@pytest.mark.asyncio
async def test_download_presigned_url(minio, temp_dir):
    await _test_download_presigned_url(minio, temp_dir, "temp1.txt", True)
    
    with open(temp_dir / "somedir" / "temp2.txt", "w") as f:
        f.write("foo")
    await _test_download_presigned_url(minio, temp_dir, "temp2.txt", True)
    
    with open(temp_dir / "somedir" / "temp3.txt", "w") as f:
        f.write("abcdefghij")
    await _test_download_presigned_url(minio, temp_dir, "temp3.txt", False)


async def _test_download_presigned_url(minio, temp_dir, filename, expected_return):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    await minio.upload_file("test-bucket/myfile", b"abcdefghij")
    
    s3c = await _client(minio)
    url = (await s3c.presign_get_urls(S3Paths(["test-bucket/myfile"])))[0]
    output = temp_dir / "somedir" / filename  # test directory creation for the first case
    async with aiohttp.ClientSession() as sess:
        res = await download_presigned_url(
            sess, url, 10, output, etag="a925576942e94b2ef57a066101b48876", timeout_sec=5)
    assert res == expected_return
    with open(output) as f:
        assert f.read() == "abcdefghij"


@pytest.mark.asyncio
async def test_download_presigned_url_multipart_and_insecure_ssl(minio, temp_dir):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("nice-bucket")
    await minio.upload_file(
        "nice-bucket/big_test_file", b"abcdefghij" * 600000, 4, b"bigolfile")

    s3c = await _client(minio)
    url = (await s3c.presign_get_urls(S3Paths(["nice-bucket/big_test_file"])))[0]
    output = temp_dir / "temp2.txt"
    async with aiohttp.ClientSession() as sess:
        # There's not a lot to test with insecure ssl other than it doesn't break things
        # Unless we want to get really crazy and set up Minio with a SSC in the tests. We don't
        res = await download_presigned_url(sess, url, 6000000, output, insecure_ssl=True)
    assert res is True
    with open(output) as f:
        assert f.read() == "abcdefghij" * 600000 * 4 + "bigolfile"


@pytest.mark.asyncio
async def test_download_presigned_url_fail_bad_args(minio, temp_dir):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    await minio.upload_file("test-bucket/myfile", b"abcdefghij")
    ps = 10
    o = temp_dir / "fail.txt"
    
    s3c = await _client(minio)
    url = (await s3c.presign_get_urls(S3Paths(["test-bucket/myfile"])))[0]
    async with aiohttp.ClientSession() as s:
        await _download_presigned_url_fail(None, url, ps, o, ValueError("session is required"))
        await _download_presigned_url_fail(s, None, ps, o, ValueError("url is required"))
        await _download_presigned_url_fail(s, "  \t   ", ps, o, ValueError("url is required"))
        await _download_presigned_url_fail(s, url, 0, o, ValueError("partsize must be > 0"))
        await _download_presigned_url_fail(s, url, 10, o, FileChangeError(
            f"Etag check failed for url http://localhost:{minio.port}/test-bucket/myfile. "
            + "Server provided a925576942e94b2ef57a066101b48876, "
            + "user provided Etag requirement is foo"),
            etag="foo"
        )
        await _download_presigned_url_fail(s, url, 3, o, FileCorruptionError(
            f"Etag check failed for url http://localhost:{minio.port}/test-bucket/myfile. "
            + "Server provided a925576942e94b2ef57a066101b48876, "
            + "file Etag is 1543089f5b20740cc5713f0437fcea8c-4"))
        await _download_presigned_url_fail(
            s, url, ps, None, ValueError("outputpath is required"))
        await _download_presigned_url_fail(
            s, url, 10, o, TimeoutError(f"Timeout downloading to file {o} with timeout 1e-05s"),
            timeout_sec=0.00001
        )
    
    
async def _download_presigned_url_fail(
    sess, url, partsize, output, expected, etag=None, timeout_sec=600
):
    with pytest.raises(Exception) as got:
        await download_presigned_url(
            sess, url, partsize, output, etag=etag, timeout_sec=timeout_sec)
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
    urlcrc = (await s3c.presign_post_urls(S3Paths(["test-bucket/bar/myfilecrc"])))[0]
    urlcrc.fields["x-amz-checksum-crc32"] = "T/xSCA=="
    async with aiohttp.ClientSession() as sess:
        await upload_presigned_url(sess, url.url, url.fields, TEST_RAND10KB, timeout_sec=5)
        await upload_presigned_url(sess, urlcrc.url, urlcrc.fields, TEST_RAND10KB)
    with open(TEST_RAND10KB, "rb") as f:
        expectedfile = f.read()
    objdata = await minio.get_object("test-bucket", "foo/myfile")
    objdatacrc = await minio.get_object("test-bucket", "bar/myfilecrc")
    # It seems that the ChecksumCRC32 field is not returned from Minio as documented in S3
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
    
    for o in (objdata, objdatacrc):
        body = await o["Body"].read()
        assert body == expectedfile
        assert o["ETag"] == '"3291fbb392f6fad06dbf331dfb74da81"'


@pytest.mark.asyncio
async def test_upload_presigned_url_with_crc_and_insecure_ssl(minio):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    
    s3c = await _client(minio)
    url = (await s3c.presign_post_urls(S3Paths(["test-bucket/foo/myfile"])))[0]
    async with aiohttp.ClientSession() as sess:
        # There's no a lot to test with insecure ssl other than it doesn't break things
        # Unless we want to get really crazy and set up Minio with a SSC in the tests. We don't
        await upload_presigned_url_with_crc32(
            sess, url.url, url.fields, TEST_RAND10KB, insecure_ssl=True, timeout_sec=5)
    with open(TEST_RAND10KB, "rb") as f:
        expectedfile = f.read()
    objdata = await minio.get_object("test-bucket", "foo/myfile")
    # It seems that the ChecksumCRC32 field is not returned from Minio as documented in S3
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
    
    body = await objdata["Body"].read()
    assert body == expectedfile
    assert objdata["ETag"] == '"3291fbb392f6fad06dbf331dfb74da81"'


@pytest.mark.asyncio
async def test_upload_presigned_url_with_crc_internals(minio):
    # this test just checks that the crc is being sent to the regular upload method
    # correctly, since it's invisible otherwise. There's no way to submit an incorrect CRC
    # and check that it fails, for example.
    # TODO TEST if Minio ever returns the CRC in a get_object just check that.
    s3c = await _client(minio)
    url = (await s3c.presign_post_urls(S3Paths(["test-bucket/foo/myfile"])))[0]
    with mock.patch("cdmtaskservice.s3.remote.upload_presigned_url") as method:
        async with aiohttp.ClientSession() as sess:
            await upload_presigned_url_with_crc32(sess, url.url, url.fields, TEST_RAND10KB)
            fields = dict(url.fields)
            fields["x-amz-checksum-crc32"] = "T/xSCA=="
        method.assert_called_once_with(
            sess, url.url, fields, TEST_RAND10KB, insecure_ssl=False, timeout_sec=600)


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
            TimeoutError(f"Timeout uploading from file {fl} with timeout 0.001s"),
            timeout_sec=0.001)


def dctp(dic, key):
    return {k: v for k, v in dic.items() if k != key}


def dctws(dic, key):
    return {k: v if not k == key else "  \t   " for k, v in dic.items()}


async def _upload_presigned_url_fail(sess, url, fields, infile, expected, timeout_sec=600):
    with pytest.raises(Exception) as got:
        await upload_presigned_url(sess, url, fields, infile, timeout_sec=timeout_sec)
    assert_exception_correct(got.value, expected)
    with pytest.raises(Exception) as got:
        await upload_presigned_url_with_crc32(sess, url, fields, infile, timeout_sec=timeout_sec)
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
    url = (await s3c.presign_post_urls(S3Paths(["test-bucket/bar/myfilecrc"])))[0]
    url.fields["x-amz-checksum-crc32"] = "T/xSCX=="
    
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
    
    await _upload_presigned_url_fail_s3_error(
        minio, url.url, url.fields, starts_with, contains, test_crc=True)


async def _upload_presigned_url_fail_s3_error(
    minio, url, fields, starts_with, contains, test_crc=False
):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    meths = [upload_presigned_url]
    if test_crc:
        meths.append(upload_presigned_url_with_crc32)
    for method in meths:
        with pytest.raises(Exception) as got:
            async with aiohttp.ClientSession() as sess:
                await method(sess, url, fields, TEST_RAND10KB)
        errmsg = str(got.value)
        assert errmsg.startswith(starts_with)
        assert contains in errmsg
        assert type(got.value) == TransferError


async def _client(minio):
    return await S3Client.create(minio.host,  minio.access_key, minio.secret_key)
