"""
Also look in test_manual for more tests.
"""

import random
import pytest

from cdmtaskservice.s3.client import S3Client
from cdmtaskservice.s3.paths import S3Paths
from cdmtaskservice.s3.exceptions import S3ClientConnectError, S3PathError
from conftest import (
    minio,  # @UnusedImport
    minio_unauthed_user,  # @UnusedImport
    assert_exception_correct,
    assert_close_to_now_sec,
)


@pytest.mark.asyncio
async def test_create_fail_missing_args():
    u = "https://localhost:1234"
    await _create_fail(None, "foo", "bar", ValueError("endpoint_url is required"))
    await _create_fail("  \t   ", "foo", "bar", ValueError("endpoint_url is required"))
    await _create_fail(u, None, "bar", ValueError("access_key is required"))
    await _create_fail(u, "  \t   ", "bar", ValueError("access_key is required"))
    await _create_fail(u, "foo", None, ValueError("secret_key is required"))
    await _create_fail(u, "foo", "  \t   ", ValueError("secret_key is required"))


@pytest.mark.asyncio
async def test_create_fail_bad_args(minio, minio_unauthed_user):
    bad_ep1 = f"localhost:{minio.port}"
    await _create_fail(
        bad_ep1, "foo", "bar",
        S3ClientConnectError("s3 connect failed: Invalid endpoint: " + bad_ep1))
    # +1 can fail on some versions / environments if the UI port is automatically set to
    # the api port + 1
    bad_ep2 = f"http://localhost:{minio.port + 3}"
    await _create_fail(
        bad_ep2, "foo", "bar",
        S3ClientConnectError(
            f's3 connect failed: Could not connect to the endpoint URL: "{bad_ep2}/"'),
        {"connect_timeout": 0.2, "retries": {"total_max_attempts": 1}},
    )
    await _create_fail(
        "https://google.com", "foo", "bar",
        S3ClientConnectError(
            "s3 response from the server at https://google.com was not parseable. "
            + "See logs for details"
        ),
    )
    await _create_fail(
        f"http://localhost:{minio.port}", minio.access_key, "bar",
        S3ClientConnectError("s3 access credentials are invalid"))
    await _create_fail(
        f"http://localhost:{minio.port}", minio_unauthed_user[0], minio_unauthed_user[1],
        S3ClientConnectError("Access denied to list buckets on the s3 system"))


async def _create_fail(host, akey, skey, expected, config=None, print_stacktrace=False):
    with pytest.raises(Exception) as got:
        await S3Client.create(host, akey, skey, config)
    assert_exception_correct(got.value, expected, print_stacktrace)


@pytest.mark.asyncio
async def test_get_object_meta_single_part(minio):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    # test that leading /s in key are ignored
    await minio.upload_file("test-bucket///test_file", b"abcdefghij")

    s3c = await _client(minio)
    objm = await s3c.get_object_meta(S3Paths(["test-bucket/test_file"]))
    assert len(objm) == 1
    _check_obj_meta(
        objm[0],
        "test-bucket/test_file",
        "a925576942e94b2ef57a066101b48876",
        10,
        None,
        False,
        1,
        10
    )


@pytest.mark.asyncio
async def test_get_object_meta_multipart_and_insecure_ssl(minio):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    await minio.upload_file(
        "test-bucket/big_test_file", b"abcdefghij" * 600000, 3, b"bigolfile")

    # There's not a lot to test with insecure ssl other than it doesn't break things
    # Unless we want to get really crazy and set up Minio with a SSC in the tests. We don't
    s3c = await _client(minio, insecure_ssl=True)
    objm = await s3c.get_object_meta(S3Paths(["test-bucket/big_test_file"]))
    assert len(objm) == 1
    _check_obj_meta(
        objm[0],
        "test-bucket/big_test_file",
        "b8185adaf462a5ac2ca9db335b290d23-4",
        18000009,
        6000000,
        True,
        4,
        6000000,
    )

@pytest.mark.asyncio
async def test_get_object_meta_mix(minio):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("nice-bucket")
    await minio.upload_file(
        "nice-bucket/big_test_file", b"abcdefghij" * 600000, 4, b"bigolfile")
    await minio.upload_file("nice-bucket/test_file", b"abcdefghij")
    
    s3c = await _client(minio)
    objm = await s3c.get_object_meta(S3Paths(
        ["nice-bucket/big_test_file", "nice-bucket/test_file"]))
    assert len(objm) == 2
    _check_obj_meta(
        objm[0],
        "nice-bucket/big_test_file",
        "9728af2f2c566b2b944b96203769175d-5",
        24000009,
        6000000,
        True,
        5,
        6000000,
    )
    _check_obj_meta(
        objm[1],
        "nice-bucket/test_file",
        "a925576942e94b2ef57a066101b48876",
        10,
        None,
        False,
        1,
        10)


@pytest.mark.asyncio
async def test_get_object_meta_fail_no_paths(minio):
    await _get_object_meta_fail(await _client(minio), None, ValueError("paths is required"))


@pytest.mark.asyncio
async def test_get_object_meta_fail_concurrency(minio):
    p = S3Paths(["foo/bar"])
    cli = await _client(minio)
    for c in [0, -1, -1000000]:
        await _get_object_meta_fail(
            cli, p, ValueError("concurrency must be >= 1"), concurrency=c)
    

@pytest.mark.asyncio
async def test_get_object_meta_fail_no_object(minio):
    await minio.clean()
    await minio.create_bucket("fail-bucket")
    await minio.upload_file("fail-bucket/foo/bar", b"foo")
    
    testset = {
        "fake-bucket/foo/bar": "The path 'fake-bucket/foo/bar' was not found on the s3 system",
        "fail-bucket/foo/baz": "The path 'fail-bucket/foo/baz' was not found on the s3 system",
    }
    for k, v in testset.items():
        await _get_object_meta_fail(await _client(minio), S3Paths([k]), S3PathError(v))


@pytest.mark.asyncio
async def test_get_object_meta_fail_unauthed(minio, minio_unauthed_user):
    # Will probably want to refactor these tests so they can be generically be applied to
    # any endpoint
    await minio.clean()
    await minio.create_bucket("fail-bucket")
    await minio.upload_file("fail-bucket/foo/bar", b"foo")
    
    user, pwd = minio_unauthed_user
    s3c = await S3Client.create(minio.host, user, pwd, skip_connection_check=True)
    await _get_object_meta_fail(
        s3c, S3Paths(["fail-bucket/foo/bar"]),
        S3PathError("Access denied to path 'fail-bucket/foo/bar' on the s3 system")
    )


@pytest.mark.asyncio
async def test_get_object_meta_fail_concurrent_paths(minio):
    # Since a taskgroup cancels all tasks after the first failure, check that we're throwing
    # the right error and not a CancelledError or something
    await minio.clean()
    await minio.create_bucket("fail-bucket")
    paths = []
    filecount = 10
    fails_on = random.randrange(filecount)
    for i in range(filecount):
        contents = str(10000 + i)  # keep the file size the same
        await minio.upload_file(f"fail-bucket/f{contents}", contents.encode())
        paths.append(f"fail-bucket/f{contents}" + ("fail" if i == fails_on else ""))
    
    for con in [1, 2, 5, 10]:
        await _get_object_meta_fail(
            await _client(minio),
            S3Paths(paths),
            S3PathError(
                f"The path 'fail-bucket/f1000{fails_on}fail' was not found on the s3 system"),
            concurrency=con
        )


async def _get_object_meta_fail(s3c, paths, expected, concurrency=1, print_stacktrace=False):
    with pytest.raises(Exception) as got:
        await s3c.get_object_meta(paths, concurrency)
    assert_exception_correct(got.value, expected, print_stacktrace)


@pytest.mark.asyncio
async def test_presign_get_urls():
    s3c = await S3Client.create(
        "https://pubminio.kbase.us", "task-service", "complicated pwd", skip_connection_check=True)
    s3p = S3Paths(["bukkit/myfile", "otherbukkit/otherdir/somefile"])
    urls = await s3c.presign_get_urls(s3p, expiration_sec=10 * 60)
    assert len(urls) == 2

    assert urls[0].startswith("https://pubminio.kbase.us/bukkit/myfile?"
                              + "AWSAccessKeyId=task-service&Signature=")
    assert "&Expires=" in urls[0]
    exp = int(urls[0].split("=")[-1])
    assert_close_to_now_sec(exp - 600)

    assert urls[1].startswith("https://pubminio.kbase.us/otherbukkit/otherdir/somefile?"
                              + "AWSAccessKeyId=task-service&Signature=")
    assert "&Expires=" in urls[1]
    exp = int(urls[1].split("=")[-1])
    assert_close_to_now_sec(exp - 600)


@pytest.mark.asyncio
async def test_presign_get_urls_fail():
    s3c = await S3Client.create(
        "https://pubminio.kbase.us", "task-service", "complicated pwd", skip_connection_check=True)
    await _presign_get_urls_fail(s3c, None, 1, "paths is required")
    for t in [0, -1, -100, -1000000]:
        await _presign_get_urls_fail(s3c, S3Paths(["foo/bar"]), t, "expiration_sec must be >= 1")
    
    
async def _presign_get_urls_fail(s3c, paths, expiration, expected):
    with pytest.raises(Exception) as got:
        await s3c.presign_get_urls(paths, expiration)
    assert_exception_correct(got.value, ValueError(expected))


@pytest.mark.asyncio
async def test_presign_post_urls():
    s3c = await S3Client.create(
        "https://pubminio.kbase.us", "task-service", "complicated pwd", skip_connection_check=True)
    s3p = S3Paths(["bukkit/myfile", "otherbukkit/otherdir/somefile"])
    # not really a good way to test that expiration works without reverse engineering the
    # policy field I guess?
    urls = await s3c.presign_post_urls(s3p, expiration_sec=10 * 60)
    assert len(urls) == 2
    
    assert urls[0].url == "https://pubminio.kbase.us/bukkit"
    _presign_post_urls_check_fields(urls[0], "myfile")
    
    assert urls[1].url == "https://pubminio.kbase.us/otherbukkit"
    _presign_post_urls_check_fields(urls[1], "otherdir/somefile")


def _presign_post_urls_check_fields(presign_post, key):
    fields = dict(presign_post.fields)
    assert "signature" in fields
    del fields["signature"]  # changes per invocation
    assert "policy" in fields
    del fields["policy"]  # changes per invocation
    assert fields == {"key": key, "AWSAccessKeyId": "task-service"}


@pytest.mark.asyncio
async def test_presign_post_urls_fail():
    s3c = await S3Client.create(
        "https://pubminio.kbase.us", "task-service", "complicated pwd", skip_connection_check=True)
    await _presign_get_post_fail(s3c, None, 1, "paths is required")
    for t in [0, -1, -100, -1000000]:
        await _presign_get_post_fail(s3c, S3Paths(["foo/bar"]), t, "expiration_sec must be >= 1")
    
    
async def _presign_get_post_fail(s3c, paths, expiration, expected):
    with pytest.raises(Exception) as got:
        await s3c.presign_post_urls(paths, expiration)
    assert_exception_correct(got.value, ValueError(expected))


async def _client(minio, insecure_ssl=False):
    return await S3Client.create(
        minio.host,  minio.access_key, minio.secret_key, insecure_ssl=insecure_ssl)


def _check_obj_meta(objm, path, e_tag, size, part_size, has_parts, num_parts, effsize):
    assert objm.path == path
    assert objm.e_tag == e_tag
    assert objm.size == size
    assert objm.part_size == part_size
    assert objm.has_parts is has_parts
    assert objm.num_parts == num_parts
    assert objm.effective_part_size == effsize