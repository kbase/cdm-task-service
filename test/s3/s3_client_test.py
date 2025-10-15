"""
Also look in test_manual for more tests.
"""

import json
import random
from pathlib import Path
import pytest

from cdmtaskservice.s3.client import (
    S3Client,
    S3ClientConnectError,
    S3BucketInaccessibleError,
    S3BucketNotFoundError,
    S3PathInaccessibleError,
    S3PathNotFoundError,
    S3UnexpectedError,
)
from cdmtaskservice.s3.paths import S3Paths
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
        "https://ci.kbase.us/services/ws/docs/_sources/knownuserbugs.rst.txt", "foo", "bar",
        S3UnexpectedError(
            "Unexpected response from S3: An error occurred (500) when calling the ListBuckets "
            + "operation (reached max retries: 4): Internal Server Error"
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
async def test_is_bucket_writeable(minio):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    s3c = await _client(minio)
    # should pass without exception
    await s3c.is_bucket_writeable("test-bucket")

# TODO TEST add tests for checking bucket format (None input etc.). For now just test functionality


@pytest.mark.asyncio
async def test_is_bucket_writeable_fail_no_bucket(minio):
    await minio.clean()
    await minio.create_bucket("fail-bucket")
    s3c = await _client(minio)
    await _is_bucket_writeable_fail(s3c, "succeed-bucket", S3BucketNotFoundError(
        "The bucket 'succeed-bucket' was not found on the s3 system")
    )


@pytest.mark.asyncio
async def test_is_bucket_writeable_fail_unauthed(minio, minio_unauthed_user):
    await minio.clean()
    await minio.create_bucket("fail-bucket")
    
    user, pwd = minio_unauthed_user
    s3c = await S3Client.create(minio.host, user, pwd, skip_connection_check=True)
    await _is_bucket_writeable_fail(s3c, "fail-bucket", S3BucketInaccessibleError(
        "Write access denied to bucket 'fail-bucket' on the s3 system"))


@pytest.mark.asyncio
async def test_is_bucket_writeable_readonly(minio, minio_unauthed_user):
    await minio.clean()
    await minio.create_bucket("fail-bucket-readonly")
    await minio.upload_file("fail-bucket-readonly/test_file", b"abcdefghij")
    user, pwd = minio_unauthed_user
    
    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    # Test will fail for unraised exception if these are uncommented
                    #"s3:PutObject",
                    #"s3:DeleteObject",
                ],
                "Resource": f"arn:aws:s3:::fail-bucket-readonly/*"
            }
        ]
    }
    polname = "s3_client_test_readonly"
    # add some crap to the filename to prevent clobbers
    polfile = Path("./temp_s3_client_test_policy_file_ipjiajgieajjaptya.json")
    try:
        with open(polfile, "w") as tf:
            json.dump(policy, tf, indent=4)
        # file must be closed or mc complains about missing content length headers
        minio.run_mc("admin", "policy", "create", minio.mc_alias, polname, tf.name)
        minio.run_mc("admin", "policy", "attach", minio.mc_alias, polname, "--user", user)
    
        s3c = await S3Client.create(minio.host, user, pwd, skip_connection_check=True)
        # check that reading works
        await s3c.get_object_meta(S3Paths(["fail-bucket-readonly/test_file"]))
    
        await _is_bucket_writeable_fail(s3c, "fail-bucket-readonly", S3BucketInaccessibleError(
            "Write access denied to bucket 'fail-bucket-readonly' on the s3 system"),
        )
    finally:
        minio.run_mc("admin", "policy", "detach", minio.mc_alias, polname, "--user", user)
        minio.run_mc("admin", "policy", "rm", minio.mc_alias, polname)
        polfile.unlink(missing_ok=True)


async def _is_bucket_writeable_fail(s3c, bucket, expected, print_stacktrace=False):
    with pytest.raises(Exception) as got:
        await s3c.is_bucket_writeable(bucket)
    assert_exception_correct(got.value, expected, print_stacktrace)


@pytest.mark.asyncio
async def test_get_object_meta_single_part_w_crc64nvme(minio):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    # test that leading /s in key are ignored
    await minio.upload_file("test-bucket///test_file", b"abcdefghij", crc64nvme="e/Vz6rUQ/+o=")

    s3c = await _client(minio)
    objm = await s3c.get_object_meta(S3Paths(["test-bucket/test_file"]))
    assert len(objm) == 1
    _check_obj_meta(
        objm[0],
        "test-bucket/test_file",
        "a925576942e94b2ef57a066101b48876",
        10,
        "e/Vz6rUQ/+o=",
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
        None,
    )


@pytest.mark.asyncio
async def test_get_object_meta_mix_w_crc64nvme(minio):
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
        "B/mN7A/vO4c=",
    )
    _check_obj_meta(
        objm[1],
        "nice-bucket/test_file",
        "a925576942e94b2ef57a066101b48876",
        10,
        None,
    )


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
        await _get_object_meta_fail(await _client(minio), S3Paths([k]), S3PathNotFoundError(v))


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
        S3PathInaccessibleError(
            "Read access denied to path 'fail-bucket/foo/bar' on the s3 system"
        )
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
            S3PathNotFoundError(
                f"The path 'fail-bucket/f1000{fails_on}fail' was not found on the s3 system"),
            concurrency=con
        )


async def _get_object_meta_fail(s3c, paths, expected, concurrency=1, print_stacktrace=False):
    with pytest.raises(Exception) as got:
        await s3c.get_object_meta(paths, concurrency)
    assert_exception_correct(got.value, expected, print_stacktrace)


@pytest.mark.asyncio
async def test_download_object(minio, tmp_path):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    await minio.upload_file("test-bucket//test_file", b"imsounique")
    await minio.upload_file(
        "test-bucket/big_test_file",
        b"abcdefghij" * 600000,
        2,  # 12MB total
    )

    s3c = await _client(minio)
    await s3c.download_objects_to_file(
        S3Paths(["test-bucket/test_file", "test-bucket/big_test_file"]),
        [tmp_path / "somedir" / "tf1", tmp_path / "otherdir" / "tf2"]
    )
    with open(tmp_path / "somedir" / "tf1", mode="rb") as f:
        assert f.read() == b"imsounique"
    with open(tmp_path / "otherdir" / "tf2", mode="rb") as f:
        assert f.read() == b"abcdefghij" * 600000 * 2


@pytest.mark.asyncio
async def test_download_object_to_file_fail_bad_paths(minio):
    cli = await _client(minio)
    lp = [Path("foo")]
    s3p = S3Paths(["foo/bar"])
    await _download_object_to_file_fail(cli, None, lp, ValueError("s3paths is required"))
    await _download_object_to_file_fail(cli, s3p, None, ValueError("local_paths is required"))
    await _download_object_to_file_fail(cli, s3p, [], ValueError("local_paths is required"))
    await _download_object_to_file_fail(
        cli, s3p, ["f", "t"], ValueError(
            "The number of local paths must equal the number of S3 paths"
        )
    )


@pytest.mark.asyncio
async def test_download_object_to_file_fail_concurrency(minio):
    p = S3Paths(["foo/bar"])
    lp = [Path("foo")]
    cli = await _client(minio)
    for c in [0, -1, -1000000]:
        await _download_object_to_file_fail(
            cli, p, lp, ValueError("concurrency must be >= 1"), concurrency=c)


@pytest.mark.asyncio
async def test_download_object_to_file_fail_no_object(minio, tmp_path):
    await minio.clean()
    await minio.create_bucket("fail-bucket")
    await minio.upload_file("fail-bucket/foo/bar", b"foo")
    
    testset = {
        "fake-bucket/foo/bar": "The path 'fake-bucket/foo/bar' was not found on the s3 system",
        "fail-bucket/foo/baz": "The path 'fail-bucket/foo/baz' was not found on the s3 system",
    }
    cli = await _client(minio)
    lp = [tmp_path / "foo"]
    for k, v in testset.items():
        await _download_object_to_file_fail(cli, S3Paths([k]), lp, S3PathNotFoundError(v))


@pytest.mark.asyncio
async def test_download_object_to_file_fail_unauthed(minio, minio_unauthed_user, tmp_path):
    # Will probably want to refactor these tests so they can be generically be applied to
    # any endpoint
    await minio.clean()
    await minio.create_bucket("fail-bucket")
    await minio.upload_file("fail-bucket/foo/bar", b"foo")
    
    user, pwd = minio_unauthed_user
    s3c = await S3Client.create(minio.host, user, pwd, skip_connection_check=True)
    await _download_object_to_file_fail(
        s3c, S3Paths(["fail-bucket/foo/bar"]), [tmp_path / "foo"],
        S3PathInaccessibleError(
            "Read access denied to path 'fail-bucket/foo/bar' on the s3 system"
        )
    )


@pytest.mark.asyncio
async def test_download_object_to_file_fail_concurrent_paths(minio, tmp_path):
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
        await _download_object_to_file_fail(
            await _client(minio),
            S3Paths(paths),
            [tmp_path / "foo"] * 10,
            S3PathNotFoundError(
                f"The path 'fail-bucket/f1000{fails_on}fail' was not found on the s3 system"),
            concurrency=con
        )


async def _download_object_to_file_fail(
    s3c, paths, local_paths, expected, concurrency=1, print_stacktrace=False
):
    with pytest.raises(Exception) as got:
        await s3c.download_objects_to_file(paths, local_paths, concurrency)
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


@pytest.mark.asyncio
async def test_presign_post_urls_with_crc():
    s3c = await S3Client.create(
        "https://pubminio.kbase.us", "task-service", "complicated pwd", skip_connection_check=True)
    s3p = S3Paths(["bukkit/myfile", "otherbukkit/otherdir/somefile"])

    urls = await s3c.presign_post_urls(s3p, ["crc1", "fakecrc2"], expiration_sec=10 * 60)
    assert len(urls) == 2
    
    assert urls[0].url == "https://pubminio.kbase.us/bukkit"
    _presign_post_urls_check_fields(urls[0], "myfile", "crc1")
    
    assert urls[1].url == "https://pubminio.kbase.us/otherbukkit"
    _presign_post_urls_check_fields(urls[1], "otherdir/somefile", "fakecrc2")


def _presign_post_urls_check_fields(presign_post, key, crc=None):
    fields = dict(presign_post.fields)
    assert "signature" in fields
    del fields["signature"]  # changes per invocation
    assert "policy" in fields
    del fields["policy"]  # changes per invocation
    expfields = {"key": key, "AWSAccessKeyId": "task-service"}
    if crc:
        expfields["x-amz-checksum-crc64nvme"] = crc
    assert fields == expfields


@pytest.mark.asyncio
async def test_presign_post_urls_fail():
    s3c = await S3Client.create(
        "https://pubminio.kbase.us", "task-service", "complicated pwd", skip_connection_check=True)
    await _presign_get_post_fail(s3c, None, None, 1, "paths is required")
    for crc in [["fake", "fake", "fake"], ["fake"]]:
        await _presign_get_post_fail(
            s3c,
            S3Paths(["foo/bar", "baz/bat"]),
            crc,
            1,
            "If checksums are supplied, there must be one per path"
        )
    for t in [0, -1, -100, -1000000]:
        await _presign_get_post_fail(
            s3c, S3Paths(["foo/bar"]), None, t, "expiration_sec must be >= 1"
        )
    
    
async def _presign_get_post_fail(s3c, paths, checksums, expiration, expected):
    with pytest.raises(Exception) as got:
        await s3c.presign_post_urls(paths, crc64nvmes=checksums, expiration_sec=expiration)
    assert_exception_correct(got.value, ValueError(expected))


async def _client(minio, insecure_ssl=False):
    return await S3Client.create(
        minio.host,  minio.access_key, minio.secret_key, insecure_ssl=insecure_ssl)


def _check_obj_meta(objm, path, e_tag, size, crc64nvme):
    assert objm.path == path
    assert objm.e_tag == e_tag
    assert objm.size == size
    assert objm.crc64nvme == crc64nvme
