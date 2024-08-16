import pytest
import io

from cdmtaskservice.s3.client import S3Client, S3ClientConnectError, S3PathError
from conftest import minio, minio_unauthed_user, assert_exception_correct


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
    bad_ep2 = f"http://localhost:{minio.port + 1}"
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
    await minio.upload_file("test-bucket/test_file", b"abcdefghij")

    s3c = await client(minio)
    objm = await s3c.get_object_meta("test-bucket/test_file")
    check_obj_meta(
        objm, "test-bucket/test_file", "a925576942e94b2ef57a066101b48876", 10, None, False, 1)


@pytest.mark.asyncio
async def test_get_object_meta_multipart(minio):
    await minio.clean()  # couldn't get this to work as a fixture
    await minio.create_bucket("test-bucket")
    await minio.upload_file(
        "test-bucket/big_test_file", b"abcdefghij" * 6000000, 3, b"bigolfile")

    s3c = await client(minio)
    objm = await s3c.get_object_meta("test-bucket/big_test_file")
    check_obj_meta(
        objm,
        "test-bucket/big_test_file",
        "e0fcd4584a5157e2d465bf0217ab8268-4",
        180000009,
        60000000,
        True,
        4,
    )


@pytest.mark.asyncio
async def test_get_object_meta_fail_bad_path(minio):
    # Will probably want to refactor these tests so they can be generically be applied to
    # any endpoints that take a path
    await minio.clean()
    await minio.create_bucket("fail-bucket")
    await minio.upload_file("fail-bucket/foo/bar", b"foo")
    
    charerr = "Bucket names may only contain '-' and lowercase ascii alphanumerics: "
    
    testset = {
        None: "An s3 path cannot be null or a whitespace string",
        "   \t   ": "An s3 path cannot be null or a whitespace string",
         "  /  ": "path '/' must start with the s3 bucket and include a key",
         "foo /  ": "path 'foo /' must start with the s3 bucket and include a key",
        " / bar   ": "path '/ bar' must start with the s3 bucket and include a key",
        "il/foo": "Bucket names must be > 2 and < 64 characters: il",
        ("illegal-bu" * 6) + "cket/foo":
            f"Bucket names must be > 2 and < 64 characters: {'illegal-bu' * 6}cket",
        "illegal.bucket/foo": "Buckets with `.` in the name are unsupported: illegal.bucket",
        "-illegal-bucket/foo": "Bucket names cannot start or end with '-': -illegal-bucket",
        "illegal-bucket-/foo": "Bucket names cannot start or end with '-': illegal-bucket-",
        "illegal*bucket/foo": charerr + "illegal*bucket",
        "illegal_bucket/foo": charerr + "illegal_bucket",
        "illegal-Bucket/foo": charerr + "illegal-Bucket",
        "illegal-βucket/foo": charerr + "illegal-βucket",
        "fake-bucket/foo/bar": "The path 'fake-bucket/foo/bar' was not found on the s3 system",
        "fail-bucket/foo/baz": "The path 'fail-bucket/foo/baz' was not found on the s3 system",
    }
    for k, v in testset.items():
        await _get_object_meta_fail(await client(minio), k, S3PathError(v))


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
        s3c, "fail-bucket/foo/bar",
        S3PathError("Access denied to path 'fail-bucket/foo/bar' on the s3 system")
    )


async def _get_object_meta_fail(s3c, path, expected, print_stacktrace=False):
    with pytest.raises(Exception) as got:
        await s3c.get_object_meta(path)
    assert_exception_correct(got.value, expected, print_stacktrace)


async def client(minio):
    return await S3Client.create(minio.host,  minio.access_key, minio.secret_key)


def check_obj_meta(objm, path, e_tag, size, part_size, has_parts, num_parts):
    assert objm.path == path
    assert objm.e_tag == e_tag
    assert objm.size == size
    assert objm.part_size == part_size
    assert objm.has_parts is has_parts
    assert objm.num_parts == num_parts
