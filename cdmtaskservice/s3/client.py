"""
An s3 client tailored for the needs of the CDM Task Service.

Note the client is *not threadsafe* as the underlying aiobotocore session is not threadsafe.
"""

import asyncio
from aiobotocore.session import get_session
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError, ClientError, HTTPClientError
from botocore.parsers import ResponseParserError
import logging
from typing import Any, Self

from cdmtaskservice.arg_checkers import (
    not_falsy as _not_falsy,
    check_num as _check_num,
    require_string as _require_string,
)
from cdmtaskservice.s3.paths import S3Paths, validate_bucket_name


_WRITE_TEST_FILENAME = (
    "cdm_task_service_write_test_with_random_stuff_to_avoid_conflicts_"
    + "jiaepjitgaihaiahgaqaijaeopatiaafaiezxvbmnxq"
)


class S3ObjectMeta:
    """ Metadata about an object in S3 storage. """
    
    __slots__ = ["path", "e_tag", "size", "crc64nvme"]
    
    def __init__(self, path: str, e_tag: str, size: int, crc64nvme: str | None = None):
        """
        Create the object meta. This is not expected to be instantiated manually.
        
        path - the path of the object in s3 storage, including the bucket.
        e-tag - the object e-tag.
        size - the total size of the object
        crc64nvme - the object's base64 encoded crc64nvme, if provided.
        """
        # input checking seems a little pointless. Could check that the path looks like a path
        # and the e-tag looks like an e-tag, but since this module should be creating this
        # class why bother. This is why access modifiers for methods are good
        self.path = path
        self.e_tag = e_tag
        self.crc64nvme = crc64nvme
        self.size = size
    
    @property
    def effective_part_size(self):
        # TODO CHECKSUMS remove - need to update nersc manager first
        return self.size


# This isn't technically S3 specific but leave it here since it's the only place it gets
# produced
class PresignedPost:
    """
    A presigned url and fields for posting data.
    
    For an S3 example, see
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html#generating-a-presigned-url-to-upload-a-file
    """
    
    __slots__ = ["url", "fields"]
    
    def __init__(self, url: str, fields: dict[str, str]):
        """
        Create the post details.
        
        url - the url for the post request.
        fields - the form fields to include with the post request.
        """
        # again input checking is a little pointless here
        self.url = url
        self.fields = fields


class S3Client:
    """
    The S3 client.
    
    Note the client is *not threadsafe* as the underlying aiobotocore session is not threadsafe.
    """
    
    @classmethod
    async def create(
        cls,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        config: dict[str, Any] = None,
        skip_connection_check: bool = False,
        insecure_ssl: bool = False,
    ) -> Self:
        """
        Create the client.
        
        endpoint_url - the URL of the s3 endpoint.
        access_key - the s3 access key.
        secret_key - the s3 secret key.
        config - Any client configuration options provided as a dictionary. See
            https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
        skip_connetion_check - don't try to list_buckets while creating the client to check
            the host and credentials are correct.
        insecure_ssl - skip ssl certificate checks.
        """
        s3c = S3Client(endpoint_url, access_key, secret_key, config, insecure_ssl)
        if not skip_connection_check:
            async def list_buckets(client):
                return await client.list_buckets()
            await s3c._run_commands([list_buckets], 1)
        return s3c
    
    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        config: dict[str, Any],
        insecure_ssl: bool,
    ):
        self._url = _require_string(endpoint_url, "endpoint_url")
        self._ak = _require_string(access_key, "access_key")
        self._sk = _require_string(secret_key, "secret_key")
        self._config = Config(**config) if config else None
        self._sess = get_session()
        self._insecure_ssl = insecure_ssl
    
    def _client(self):
        # Creating a client seems to be pretty cheap, usually < 20ms.
        return self._sess.create_client(
            "s3",
            endpoint_url=self._url,
            aws_access_key_id=self._ak,
            aws_secret_access_key=self._sk,
            config=self._config,
            verify=not self._insecure_ssl,
        )
        
    async def _fnc_wrapper(self, client, func):
        try:
            return await func(client)
        except EndpointConnectionError as e:
            raise S3ClientConnectError(f"s3 connect failed: {e}") from e
        except ResponseParserError as e:
            # TODO TEST logging
            # This is currently untested as I currently don't know a way to test it reliably
            # It used to be testable by GETing a text resource
            logging.getLogger(__name__).exception(f"Unable to parse response from S3:\n{e}\n")
            raise S3ClientConnectError(
                f"s3 response from the server at {self._url} was not parseable. "
                + "See logs for details"
            ) from e
        except HTTPClientError as e:
            # This is currently untested as I can't figure out a way to test it reliably
            # between GHA and my laptop. It can happen though
            # May need to add more explanation here based on the error text
            raise S3UnexpectedError(f"An unexpected HTTP error occurred: {e}") from e
        except ClientError as e:
            bucket = getattr(func, "bucket", None)
            write = getattr(func, "write", False)
            path = getattr(func, "path", None)
            code = e.response["Error"]["Code"]
            if code == "SignatureDoesNotMatch":
                raise S3ClientConnectError("s3 access credentials are invalid")
            if code == "404" or code == "NoSuchBucket":
                if bucket:
                    raise S3BucketNotFoundError(
                        f"The bucket '{bucket}' was not found on the s3 system"
                    )
                raise S3PathNotFoundError(
                    f"The path '{path}' was not found on the s3 system"
                ) from e
            if code == "AccessDenied" or code == "403":  # why both? Both 403s
                # may need to add other cases here
                if bucket:
                    op = "Write" if write else "Read"
                    raise S3BucketInaccessibleError(
                        f"{op} access denied to bucket '{bucket}' on the s3 system"
                    )
                elif path:
                    raise S3PathInaccessibleError(
                        f"Access denied to path '{path}' on the s3 system"
                    ) from e
                else:
                    raise S3ClientConnectError(
                        "Access denied to list buckets on the s3 system"
                    ) from e
            logging.getLogger(__name__).exception(
                f"Unexpected response from S3. Response data:\n{e.response}")
            raise S3UnexpectedError(f"Unexpected response from S3: {e}") from e
        
    async def _run_commands(self, async_client_callables, concurrency):
        # Look in test_manual for performance tests
        semaphore = asyncio.Semaphore(concurrency)
        async def sem_coro(coro):
            async with semaphore:
                return await coro
        results = []
        coros = []
        try: 
            async with self._client() as client:
                async with asyncio.TaskGroup() as tg:
                    for acall in async_client_callables:
                        coros.append(self._fnc_wrapper(client, acall))
                        results.append(tg.create_task(sem_coro(coros[-1])))
        except ValueError as e:
            raise S3ClientConnectError(f"s3 connect failed: {e}") from e
        except ExceptionGroup as eg:
            e = eg.exceptions[0]  # just pick one, essentially at random
            raise e from eg
        finally:
            # otherwise you can get coroutine never awaited warnings if a failure occurs
            for c in coros:
                c.close()
        return [r.result() for r in results]

    async def is_bucket_writeable(self, bucket: str):
        """
        Confirm a bucket exists and is writeable on the S3 system or throw an error otherwise.
        Writes a test file to the bucket.
        
        bucket - the name of the bucket.
        """
        # TODO TEST add tests around invalid bucket names
        bucket = validate_bucket_name(_require_string(bucket, "bucket"))
        async def write(client, buk=bucket):
            # apparently this is how you check write access to a bucket. Yuck
            await client.put_object(Bucket=buk, Key=_WRITE_TEST_FILENAME, Body="test")
            await client.delete_object(Bucket=bucket, Key=_WRITE_TEST_FILENAME)
        write.write = True
        write.bucket = bucket
        await self._run_commands([write], 1)

    async def get_object_meta(self, paths: S3Paths, concurrency: int = 10) -> list[S3ObjectMeta]:
        """
        Get metadata about a set of objects.
        
        paths - the paths to query
        concurrency - the number of simultaneous connections to S3
        """
        _not_falsy(paths, "paths")
        _check_num(concurrency, "concurrency")
        funcs = []
        for buk, key, path in paths.split_paths(include_full_path=True):
            async def head(client, buk=buk, key=key):  # bind the current value of the variables
                return await client.head_object(Bucket=buk, Key=key, ChecksumMode="ENABLED")
            head.path = path
            funcs.append(head)
        results = await self._run_commands(funcs, concurrency)
        ret = []
        for res, path in zip(results, paths.paths):
            ret.append(S3ObjectMeta(
                path=path,
                e_tag = res["ETag"].strip('"'),
                size=res["ContentLength"],
                crc64nvme=res.get("ChecksumCRC64NVME"),
            ))
        return ret

    async def presign_get_urls(self, paths: S3Paths, expiration_sec: int = 3600) -> list[str]:
        """
        Presign urls to allow getting s3 paths. Does not confirm the path exists.
        
        paths - the paths in question.
        expiration_sec - the expiration time of the urls.
        """
        _not_falsy(paths, "paths")
        _check_num(expiration_sec, "expiration_sec")
        results = []
        async with self._client() as client:
            for buk, key in paths.split_paths():
                results.append(await client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": buk, "Key": key},
                    ExpiresIn=expiration_sec
                ))
        return results
    
    
    async def presign_post_urls(
        self,
        paths: S3Paths,
        crc64nvmes: list[str] = None,
        expiration_sec: int = 3600
    ) -> list[PresignedPost]:
        """
        Presign urls to allow posting to s3 paths. Does not check for overwrites.
        
        paths - the paths in question.
        crc64nvmes - the base64 encoded CRC-64/NVME checksums of the files.
        expiration_sec - the expiration time of the urls.
        """
        _not_falsy(paths, "paths")
        if crc64nvmes and len(crc64nvmes) != len(paths):
            raise ValueError("If checksums are supplied, there must be one per path")
        _check_num(expiration_sec, "expiration_sec")
        crc64nvmes = crc64nvmes if crc64nvmes else [None] * len(paths)
        results = []
        async with self._client() as client:
            for (buk, key), crc in zip(paths.split_paths(), crc64nvmes):
                args = {"Bucket": buk, "Key": key, "ExpiresIn": expiration_sec}
                if crc:
                    args["Fields"] = {"x-amz-checksum-crc64nvme": crc}
                    args["Conditions"] = [{"x-amz-checksum-crc64nvme": crc}]
                ret = await client.generate_presigned_post(**args)
                results.append(PresignedPost(ret["url"], ret["fields"]))
        return results


class S3ClientConnectError(Exception):
    """ Error thrown when the S3 client could not connect to the server. """ 


class S3BucketNotFoundError(Exception):
    """ Error thrown when an S3 bucket does not exist on the server. """


class S3BucketInaccessibleError(Exception):
    """ Error thrown when an S3 bucket is not accessible to the user. """


class S3PathNotFoundError(Exception):
    """ Error thrown when an S3 path does not exist on the server. """


class S3PathInaccessibleError(Exception):
    """ Error thrown when an S3 path is not accessible to the user. """


class S3UnexpectedError(Exception):
    """ Error thrown an unexpected error occurs. """
