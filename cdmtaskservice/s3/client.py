"""
An s3 client tailored for the needs of the CDM Task Service.

Note the client is *not threadsafe* as the underlying aiobotocore session is not threadsafe.
"""

from aiobotocore.session import get_session
from botocore.config import Config
from botocore.exceptions import EndpointConnectionError, ClientError
from botocore.parsers import ResponseParserError
import logging
from typing import Any, Self

from .exceptions import S3ClientConnectError, S3PathError, S3UnexpectedError
from .paths import S3Paths


class S3ObjectMeta:
    """ Metadata about an object in S3 storage. """
    
    __slots__ = ["path", "e_tag", "part_size", "size"]
    
    def __init__(self, path: str, e_tag: str, size: int, part_size: int):
        """
        Create the object meta. This is not expected to be instantiated manually.
        
        path - the path of the object in s3 storage, including the bucket.
        e-tag - the object e-tag.
        size - the total size of the object
        part_size - the part size used in a multipart upload, if relevant.
        """
        # input checking seems a little pointless. Could check that the path looks like a path
        # and the e-tag looks like an e-tag, but since this module should be creating this
        # class why bother. This is why access modifiers for methods are good
        self.path = path
        self.e_tag = e_tag
        self.size = size
        self.part_size = part_size
        # could check that the part size makes sense given the e-tag... meh
    
    @property
    def has_parts(self) -> bool:
        """ Returns true if the object was uploaded as multipart. """
        # MD5s have 32 characters. An e-tag with parts looks like `<MD5>-#parts`
        return len(self.e_tag) > 32

    @property
    def num_parts(self) -> int:
        """ 
        Returns the number of parts used in a multipart upload or 1 if the upload was not
        multipart.
        """
        if self.has_parts:
            return int(self.e_tag.split("-")[1])
        return 1


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
        skip_connection_check: bool = False
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
        """
        s3c = S3Client(endpoint_url, access_key, secret_key, config)
        if not skip_connection_check:
            async def list_buckets(client):
                return await client.list_buckets()
            await s3c._run_command(list_buckets)
        return s3c
    
    def __init__(
        self, endpoint_url: str, access_key: str, secret_key: str, config: dict[str, Any]
    ):
        self._url = self._require_string(endpoint_url, "endpoint_url")
        self._ak = self._require_string(access_key, "access_key")
        self._sk = self._require_string(secret_key, "secret_key")
        self._config = Config(**config) if config else None
        self._sess = get_session()
    
    def _require_string(self, string, name):
        if not string or not string.strip():
            raise ValueError(f"{name} is required")
        return string.strip()
    
    def _client(self):
        # Creating a client seems to be pretty cheap, usually < 20ms.
        return self._sess.create_client(
            "s3",
            endpoint_url=self._url,
            aws_access_key_id=self._ak,
            aws_secret_access_key=self._sk,
            config=self._config,
        )
        
    async def _run_command(self, async_client_callable, path=None):
        try: 
            async with self._client() as client:
                return await async_client_callable(client)
        except (ValueError, EndpointConnectionError) as e:
            raise S3ClientConnectError(f"s3 connect failed: {e}") from e
        except ResponseParserError as e:
            # TODO TEST logging
            # TODO LOGGING figure out how logging is going to work
            logging.getLogger(__name__).error(
                f"Unable to parse response from S3:\n{e}\n")
            raise S3ClientConnectError(
                f"s3 response from the server at {self._url} was not parseable. "
                + "See logs for details"
            ) from e
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "SignatureDoesNotMatch":
                raise S3ClientConnectError("s3 access credentials are invalid")
            if code == "404":
                raise S3PathError(f"The path '{path}' was not found on the s3 system") from e
            if code == "AccessDenied" or code == "403":  # why both? Both 403s
                if not path:
                    raise S3ClientConnectError(
                        "Access denied to list buckets on the s3 system") from e
                # may need to add other cases here
                raise S3PathError(f"Access denied to path '{path}' on the s3 system") from e
            # no way to test this since we're trying to cover all possible errors in tests
            logging.getLogger(__name__).error(
                f"Unexpected response from S3. Response data:\n{e.response}\nTraceback:\n{e}\n")
            raise S3UnexpectedError(str(e)) from e

    async def get_object_meta(self, paths: S3Paths) -> list[S3ObjectMeta]:
        """
        Get metadata about a set of objects.
        
        paths - the paths to query
        """
        if not paths:
            raise ValueError("paths is required")
        ret = []
        for buk, key, path in paths.split_paths(include_full_path=True):
            # TODO parallelize w/ max connection #
            # https://github.com/kbase/collections/blob/4c4a7f9b71e4d058799e8679b7c9710ab02bd5de/src/service/matchers/minhash_homology_matcher.py#L111-L120
            async def head(client):
                return await client.head_object(Bucket=buk, Key=key, PartNumber=1)
            res = await self._run_command(head, path=path)
            size = res["ContentLength"]
            part_size = None
            if "PartsCount" in res:
                part_size = size
                content_range = res["ResponseMetadata"]["HTTPHeaders"]["content-range"]
                size = int(content_range.split("/")[1])
            ret.append(S3ObjectMeta(
                path=path,
                e_tag = res["ETag"].strip('"'),
                size=size,
                part_size=part_size
            ))
        return ret

