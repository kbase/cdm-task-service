"""
A configuration class for the service for S3 information, including clients.
"""

from pydantic import BaseModel, Field

from cdmtaskservice.s3.client import S3Client


class S3Config(BaseModel):
    """
    S3 configuration and client generation.
    """
    
    internal_url: str
    """ The URL of the S3 instance to use for data storage. """
    
    external_url: str
    """ The URL of the S3 instance accessible to external code or services. """
    
    access_key: str
    """ The S3 access key. """
    
    access_secret: str
    """ The S3 access secret. """
    
    error_log_path: str
    """ A path in S3 where error logs should be stored. """
    
    insecure: bool = False
    """
    Whether to skip checking the SSL certificate for the S3 instance,
    leaving the service open to MITM attacks.
    """
    
    verify_external_url: bool = True
    """ Whether to verify connectivity to the external S3 url at service startup. """

    # Internal singletons
    _s3_client: S3Client | None = None
    _s3_external_client: S3Client | None = None

    async def initialize_clients(self):
        """
        Initialize the S3 clients. NOTE: this method is not safe for concurrent access.
        
        Calling this method more than once has no effect.
        """
        if self._s3_client is None:
            self._s3_client = await S3Client.create(
                self.internal_url,
                self.access_key,
                self.access_secret,
                insecure_ssl=self.insecure
            )
        if self._s3_external_client is None:
            self._s3_external_client = await S3Client.create(
                self.external_url,
                self.access_key,
                self.access_secret,
                insecure_ssl=self.insecure,
                skip_connection_check=not self.verify_external_url
            )

    def get_internal_client(self) -> S3Client:
        """
        Get an S3 client pointed at the internal url.
        initialize_clients must have been called first.
        """
        if self._s3_client is None:
            raise ValueError("The clients have not yet been initialized")
        return self._s3_client
    
    def get_external_client(self) -> S3Client:
        """
        Get an S3 client pointed at the external url.
        initialize_clients must have been called first.
        """
        if self._s3_external_client is None:
            raise ValueError("The clients have not yet been initialized")
        return self._s3_external_client

