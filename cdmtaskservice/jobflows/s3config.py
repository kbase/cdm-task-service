"""
S3 configuration items for job flows.
"""

from dataclasses import dataclass

from cdmtaskservice.s3.client import S3Client


@dataclass(frozen=True)
class S3Config:
    """ S3 configuration information for job flows. """
    
    # TODO CODE check args aren't None / whitespace only. Not a lib class so YAGNI for now
    
    client: S3Client
    """ An S3 client pointing to an internal endpoint for the S3 storage instance. """
    
    external_client: S3Client
    """
    An S3 client pointing to an external endpoint for the S3 storage instance. This is
    typically visible to external compute sites and is protected by IP restrictions. It
    may not be available to the running process  Access may be slower due to multiple layers
    of protection (e.g. CloudFlare).
    """
    
    error_log_path: str
    """ A path in S3 where error logs should be stored. """
    
    insecure: bool = False
    """
    Whether to skip checking the SSL certificate for the S3 instance,
    leaving the service open to MITM attacks.
    """
