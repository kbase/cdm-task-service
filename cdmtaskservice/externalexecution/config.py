"""
Configuration for the event handler.
"""

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Annotated, Any
from uuid import UUID


class Config(BaseSettings):
    """
    The configuration for the CDM events processor.
    """
    model_config = SettingsConfigDict(case_sensitive=True, str_strip_whitespace=True)
    
    
    job_id: Annotated[UUID, Field(
        validation_alias="JOB_ID",
        examples=["b9faffb2-453a-4ebe-9bba-1b96636cb3b1"],
        description="The job's ID.",
    )]
    container_number: Annotated[int, Field(
        validation_alias="CONTAINER_NUMBER",
        examples=[1],
        description="The container number within the job",
        ge=1,
    )]
    cts_url: Annotated[str, Field(
        validation_alias="SERVICE_ROOT_URL",
        examples=["https://ci.kbase.us/servies/cts"],
        description="The root URL of the CDM Task Service.",
        min_length=1,
    )]
    cts_token: Annotated[str | None, Field(
        validation_alias="TOKEN",
        description="A CDM task service token providing the external executor role. " 
            + "The token must be provided in the environment or a file."
            + "If both are provided the file takes precedence.",
        min_length=1,
    )] = None
    cts_token_path: Annotated[str | None, Field(
        validation_alias="TOKEN_PATH",
        examples=["/cts/token"],
        description=
            "A path to a file containing a CDM task service token providing the external "
            + "executor role. "
            + "The token must be provided in the environment or a file."
            + "If both are provided the file takes precedence.",
        min_length=1
    )] = None
    s3_url: Annotated[str, Field(
        validation_alias="S3_URL",
        examples=["https://minio.kbase.us"],
        description="The root URL of the S3 instance for storing data.",
        min_length=1,
    )]
    s3_access_key: Annotated[str, Field(
        validation_alias="S3_ACCESS_KEY",
        examples=["my_minio_user"],
        description="The S3 access key.",
        min_length=1,
    )]
    s3_access_secret: Annotated[str | None, Field(
        validation_alias="S3_SECRET",
        examples=["supersekrit"],
        description="The S3 access secret. "
            + "The secret must be provided in the the environment or a file."
            + "If both are provided the file takes precedence.",
        min_length=1,
    )] = None
    s3_access_secret_path: Annotated[str | None, Field(
        validation_alias="S3_SECRET_PATH",
        examples=["/cts/s3_secret"],
        description="A path to a file containing the S3 access secret. "
            + "The secret must be provided in the the environment or a file."
            + "If both are provided the file takes precedence.",
        min_length=1,
    )] = None
    s3_error_log_path: Annotated[str, Field(
        validation_alias="S3_ERROR_LOG_PATH",
        examples=["cts-bucket/logs"],
        description="The S3 path, including the bucket, where error log files should be stored.",
        min_length=1,
    )]
    s3_insecure: Annotated[bool, Field(
        validation_alias="S3_INSECURE",
        description=
            "Whether to skip checking the SSL certificate for the S3 instance, "
            + "leaving the service open to MITM attacks.",
    )] = False
    job_update_timeout_min: Annotated[int, Field(
        validation_alias="JOB_UPDATE_TIMEOUT_MIN",
        examples=[360],
        description="The timeout, in minutes, for when a job should stop trying to update "
            + "job state in the CTS and fail.",
        ge=1,
    )]
    mount_prefix_override: Annotated[str | None, Field(
        validation_alias="MOUNT_PREFIX_OVERRIDE",
        examples=["/var/lib/condor/execute:/home/user1/condor_stuff"],
        description="A host container mount path prefix override in the form " +
            "<prefix of path to replace>:<path to replace prefix with>",
    )] = None
    
    @model_validator(mode='after')
    def check_field_groups(self):
        if self.cts_token is None and self.cts_token_path is None:
            raise ValueError(
                "At least one of the 'TOKEN' or 'TOKEN_PATH' environment variables must be "
                + "provided"
        )
        if self.s3_access_secret is None and self.s3_access_secret_path is None:
            raise ValueError(
                "At least one of the 'S3_SECRET' or 'S3_SECRET_PATH' environment variables "
                + "must be provided"
        )
        return self
    
    _SAFE_FIELDS = {
        "job_id",
        "container_number",
        "cts_url",
        "cts_token_path",
        "s3_url",
        "s3_access_key",
        "s3_access_secret_path",
        "s3_error_log_path",
        "s3_insecure",
        "job_update_timeout_sec",
    }
    
    def safe_dump(self) -> dict[str, Any]:
        """
        Return the settings as a dictionary with any sensitive fields (passwords, etc.) redacted.
        """
        return {
            k: v if not v or k in self._SAFE_FIELDS else "REDACTED BY THE MINISTRY OF TRUTH"
            for k, v in self.model_dump().items()
        }

    def _get_secret(self, secret_file: str, secret: str):
        if secret_file:
            with open(secret_file) as f:
                return f.read().strip()
        return secret

    def get_cts_token(self):
        """ Return the CTS token. """
        return self._get_secret(self.cts_token_path, self.cts_token)

    def get_s3_access_secret(self):
        """ Return the S3 secret. """
        return self._get_secret(self.s3_access_secret_path, self.s3_access_secret)
