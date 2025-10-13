"""
Configuration for the event handler.
"""

from pydantic import Field
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
    cts_token: Annotated[str, Field(
        validation_alias="TOKEN",
        description="A CDM task service token providing the external executor role.",
        min_length=1,
    )]
    # TODO implement when downloading files
    # s3_url: Annotated[str, Field(
    #     validation_alias="S3_URL",
    #     examples=["https://minio.kbase.us"],
    #     description="The root URL of the S3 instance for storing data.",
    #     min_length=1,
    # )]
    # s3_access_key: Annotated[str, Field(
    #     validation_alias="S3_ACCESS",
    #     examples=["my_minio_user"],
    #     description="The S3 access key.",
    #     min_length=1,
    # )]
    s3_access_secret: Annotated[str, Field(
        validation_alias="S3_SECRET",
        examples=["supersekrit"],
        description="The S3 access secret.",
        min_length=1,
    )]
    
    _SAFE_FIELDS = {
        "job_id",
        "container_number",
        "cts_url",
        "s3_url",
        "s3_access_key",
    }
    
    def safe_dump(self) -> dict[str, Any]:
        """
        Return the settings as a dictionary with any sensitive fields (passwords, etc.) redacted.
        """
        return {
            k: v if k in self._SAFE_FIELDS else "REDACTED BY THE MINISTRY OF TRUTH"
            for k, v in self.model_dump().items()
        }
