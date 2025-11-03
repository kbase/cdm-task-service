"""
Configuration for the CTS HTCondor client.
"""

from pydantic import BaseModel, Field
from typing import Annotated
from yarl import URL

from cdmtaskservice.localfiles import get_condor_exe_url, get_code_archive_url


class CondorClientConfig(BaseModel):
    """
    Configuration items for the HTCondor client.
    """
    
    initial_dir: str
    """
    Where the job logs should be stored on the condor scheduler host.
    The path must exist there and locally.
    """

    service_root_url: str
    """
    The URL of the service root, used by the remote job to get and update job state.
    """
    
    executable_url_override: str | None
    """
    A URL, if any, to use for downloading the HTCondor shell script rather than the
    default location.
    Must end in a file name and have no query or fragment.
    """
    
    code_archive_url_override: str | None
    """
    A URL, if any, to use for downloading the tgz code archive rather than the default location.
    Must end in a file name and have no query or fragment.
    """
    
    client_group: str | None
    """
    The client group to submit jobs to, if any. This is a classad on a worker with
    the name CLIENTGROUP.
    """
    
    token_path: str
    """
    The path on the condor worker containing a KBase token for use when
    contacting the service.
    """

    s3_access_secret_path: str
    """
    The path on the condor worker containing the s3 access secret for the S3 instance.
    """
    
    job_update_timeout_min: Annotated[int, Field(ge=1)]
    """
    The number of minutes to wait when trying to update the job state in the service before
    failing.
    """
    
    mount_prefix_override: str | None
    """
    A host container mount path prefix override in the form
    <prefix of path to replace>:<path to replace prefix with>
    """
    # TODO CODE add validator for mount prefix here and in the other 18 config classes
    
    additional_path: str | None
    """
    Additional path elements to prepend to the condor worker $PATH.
    """
    
    cache_dir: str
    """
    A directory appropriate for caching cross-job data such as uv dependencies.
    """

    def get_executable_url(self) -> str:
        """
        Get the URL where the external executor should download the shell executable for the job.
        """
        exe_ov = URL(self.executable_url_override) if self.executable_url_override else None
        return str(get_condor_exe_url(base_url=URL(self.service_root_url), override=exe_ov))
    
    def get_code_archive_url(self) -> str:
        """
        Get the URL where the external executor should download the CTS code archive.
        """
        code_ov = URL(self.code_archive_url_override) if self.code_archive_url_override else None
        return str(get_code_archive_url(base_url=URL(self.service_root_url), override=code_ov))
