"""
Configuration for the CTS HTCondor client.
"""

from pydantic import BaseModel, Field
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
