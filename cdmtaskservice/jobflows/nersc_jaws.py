"""
Manages running jobs at NERSC using the JAWS system.
"""

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.job_state import JobState
from cdmtaskservice.nersc.manager import NERSCManager
from cdmtaskservice.s3.client import S3Client

# Not sure how other flows would work and how much code they might share. For now just make
# this work and pull it apart / refactor later.


class NERSCJAWSRunner:
    """
    Runs jobs at NERSC using JAWS.
    """
    
    def __init__(
        self,
        nersc_manager: NERSCManager,
        job_state: JobState,
        s3_client: S3Client,
        s3_external_client: S3Client,
        jaws_token: str,
        jaws_group: str,
        service_root_url: str,
        s3_insecure_ssl: bool = False,
    ):
        """
        Create the runner.
        
        nersc_manager - the NERSC manager.
        job_state - the job state manager.
        s3_client - an S3 client pointed to the data stores.
        s3_external_client - an S3 client pointing to an external URL for the S3 data stores
            that may not be accessible from the current process, but is accessible to remote
            processes at NERSC.
        jaws_token - a token for the JGI JAWS system.
        jaws_group - the group to use for running JAWS jobs.
        service_root_url - the URL of the service root, used for constructing service callbacks.
        s3_insecure_url - whether to skip checking the SSL certificate for the S3 instance,
            leaving the service open to MITM attacks.
        """
        self._nman = _not_falsy(nersc_manager, "nersc_manager")
        self._jstate = _not_falsy(job_state, "job_state")
        self._s3 = _not_falsy(s3_client, "s3_client")
        self._s3ext = _not_falsy(s3_external_client, "s3_external_client")
        self._s3insecure = s3_insecure_ssl
        self._jtoken = _require_string(jaws_token, "jaws_token")
        self._jgroup = _require_string(jaws_group, "jaws_group")
        self._callback_root = _require_string(service_root_url, "service_root_url")
