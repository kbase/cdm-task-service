"""
A module for determining paths for callback URLs for the service.
"""


_CALLBACK = "callback"
_DOWNLOAD_COMPLETE = "download"
_JOB_COMPLETE = "job"


def get_download_complete_callback(root_url: str = None, job_id: str = None):
    """
    Get a url or path for a service callback to communicate that a download is complete.
    
    root_url - prepend the path with the given root url.
    job_id - suffix the path with a job ID.
    """
    return _get_callback(_DOWNLOAD_COMPLETE, root_url, job_id)


def get_job_complete_callback(root_url: str = None, job_id: str = None):
    """
    Get a url or path for a service callback to communicate that a remote job is complete.
    
    root_url - prepend the path with the given root url.
    job_id - suffix the path with a job ID.
    """
    return _get_callback(_JOB_COMPLETE, root_url, job_id)


def _get_callback(subpath: str, root_url: str = None, job_id: str = None):
    cb = [root_url] if root_url else []
    cb += [_CALLBACK, subpath]
    cb += [job_id] if job_id else []
    return "/".join(cb)
