"""
A module for determining paths for callback URLs for the service.
"""


_CALLBACK = "callback"
_DOWNLOAD_COMPLETE = "download"


def get_download_complete_callback(root_url: str = None, job_id: str = None):
    """
    Get a url or path for a service callback to communicate that a download is complete.
    
    root_url - prepend the path with the given root url.
    job_id - suffix the path with a job ID.
    """
    cb = [root_url] if root_url else []
    cb += [_CALLBACK, _DOWNLOAD_COMPLETE]
    cb += [job_id] if job_id else []
    return "/".join(cb)
