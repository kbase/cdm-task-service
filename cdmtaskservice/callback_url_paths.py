"""
A module for determining paths for callback URLs for the service.
"""

import os

from cdmtaskservice import models

_CALLBACK = "callback"
_DOWNLOAD_COMPLETE = "download"
_JOB_COMPLETE = "job"
_UPLOAD_COMPLETE = "upload"
_ERROR_LOG_UPLOAD_COMPLETE = "errlogs"
_REFDATA_DOWNLOAD_COMPLETE ="refdata_download"


def get_download_complete_callback(root_url: str = None, job_id: str = None) -> str:
    """
    Get a url or path for a service callback to communicate that a download is complete.
    
    root_url - prepend the path with the given root url.
    job_id - suffix the path with a job ID.
    """
    return _get_callback(_DOWNLOAD_COMPLETE, root_url, job_id)


def get_job_complete_callback(root_url: str = None, job_id: str = None) -> str:
    """
    Get a url or path for a service callback to communicate that a remote job is complete.
    
    root_url - prepend the path with the given root url.
    job_id - suffix the path with a job ID.
    """
    return _get_callback(_JOB_COMPLETE, root_url, job_id)


def get_upload_complete_callback(root_url: str = None, job_id: str = None) -> str:
    """
    Get a url or path for a service callback to communicate that an upload is complete.
    
    root_url - prepend the path with the given root url.
    job_id - suffix the path with a job ID.
    """
    return _get_callback(_UPLOAD_COMPLETE, root_url, job_id)


def get_error_log_upload_complete_callback(root_url: str = None, job_id: str = None) -> str:
    """
    Get a url or path for a service callback to communicate that a log file upload for a job
    in an errored state is complete.
    
    root_url - prepend the path with the given root url.
    job_id - suffix the path with a job ID.
    """
    return _get_callback(_ERROR_LOG_UPLOAD_COMPLETE, root_url, job_id)


def get_refdata_download_complete_callback(
        root_url: str = None, refdata_id: str = None, cluster: models.Cluster = None
    ) -> str:
    """
    Get a url or path for a service callback to communicate that a reference data download
    is complete.
    
    root_url - prepend the path with the given root url.
    refdata_id - suffix the path with a reference data ID.
    cluster - suffix the path with the remote cluster.
    """
    return _get_callback(_REFDATA_DOWNLOAD_COMPLETE, root_url, refdata_id, cluster)


def _get_callback(
        subpath: str, root_url: str = None, entity_id: str = None, cluster: models.Cluster = None
    ) -> str:
    cb = [root_url] if root_url else []
    cb += [_CALLBACK, subpath]
    cb += [entity_id] if entity_id else []
    cb += [cluster.value] if cluster else []
    return os.path.join(*cb)
