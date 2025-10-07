"""
Deals with serving local files via the API.
"""

from yarl import URL


CONDOR_EXE_PATH = "external_exec/files/condor/executable.sh"
"""
The path of the url where, by default, the HTCondor executable file can be found. This file
is run on the condor workers.
"""


CODE_ARCHIVE_PATH = "external_exec/files/code_archive.tgz"
"""
The path of the url where, by default, the code archive tgz can be found.
"""


def get_condor_exe_url(*, base_url: URL | None = None, override: URL | None = None) -> URL:
    """
    Get the full url for the HTCondor executable file to be run on workers.
    
    One of the following is required:
    base_url - the base url of the service. CONDOR_EXE_PATH is appended to this.
    override - if present, ignore the base_url and path and return the override url.
    """
    return _get_url(base_url, override, CONDOR_EXE_PATH)


def get_code_archive_url(*, base_url: URL | None = None, override: URL | None = None) -> URL:
    """
    Get the full url for the code archive tgz file to be run on workers.
    
    One of the following is required:
    base_url - the base url of the service. CODE_ARCHIVE_PATH is appended to this.
    override - if present, ignore the base_url and path and return the override url.
    """
    return _get_url(base_url, override, CODE_ARCHIVE_PATH)


def _get_url(base_url, override, path):
    if override:
        return override
    if not base_url:
        raise ValueError("Either base_url or override is required")
    return base_url / path
