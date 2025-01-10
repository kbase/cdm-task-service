'''
Remote code to be run at NERSC.

In particular, non-standard lib dependency imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
'''

import asyncio
import hashlib
import json
import logging
import os
import requests
from typing import Any

from cdmtaskservice.s3.remote import process_data_transfer_manifest as s3_pdtm


def _generate_md5s(path: str, files: list[dict[str, Any]]):
    # Maybe could speed this up with parallelization or async? Probably disk limited
    # Test different approaches if it's taking a long time
    path2md5 = {}
    for f in files:
        with open(f["file"], "rb") as fi:
            path2md5[f["file"]] = hashlib.file_digest(fi, "md5").hexdigest()
    with open(path, "w") as f:
        json.dump(path2md5, f)


def process_data_transfer_manifest(
        manifest_file_path: str,
        callback_url: str,
        md5_json_file_path: str = None,
    ):
    """
    Processes an upload manifest file.
    
    manifest_file_path - the path to to the transfer manifest file.
    touch_on_complete - a path to a file to touch when the transfer is complete. Separate from
        the manifest to guard against load issues.
    """
    # The manifest should be only used by the CDM task service and so we don't document
    # its structure.
    # Similarly, it should only be produced and consumed by the service, and so we don't
    # stress error checking too much.
    # TODO TEST add tests for this and its dependency functions, including logging
    # Potential performance improvement could include a shared cross job cache for files
    #    only useful if jobs are reusing the same files, which seems def possible
    # TODO LOGGINNG logging doesn't work
    # TODO IMPORTANT ERRORHANDLING write an output file that can be read by the CTS
    #                sfapi tasks only last for 10 minutes after completions
    log = logging.getLogger(__name__)
    try:
        with open(manifest_file_path) as f:
            manifest = json.load(f)
        if md5_json_file_path:  # assume that this is only present for uploads
            _generate_md5s(md5_json_file_path, manifest["file-transfers"]["files"])
        asyncio.run(s3_pdtm(manifest["file-transfers"]))
    finally:
        log.info(f"Pinging callback url {callback_url}")
        ret = requests.get(callback_url)
        if ret.status_code < 200 or ret.status_code > 299:
            log.error(ret.text)
        ret.raise_for_status()


if __name__ == "__main__":
    process_data_transfer_manifest(
        os.environ["CTS_MANIFEST_LOCATION"],
        os.environ["CTS_CALLBACK_URL"],
        os.environ.get("CTS_MD5_FILE_LOCATION")
    )
