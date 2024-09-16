'''
Remote code to be run at NERSC.

In particular, non-standard lib dependency imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
'''

import asyncio
import json
import os
from pathlib import Path

from cdmtaskservice.s3.remote import process_data_transfer_manifest as s3_pdtm


def process_data_transfer_manifest(manifest_file_path: str, touch_on_complete: str):
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
    # TODO TEST add tests for this and its dependency functions.
    try:
        with open(manifest_file_path) as f:
            manifest = json.load(f)
        asyncio.run(s3_pdtm(manifest["file-transfers"]))
    finally:
        Path(touch_on_complete).touch()


if __name__ == "__main__":
    process_data_transfer_manifest(
        os.environ["CTS_MANIFEST_LOCATION"], os.environ["CTS_TOUCH_ON_COMPLETE"])
