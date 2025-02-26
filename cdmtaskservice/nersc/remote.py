'''
Remote code to be run at NERSC.

In particular, non-standard lib dependency imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
'''

import asyncio
import hashlib
import json
import os
from pathlib import Path
import requests
import sys
import traceback
from typing import Any, Callable

from cdmtaskservice.jaws.output import parse_outputs_json, OUTPUTS_JSON_FILE
from cdmtaskservice.jaws.remote import parse_errors_json
from cdmtaskservice.s3.remote import (
    crc64nvme_b64,
    process_data_transfer_manifest as s3_pdtm,
)

# TODO TEST add tests for this file and its dependency functions


def calculate_checksums(jaws_output_dir: str, checksum_output_file: str):
    """
    Parse the JAWS output.json file and write CRC64/NVME checksums to a result file.
    """
    jdir = Path(jaws_output_dir)
    with open(jdir / OUTPUTS_JSON_FILE) as f:
        outs = parse_outputs_json(f)
    res = {"files": [], "stdouts": [], "stderrs": []}
    # TODO PERF may want to parallelize this with a max process limit
    for s3_path, result_path in outs.output_files.items():
        crc = crc64nvme_b64(jdir / result_path)
        res["files"].append({"crc64nvme": crc, "s3path": s3_path, "respath": result_path})
    for so in outs.stdout:
        crc = crc64nvme_b64(jdir / so)
        res["stdouts"].append({"crc64nvme": crc, "respath": so})
    for se in outs.stderr:
        crc = crc64nvme_b64(jdir / se)
        res["stderrs"].append({"crc64nvme": crc, "respath": se})
    with open(checksum_output_file, "w") as f:
        json.dump(res, f, indent=4)


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
        md5_json_file_path: str = None,
    ):
    """
    Processes a data transfer manifest file.
    
    manifest_file_path - the path to to the transfer manifest file.
    md5_json_file_path - an optional path to write a JSON mapping of file path to MD4 for the
        uploaded files. Only valid with an upload manifest.
    """
    # The manifest should be only used by the CDM task service and so we don't document
    # its structure.
    # Similarly, it should only be produced and consumed by the service, and so we don't
    # stress error checking too much.
    # Potential performance improvement could include a shared cross job cache for files
    #    only useful if jobs are reusing the same files, which seems def possible
    with open(manifest_file_path) as f:
        manifest = json.load(f)
    if md5_json_file_path:  # assume that this is only present for uploads
        _generate_md5s(md5_json_file_path, manifest["file-transfers"]["files"])
    asyncio.run(s3_pdtm(manifest["file-transfers"]))
    return None


def process_errorsjson(
        errorsjson_file_path: str,
        logfiles_directory: str,
        manifest_file_path: str,
    ):
    """
    Processes a JAWS errors.json file and uploads the resulting log files via a provided
    manifest.
    
    errorjson_file_path - the path to the JAWS errors.json file
    logfiles_directory - where to write the various log files extracted from the errors file
    manifest_file_path - an upload manifest for the log files
    """
    logfiles_directory = Path(logfiles_directory)
    logfiles_directory.mkdir(parents=True, exist_ok=True)
    with open(errorsjson_file_path) as f:
        ret = parse_errors_json(f, logfiles_directory)
    process_data_transfer_manifest(manifest_file_path)
    return ret


def _error_wrapper(func: Callable, args: list[str], result_file_path: str, callback_url: str):
    failed = False
    cts_env = {k: v for k, v in os.environ.items() if k.startswith("CTS_")}
    try:
        data = func(*args)
        with open(result_file_path, "w") as f:
            json.dump({"result": "success", "data": data, "cts_env": cts_env}, f, indent=4)
    except Exception as e:
        failed = True
        jext = traceback.format_exc()
        with open(result_file_path, "w") as f:
            j = {"result": "fail", "job_msg": str(e), "job_trace": jext, "cts_env": cts_env}
            json.dump(j, f, indent=4)
    if callback_url:
        cf = Path(result_file_path)
        callback_file = cf.parent / f"{cf.stem}.callback_error{cf.suffix}"
        try:
            # may want some retries here, halting on incorrect job state messages
            ret = requests.get(callback_url)
            if ret.status_code < 200 or ret.status_code > 299:
                failed = True
                # log callback errors for debugging purposes, service will never see this
                # since it's written post callback
                with open(callback_file, "w") as f:
                    j = {
                        "result": "fail",
                        "callback_text": ret.text,
                        "callback_code": ret.status_code,
                        "cts_env": cts_env
                    }
                    json.dump(j, f, indent=4)
        except Exception as e:
            failed = True
            with open(callback_file, "w") as f:
                j = {
                    "result": "fail",
                    "callback_msg": str(e),
                    "callback_trace": traceback.format_exc(),
                    "cts_env": cts_env,
                }
                json.dump(j, f, indent=4)
    if failed:
        sys.exit(1)


def main():
    mode = os.environ["CTS_MODE"]
    resfile = os.environ["CTS_RESULT_FILE_LOCATION"]
    callback_url = os.environ["CTS_CALLBACK_URL"]
    if mode == "manifest":
        _error_wrapper(
            process_data_transfer_manifest,
            # TODO CHECKSUM remove MD5 stuff
            [os.environ["CTS_MANIFEST_LOCATION"], os.environ.get("CTS_MD5_FILE_LOCATION")],
            resfile,
            callback_url
        )
    elif mode == "errorsjson":
        _error_wrapper(
            process_errorsjson,
            [
                os.environ["CTS_ERRORS_JSON_LOCATION"],
                os.environ["CTS_CONTAINER_LOGS_LOCATION"],
                os.environ["CTS_MANIFEST_LOCATION"],  # expected to be an upload manifest
            ],
            resfile,
            callback_url
        )
    elif mode == "checksum":
        _error_wrapper(
            calculate_checksums,
            [
                os.environ["CTS_JAWS_OUTPUT_DIR"],
                os.environ["CTS_CHECKSUM_FILE_LOCATION"],
            ],
            resfile,
            None,  # expected to be run by the manager as a blocking task for now 
        )
    else:  # Should never happen
        raise ValueError(f"unexpected mode: {mode}")


if __name__ == "__main__":
    main()
