'''
Remote code to be run at NERSC.

In particular, non-standard lib dependency imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
'''

import asyncio
import json
import logging
import os
from pathlib import Path
import requests
import subprocess
import sys
import traceback
from typing import Callable

from cdmtaskservice.jaws.remote import ( 
    parse_errors_json,
    parse_outputs_json,
    OUTPUTS_JSON_FILE,
)
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
    # TODO PERF could also chunk large files and combine CRCs via awscrt.checksums.combine_crc64nvme
    for s3_path, result_path in outs.output_files.items():
        fpath = jdir / result_path
        crc = crc64nvme_b64(fpath)
        res["files"].append({
            "crc64nvme": crc,
            "s3path": s3_path,
            "respath": result_path,
            "size": fpath.stat().st_size,
        })
    for so in outs.stdout:
        crc = crc64nvme_b64(jdir / so)
        res["stdouts"].append({"crc64nvme": crc, "respath": so})
    for se in outs.stderr:
        crc = crc64nvme_b64(jdir / se)
        res["stderrs"].append({"crc64nvme": crc, "respath": se})
    with open(checksum_output_file, "w") as f:
        json.dump(res, f, indent=4)


def process_data_transfer_manifest(manifest_file_path: str):
    """
    Processes a data transfer manifest file.
    
    manifest_file_path - the path to to the transfer manifest file.
    """
    # The manifest should be only used by the CDM task service and so we don't document
    # its structure.
    # Similarly, it should only be produced and consumed by the service, and so we don't
    # stress error checking too much.
    # Potential performance improvement could include a shared cross job cache for files
    #    only useful if jobs are reusing the same files, which seems def possible
    with open(manifest_file_path) as f:
        manifest = json.load(f)
    asyncio.run(s3_pdtm(manifest["file-transfers"]))
    return None


def sync_refdata_to_dtn(
    staging_dir: str,
    dtn_host: str,
    dest_dir: str,
    completion_file: str,
    completion_file_contents: str,
):
    """
    Copy reference data downloaded and unpacked into a local staging directory to its final
    location on a NERSC DTN-mounted filesystem, and write the completion file JAWS watches for.

    This is necessary because reference data lives on a filesystem (/global/dna) that is only
    writable from NERSC DTNs, but `xfer` QOS Slurm jobs run on Perlmutter login nodes. This
    relies on the NERSC sshproxy SSH key already present in $HOME being valid and shared between
    login and DTN nodes, requiring no further authentication.

    staging_dir - the local directory the reference data was downloaded and unpacked into.
    dtn_host - the DTN hostname to copy the data to.
    dest_dir - the final, DTN-only-writable destination directory for the reference data.
    completion_file - the path, on the DTN host, of the completion file JAWS watches for.
    completion_file_contents - the contents to write to the completion file.
    """
    # --protect-args: dest_dir / completion_file are always built from a server-generated UUID
    #     plus fixed config strings, never user input, so this is defense in depth rather than a
    #     fix for a reachable bug.
    # --timeout / ConnectTimeout: without these a stalled connection hangs until the enclosing
    #     Slurm job's wall-clock limit kills it (up to 48h under the `xfer` QOS).
    # --partial: keep partially-transferred files on interruption so a retry isn't guaranteed to
    #     re-copy everything from scratch.
    rsync_base_args = [
        "rsync", "-a", "--mkpath", "--protect-args", "--partial", "--timeout=300",
        "-e", "ssh -o BatchMode=yes -o ConnectTimeout=30",
    ]
    subprocess.run(
        [
            *rsync_base_args,
            f"{str(staging_dir).rstrip('/')}/",
            f"{dtn_host}:{dest_dir}/",
        ],
        check=True,
    )
    # Write the completion file locally, next to the (by now already synced) staging dir, and
    # let rsync push it over rather than shelling out to write it remotely.
    local_completion_file = Path(staging_dir).parent / Path(completion_file).name
    local_completion_file.write_text(f"{completion_file_contents}\n")
    subprocess.run(
        [
            *rsync_base_args,
            str(local_completion_file),
            f"{dtn_host}:{completion_file}",
        ],
        check=True,
    )


def process_refdata_download_manifest(
    manifest_file_path: str,
    staging_dir: str,
    dtn_host: str,
    dest_dir: str,
    completion_file: str,
    completion_file_contents: str,
):
    """
    Downloads reference data files per a transfer manifest into a local staging directory, then
    copies them to their final DTN-only-writable location and writes the JAWS completion file.

    manifest_file_path - the path to the transfer manifest file. Its file entries are expected
        to point into staging_dir.
    staging_dir - the local directory the reference data will be downloaded and unpacked into.
    dtn_host - the DTN hostname to copy the data to.
    dest_dir - the final, DTN-only-writable destination directory for the reference data.
    completion_file - the path, on the DTN host, of the completion file JAWS watches for.
    completion_file_contents - the contents to write to the completion file.
    """
    process_data_transfer_manifest(manifest_file_path)
    sync_refdata_to_dtn(staging_dir, dtn_host, dest_dir, completion_file, completion_file_contents)
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
            # Redirects are disallowed since the callback target is dynamically supplied and
            # a redirect could be used to route the request to an arbitrary, untrusted host.
            ret = requests.get(callback_url, allow_redirects=False)
            if ret.status_code < 200 or ret.status_code > 299:
                failed = True
                # log callback errors for debugging purposes, service will never see this
                # since it's written post callback
                with open(callback_file, "w") as f:
                    j = {
                        "result": "fail",
                        "callback_text": ret.text,
                        "callback_code": ret.status_code,
                        "callback_redirect_location": ret.headers.get("Location"),
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
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    mode = os.environ["CTS_MODE"]
    resfile = os.environ["CTS_RESULT_FILE_LOCATION"]
    callback_url = os.environ["CTS_CALLBACK_URL"]
    logging.getLogger(__name__).info(f"Starting remote task in mode {mode}")
    if mode == "manifest":
        _error_wrapper(
            process_data_transfer_manifest,
            [os.environ["CTS_MANIFEST_LOCATION"]],
            resfile,
            callback_url
        )
    elif mode == "refdata_manifest":
        _error_wrapper(
            process_refdata_download_manifest,
            [
                # TODO CODE staging dir is redundant with the manifest, the file in
                #           the manifest should be prefixed with the staging dir
                os.environ["CTS_MANIFEST_LOCATION"],
                os.environ["CTS_STAGING_DIR"],
                os.environ["CTS_DTN_HOST"],
                os.environ["CTS_REFDATA_DEST_DIR"],
                os.environ["CTS_COMPLETION_FILE_LOCATION"],
                os.environ["CTS_COMPLETION_FILE_CONTENTS"],
            ],
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
    logging.getLogger(__name__).info(f"Remote task in mode {mode} complete")


if __name__ == "__main__":
    main()
