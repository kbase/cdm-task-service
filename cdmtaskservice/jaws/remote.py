"""
Code for parsing JAWS output expected to be run at a remote location, e.g. on NERSC.

In particular, non-standard lib dependency imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
"""

import io
import json
from pathlib import Path
from typing import NamedTuple

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
# NOTE - importing the constants directly will cause the NERSC manager's dependency resolution
# code to break, since they're just literal imports and don't point back to their parent module 
from cdmtaskservice.jaws import constants
from cdmtaskservice.jobflows.container_filenames import get_filenames_for_container


OUTPUTS_JSON_FILE = "outputs.json"
"""
The name of the file in the JAWS output directory containing the output file paths.
"""

ERRORS_JSON_FILE = "errors.json"
"""
The filename of the errors JSON file in the jaws output directory.
"""


class OutputsJSON(NamedTuple):
    """
    The parsed contents of the JAWS outputs.json file, which contains the paths to files
    output by the JAWS job.
    """
    
    output_files: dict[str, str]
    """
    The result file paths of the job as a dict of the relative path in the output directory
    for the job container to the relative path in the JAWS results directory.
    """
    # Could maybe be a little more efficient by wrapping the paths in a class and parsing the
    # container path on demand... YAGNI
    
    stdout: list[str]
    """
    The standard out file paths relative to the JAWS results directory, ordered by
    container number.
    """
    
    stderr: list[str]
    """
    The standard error file paths relative to the JAWS results directory, ordered by
    container number.
    """


def parse_outputs_json(outputs_json: io.BytesIO) -> OutputsJSON:
    """
    Parse the JAWS outputs.json file from a completed run. If any output files have the same
    path inside their specific container, only one path is returned and which path is returned
    is not specified.
    """
    js = json.load(_not_falsy(outputs_json, "outputs_json"))
    outfiles = {}
    stdo = []
    stde = []
    for key, val in js.items():
        if key.endswith(constants.OUTPUT_FILES):
            for files in val:
                outfiles.update({_get_relative_file_path(f): f for f in files})
        # assume files are ordered correctly. If this is wrong sort by path first
        elif key.endswith(constants.STDOUTS):
            stdo = val
        elif key.endswith(constants.STDERRS):
            stde = val
        else:
            # shouldn't happen, but let's not fail silently if it does
            raise ValueError(f"unexpected JAWS outputs.json key: {key}")
    return OutputsJSON(outfiles, stdo, stde)


def parse_pipeline_outputs_json(
    outputs_json: io.BytesIO, allowed_keys: list[str]
) -> dict[str, str]:
    """
    Parse a pipeline WDL outputs.json, returning only the entries whose key is in
    allowed_keys, mapped to the file path for that key.

    Keys in allowed_keys that are absent or null in outputs.json are silently skipped - both
    are legitimate for an optional (File?) WDL output. Keys present in outputs.json but not
    in allowed_keys are ignored - that's the point of the allow-list.

    Raises ValueError if an allow-listed key's value is present, non-null, and not a JSON
    string - i.e. doesn't match the "single File output" assumption implied by allow-listing
    it (e.g. an Array[File] or Array[String] mistakenly allow-listed). This does not catch a
    mistakenly allow-listed WDL String output, since that's indistinguishable from a File
    path string in JSON.
    """
    js = json.load(_not_falsy(outputs_json, "outputs_json"))
    out = {}
    for key in _not_falsy(allowed_keys, "allowed_keys"):
        if key not in js or js[key] is None:
            continue
        val = js[key]
        if not isinstance(val, str):
            raise ValueError(
                f"Expected a string (File path) value for allow-listed output key "
                f"'{key}', got {type(val).__name__}"
            )
        out[key] = val
    return out


def _get_relative_file_path(file: str) -> str:
    """
    Given a JAWS output file path, get the file path relative to the container output directoy,
    e.g. the file that was written from the container's perspective.
    """
    return _require_string(file, "file").split(f"/{constants.OUTPUT_DIR}/")[-1]


def parse_errors_json(errors_json: io.BytesIO, logpath: Path) -> list[tuple[int, str | None]]:
    """
    Parses a JAWS errors.json file and writes the return code, stdout, and stderr files
    to the given path, with the names of the files as
    `container-{container number}-[rc | stdout | stderr].txt`.
    
    Assumes there's only one container name in the json, which is the case for CTS jobs.
    
    Returns a list of tuples of the container return code and an error message
    from Cromwell ordered by the container number.
    The error message is typically only useful for someone with Cromwell familiarity.
    """
    # may need to use an iterative parsing strategy with `ijson` or something
    # These files could be really big
    # Pretty sure there are error conditions that may occur where this parser will choke,
    # will deal with them as they happen
    if not logpath:
        raise ValueError("logpath is required")
    if not errors_json:
        raise ValueError("errors_json is required")
    j = json.load(errors_json)
    if not j:
        raise ValueError("No errors in error json")
    j = j["calls"]
    if len(j) != 1:
        raise ValueError("Expected only one call")
    j = j[list(j.keys())[0]]
    id2rc = {}
    # Could parallelize some of this or use async if necessary... YAGNI 
    for c in j:
        cid = c["shardIndex"]
        if cid in id2rc:
            raise ValueError(f"Duplicate shardIndex: {cid}")
        rc = c.get("returnCode")  # no rc if container didn't run
        # never seen a structure other than this, may need changes
        err = c["failures"][0]["message"]
        id2rc[cid] = (rc, err)
        # TODO ERRORHANDLING if the docker image string is invalid, there's no stdout or err logs
        #                    and no return code. Make this more flexible and tell the server
        #                    what's available.
        sof, sef = get_filenames_for_container(cid)
        with open(logpath / sof, "w") as f:
            f.write(c["stdoutContents"])
        with open(logpath / sef, "w") as f:
            f.write(c["stderrContents"])
    if len(id2rc.keys()) - 1 != max(id2rc.keys()):
        raise ValueError("shardIndexes are not continuous integers from zero")
    return [id2rc[k] for k in sorted(id2rc.keys())]
