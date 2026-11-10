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


class OutputFileGroup(NamedTuple):
    """
    A single container's (JAWS scatter shard's) own group of declared output files.
    """

    output_dir: str | None
    """
    The relative path, within the JAWS results directory, of this container's own output
    directory - the directory named constants.OUTPUT_DIR that every file in this group is
    expected to be reported under. None if the container produced no output files.
    """

    files: dict[str, str]
    """
    A dict of the relative path in this container's own output directory for each of its
    output files to the raw relative path in the JAWS results directory.
    """


class OutputsJSON(NamedTuple):
    """
    The parsed contents of the JAWS outputs.json file, which contains the paths to files
    output by the JAWS job.
    """
    
    output_file_groups: list[OutputFileGroup]
    """
    The result files of the job, one group per container (JAWS scatter shard). Grouped by
    container, rather than flattened into a single dict, so a container's own output directory
    - not just the JAWS results directory for the whole job, which may contain multiple
    containers' output directories - can be used to validate that a file (or a symlink to one)
    actually resolves inside the output area it was reported from. If two containers produce
    the same relative output path, both are retained here; a caller that needs a single flat
    mapping decides which one to keep.
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


def _file_group(files: list[str]) -> OutputFileGroup:
    marker = f"/{constants.OUTPUT_DIR}/"
    file_map = {}
    output_dirs = set()
    for f in files:
        f = _require_string(f, "file")
        # Split on the *first* occurrence, not the last: the container has write access to its
        # own output directory and could create a subdirectory that's also named
        # constants.OUTPUT_DIR. The first occurrence is always the real `find` search root
        # Cromwell reported the file under, regardless of what's nested inside it afterward.
        before, sep, after = f.partition(marker)
        if not sep:
            raise ValueError(
                f"Expected JAWS output file path to be under a '{constants.OUTPUT_DIR}' "
                + f"directory: {f}"
            )
        file_map[after] = f
        output_dirs.add(f"{before}/{constants.OUTPUT_DIR}")
    if len(output_dirs) > 1:
        raise ValueError(
            "JAWS reported output files for a single container under more than one output "
            + f"directory, which should never happen: {sorted(output_dirs)}"
        )
    return OutputFileGroup(output_dirs.pop() if output_dirs else None, file_map)


def parse_outputs_json(outputs_json: io.BytesIO) -> OutputsJSON:
    """
    Parse the JAWS outputs.json file from a completed run. Within a single container's own
    output files, if two files have the same relative path, only one path is returned and
    which path is returned is not specified.
    """
    js = json.load(_not_falsy(outputs_json, "outputs_json"))
    output_groups = []
    stdo = []
    stde = []
    for key, val in js.items():
        if key.endswith(constants.OUTPUT_FILES):
            for files in val:
                output_groups.append(_file_group(files))
        # assume files are ordered correctly. If this is wrong sort by path first
        elif key.endswith(constants.STDOUTS):
            stdo = val
        elif key.endswith(constants.STDERRS):
            stde = val
        else:
            # shouldn't happen, but let's not fail silently if it does
            raise ValueError(f"unexpected JAWS outputs.json key: {key}")
    return OutputsJSON(output_groups, stdo, stde)


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
