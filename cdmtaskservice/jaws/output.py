"""
Code for handling JAWS output files.

Note that this file is expected to be run at NERSC. As such, non-standard lib dependency
imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
"""

import io
import json
from typing import NamedTuple

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
# NOTE - importing the constants directly will cause the NERSC manager's dependency resolution
# code to break, since they're just literal imports and don't point back to their parent module 
from cdmtaskservice.jaws import constants


OUTPUTS_JSON_FILE = "outputs.json"
"""
The name of the file in the JAWS output directory containing the output file paths.
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
                outfiles.update({get_relative_file_path(f): f for f in files})
        # assume files are ordered correctly. If this is wrong sort by path first
        elif key.endswith(constants.STDOUTS):
            stdo = val
        elif key.endswith(constants.STDERRS):
            stde = val
        else:
            # shouldn't happen, but let's not fail silently if it does
            raise ValueError(f"unexpected JAWS outputs.json key: {key}")
    return OutputsJSON(outfiles, stdo, stde)


def get_relative_file_path(file: str) -> str:
    """
    Given a JAWS output file path, get the file path relative to the container output directoy,
    e.g. the file that was written from the container's perspective.
    """
    return _require_string(file, "file").split(f"/{constants.OUTPUT_DIR}/")[-1]
