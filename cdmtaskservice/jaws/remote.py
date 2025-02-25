"""
Code for parsing JAWS output expected to be run at a remote location, e.g. on NERSC.

In particular, non-standard lib dependency imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
"""

import io
import json
from pathlib import Path


ERRORS_JSON_FILE = "errors.json"
"""
The filename of the errors JSON file in the jaws output directory.
"""


def get_filenames_for_container(container_num: int) -> tuple[str, str, str]:
    """
    Given a container number, get the return code, stdout, and stderr filenames in that order
    as a tuple.
    """
    if container_num < 0:
        raise ValueError("container_num must be >= 0")
    return (
        f"container-{container_num}-rc.txt",
        f"container-{container_num}-stdout.txt",
        f"container-{container_num}-stderr.txt",
    )


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
        rc = c.get("returnCode")
        # never seen a structure other than this, may need changes
        err = c["failures"][0]["message"]
        id2rc[cid] = (rc, err)
        # TODO ERRORHANDLING if the docker image string is invalid, there's no stdout or err logs
        #                    and no return code. Make this more flexible and tell the server
        #                    what's available.
        rcf, sof, sef = get_filenames_for_container(cid)
        with open(logpath / rcf, "w") as f:
            f.write(f"{rc if rc is not None else 'container_did_not_run'}\n")
        with open(logpath / sof, "w") as f:
            f.write(c["stdoutContents"])
        with open(logpath / sef, "w") as f:
            f.write(c["stderrContents"])
    if len(id2rc.keys()) - 1 != max(id2rc.keys()):
        raise ValueError("shardIndexes are not continuous integers from zero")
    return [id2rc[k] for k in sorted(id2rc.keys())]
