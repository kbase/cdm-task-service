'''
Module that determines a file's expected path given a job input.
'''

from bidict import bidict
import marisa_trie
import math
import os

from cdmtaskservice.models import JobInput, S3File


def determine_file_locations(job_input: JobInput) -> dict[S3File | str, str]:
    """
    Given job input, determine where files should be placed given a set of input roots.
    
    job_input - the job input.
    
    Returns a mapping of each file in the job input to the file path with any relevant input root
    removed, or just the filename if no input roots match.
    """
    trie, pathseg_to_id = _setup_trie(job_input)
    ret = {}
    seen = {}
    for i, f in enumerate(job_input.input_files):
        fstr = f.file if isinstance(f, S3File) else f
        # if the trie is empty or only contains "" could speed things up by not translating the
        # path
        idpath = _path_to_id_string(fstr, pathseg_to_id)
        pref = [p for p in trie.prefixes(idpath) if p != idpath]
        if pref:
            pref = sorted(pref, key=lambda x: len(x))[-1]
            loc = _id_string_to_path(idpath.removeprefix(pref + "/"), pathseg_to_id)
        else:
            loc = os.path.basename(fstr)
        if loc in seen:
            raise InputFileCollisionError(
                f"Input files '{fstr}' at index {i} and '{seen[loc][0]}' "
                + f"at index {seen[loc][1]} collide at path "
                + f"'{job_input.params.input_mount_point}/{loc}'")
        ret[f] = loc
        seen[loc] = (fstr, i)
    return ret


def _setup_trie(job_input: JobInput) -> (marisa_trie.Trie, bidict[str, str]):
    # Make a poor man's path trie using IDs to substitute for path segments.
    # This seems dumb, there must be a better way
    # other than creating a trie specifically for paths rather than strings, anyway
    pathseg_to_id = {}
    id_ = 0
    empty_path = False
    if job_input.input_roots:
        for ir in job_input.input_roots:
            if ir == "":
                empty_path = True
            else:
                id_ = _process_path(ir, id_, pathseg_to_id)
    for f in job_input.input_files:
        id_ = _process_path(f.file if isinstance(f, S3File) else f, id_, pathseg_to_id)
    # Make all IDs the same length so an ID of 2 can't prefix and ID of 22, etc.
    # Could probably save some space using base64 or something... YAGNI for now
    digits = int(math.log10(len(pathseg_to_id))) + 1
    pathseg_to_id = bidict({k: str(v).zfill(digits) for k, v in pathseg_to_id.items()})

    trie_in = [""] if empty_path else []
    if job_input.input_roots:
        for ir in job_input.input_roots:
            if ir:
                trie_in.append(_path_to_id_string(ir, pathseg_to_id))
    return marisa_trie.Trie(trie_in), pathseg_to_id


def _process_path(path: str, id_: int, pathseg_to_id: dict[str, str]) -> int:
    for p in path.split("/"):
        if p not in pathseg_to_id:
            pathseg_to_id[p] = id_
            id_ += 1
    return id_


def _path_to_id_string(path: str, pathseg_to_id: bidict[str, int]) -> str:
    return "/".join(pathseg_to_id[p] for p in path.split("/"))


def _id_string_to_path(id_string: str, pathseg_to_id: bidict[str, int]) -> str:
    return "/".join(pathseg_to_id.inverse[id_] for id_ in id_string.split("/"))


class InputFileCollisionError(Exception):
    """
    Error thrown when two input files map to the same location.
    """
