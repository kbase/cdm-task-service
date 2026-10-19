"""
Shared client for resolving and staging AI models hosted on HuggingFace.

Expected to be run at NERSC.
In particular, non-standard lib dependency imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
"""

from pathlib import Path
from typing import cast

from huggingface_hub import HfApi, snapshot_download
from huggingface_hub.errors import (
    HFValidationError,
    RepositoryNotFoundError,
    RevisionNotFoundError,
)

from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string


class AIModelResolutionError(Exception):
    """
    Thrown when a repo id + revision cannot be resolved to a commit hash because the repo id
    is malformed, the repo doesn't exist or is inaccessible, or the revision doesn't exist on
    the repo - i.e. the caller supplied a bad (repo_id, revision) pair. Not thrown for network
    failures or other unexpected errors contacting HuggingFace - those propagate unchanged,
    since they aren't the caller's fault.
    """


def resolve_revision(repo_id: str, revision: str) -> str:
    """
    Resolve a HuggingFace repo id and revision (a branch, tag, or commit hash) to the
    revision's commit hash. Makes no changes to local or remote state and downloads no files.

    repo_id - the HuggingFace repo id, e.g. `org/name`.
    revision - a branch, tag, or commit hash to resolve.

    returns the resolved commit hash.
    """
    repo_id = _require_string(repo_id, "repo_id")
    revision = _require_string(revision, "revision")
    try:
        info = HfApi().model_info(repo_id, revision=revision)
    except (HFValidationError, RepositoryNotFoundError, RevisionNotFoundError) as e:
        raise AIModelResolutionError(str(e)) from e
    # ModelInfo.sha is typed as str | None since it's absent for some other model_info() call
    # shapes (e.g. requesting only specific `expand` properties), but the hub always returns it
    # for a plain lookup like this one.
    return cast(str, info.sha)


def stage_snapshot(repo_id: str, revision_hash: str, dest_dir: Path) -> None:
    """
    Download a full snapshot of a HuggingFace repo at a specific, already resolved commit
    hash to a local directory as real files - no cache directory symlink layout is used.
    Relies on huggingface_hub's own per-file hash/ETag verification against the hub; no
    additional checksumming is performed here.

    repo_id - the HuggingFace repo id, e.g. `org/name`.
    revision_hash - the resolved commit hash to download.
    dest_dir - the directory in which to place the snapshot's files.
    """
    repo_id = _require_string(repo_id, "repo_id")
    revision_hash = _require_string(revision_hash, "revision_hash")
    dest_dir = _not_falsy(dest_dir, "dest_dir")
    snapshot_download(repo_id, revision=revision_hash, local_dir=dest_dir)
