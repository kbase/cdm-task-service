"""
Shared client for resolving and staging AI models hosted on HuggingFace.

Expected to be run at NERSC.
In particular, non-standard lib dependency imports should be kept to a minimum and the newest
python features should be avoided to make setup on the remote cluster simple and allow for older
python versions.
"""

from pathlib import Path
from typing import cast, NamedTuple

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
    the repo - i.e. the caller supplied a bad (repo_id, revision) pair. Also thrown when
    resolution otherwise succeeds but the resulting snapshot's total size is implausibly small,
    which almost certainly means the repo/revision isn't a usable AI model (e.g. an empty repo,
    or a revision with no real file content) rather than that the size check itself is wrong.
    Not thrown for network failures or other unexpected errors contacting HuggingFace - those
    propagate unchanged, since they aren't the caller's fault.
    """


# A real AI model snapshot - even a minimal one with just a config and a small tokenizer - is
# always well clear of this floor; the tiny public HuggingFace repos used in this module's own
# tests are ~220-285 KB, two orders of magnitude above it. Anything smaller indicates the repo
# is empty, the revision has no real file content, or huggingface_hub's file metadata lookup
# silently failed (see resolve_revision's size calculation) - all cases where staging would
# otherwise proceed with a snapshot that isn't actually a usable AI model.
_MIN_SANE_SNAPSHOT_SIZE_BYTES = 1024


class ResolvedRevision(NamedTuple):
    """ The result of resolving a HuggingFace repo id and revision. """

    revision_hash: str
    """ The resolved commit hash. """

    size_bytes: int
    """ The total size, in bytes, of every file in the snapshot. """


def resolve_revision(repo_id: str, revision: str) -> ResolvedRevision:
    """
    Resolve a HuggingFace repo id and revision (a branch, tag, or commit hash) to the
    revision's commit hash and total snapshot size. Makes no changes to local or remote state
    and downloads no files - the size comes from file metadata returned alongside the lookup,
    not from downloading the files themselves.

    repo_id - the HuggingFace repo id, e.g. `org/name`.
    revision - a branch, tag, or commit hash to resolve.

    returns the resolved commit hash and the snapshot's total size in bytes.
    """
    repo_id = _require_string(repo_id, "repo_id")
    revision = _require_string(revision, "revision")
    try:
        info = HfApi().model_info(repo_id, revision=revision, files_metadata=True)
    except (HFValidationError, RepositoryNotFoundError, RevisionNotFoundError) as e:
        raise AIModelResolutionError(str(e)) from e
    # ModelInfo.sha is typed as str | None since it's absent for some other model_info() call
    # shapes (e.g. requesting only specific `expand` properties), but the hub always returns it
    # for a plain lookup like this one.
    # siblings' sizes are populated because files_metadata=True was passed above; each sibling's
    # size is only None if the file info couldn't be retrieved for that specific file, which
    # shouldn't happen for a lookup that otherwise succeeded, but is treated as 0 rather than
    # raising, since an exact count isn't worth failing registration over.
    size = sum(s.size or 0 for s in (info.siblings or []))
    if size < _MIN_SANE_SNAPSHOT_SIZE_BYTES:
        raise AIModelResolutionError(
            f"Resolved snapshot for {repo_id}@{revision} has an implausibly small total size "
            f"of {size} bytes - this is not a usable AI model"
        )
    return ResolvedRevision(revision_hash=cast(str, info.sha), size_bytes=size)


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
