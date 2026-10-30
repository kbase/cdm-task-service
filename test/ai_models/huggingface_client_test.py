"""
Tests for the HuggingFace client against the real huggingface.co service - no mocking.

These tests download a tiny, stable, public test model (`julien-c/dummy-unknown`, ~280kB) and
so require network access to huggingface.co. They're kept fast/small enough to run as part of
the normal automated suite rather than test_manual, mirroring how image_remote_lookup_test.py
exercises the real ghcr.io/Docker Hub registries rather than mocking them.
"""

import re
from unittest.mock import create_autospec, patch

import pytest
from huggingface_hub import HfApi
from huggingface_hub.errors import HFValidationError, RepositoryNotFoundError, RevisionNotFoundError
from huggingface_hub.hf_api import ModelInfo
import httpx2

from cdmtaskservice.ai_models.huggingface_client import (
    AIModelResolutionError,
    ResolvedRevision,
    resolve_revision,
    stage_snapshot,
)

_REPO_ID = "julien-c/dummy-unknown"
_REVISION_HASH = "60b8d3fe22aebb024b573f1cca224db3126d10f3"
# an older commit on the same repo, distinct from both _REVISION_HASH and whatever `main`
# currently points to - proves `revision` is actually honored rather than always resolving to
# HEAD
_OLD_REVISION_HASH = "27558a939c1e24f0f537189ab2488cdaf35850a4"

# a second, unrelated tiny stable public test repo - proves resolution isn't somehow
# specific to _REPO_ID
_REPO_ID_2 = "julien-c/bert-xsmall-dummy"
_REVISION_HASH_2 = "9d3811da21adb66feb315118023f528ed10c6b18"

# The exact total size of _REPO_ID's known file list at _REVISION_HASH, matching
# test_stage_snapshot's expected_sizes below.
_REPO_SIZE = 391 + 1114 + 496 + 58499 + 36 + 65074 + 157312 + 239

# _OLD_REVISION_HASH predates flax_model.msgpack's addition to the repo, so its total is
# _REPO_SIZE minus that one file
# (see https://huggingface.co/julien-c/dummy-unknown/tree/27558a939c1e24f0f537189ab2488cdaf35850a4).
_OLD_REPO_SIZE = _REPO_SIZE - 58499

# the exact total size of _REPO_ID_2's known file list at _REVISION_HASH_2
# (see https://huggingface.co/julien-c/bert-xsmall-dummy/tree/main).
_REPO_SIZE_2 = 391 + 539 + 448 + 58515 + 65467 + 159176 + 63


def test_resolve_revision_with_hash():
    assert resolve_revision(_REPO_ID, _REVISION_HASH) == ResolvedRevision(
        _REVISION_HASH, _REPO_SIZE
    )


def test_resolve_revision_with_branch():
    assert resolve_revision(_REPO_ID, "main") == ResolvedRevision(_REVISION_HASH, _REPO_SIZE)


def test_resolve_revision_with_older_hash():
    assert resolve_revision(_REPO_ID, _OLD_REVISION_HASH) == ResolvedRevision(
        _OLD_REVISION_HASH, _OLD_REPO_SIZE
    )


def test_resolve_revision_with_different_repo():
    assert resolve_revision(_REPO_ID_2, _REVISION_HASH_2) == ResolvedRevision(
        _REVISION_HASH_2, _REPO_SIZE_2
    )


@pytest.mark.parametrize("bad_repo_id", [None, "", "   "])
def test_resolve_revision_fail_no_repo_id(bad_repo_id):
    with pytest.raises(ValueError, match="^repo_id is required$"):
        resolve_revision(bad_repo_id, "main")


@pytest.mark.parametrize("bad_revision", [None, "", "   "])
def test_resolve_revision_fail_no_revision(bad_revision):
    with pytest.raises(ValueError, match="^revision is required$"):
        resolve_revision(_REPO_ID, bad_revision)


def test_resolve_revision_fail_no_such_repo():
    with pytest.raises(AIModelResolutionError, match="Repository Not Found") as got:
        resolve_revision("cdm-task-service-test-org/no-such-repo-exists", "main")
    assert isinstance(got.value.__cause__, RepositoryNotFoundError)


def test_resolve_revision_fail_no_such_revision():
    with pytest.raises(
        AIModelResolutionError, match="Revision Not Found.*no-such-revision-exists"
    ) as got:
        resolve_revision(_REPO_ID, "no-such-revision-exists")
    assert isinstance(got.value.__cause__, RevisionNotFoundError)


def test_resolve_revision_fail_malformed_repo_id():
    # a malformed repo id is rejected client-side (no HTTP call at all), but it's still a bad
    # input from the caller, so it's translated the same as a not-found repo or revision
    with pytest.raises(
        AIModelResolutionError, match="Repo id must use alphanumeric chars"
    ) as got:
        resolve_revision("not a valid repo id!!", "main")
    assert isinstance(got.value.__cause__, HFValidationError)


@pytest.mark.parametrize("siblings", [
    [],
    None,
    [{"rfilename": "config.json", "size": 10}],
    [{"rfilename": "config.json"}],  # size omitted entirely, same as a None size
])
def test_resolve_revision_fail_implausibly_small_snapshot(siblings):
    # The resolution lookup itself succeeds (a real sha is returned), but the snapshot's total
    # size is zero or far below any real AI model - patch is used only to control the siblings
    # list returned by model_info, so the size-sanity check can be exercised deterministically
    # without depending on a real HuggingFace repo staying empty/tiny forever.
    fake_info = ModelInfo(id=_REPO_ID, sha=_REVISION_HASH, siblings=siblings)
    size = sum(s.size or 0 for s in (fake_info.siblings or []))
    fake_api = create_autospec(HfApi, instance=True)
    fake_api.model_info.return_value = fake_info
    with patch("cdmtaskservice.ai_models.huggingface_client.HfApi", return_value=fake_api):
        with pytest.raises(AIModelResolutionError, match=(
            f"^Resolved snapshot for {re.escape(_REPO_ID)}@{re.escape(_REVISION_HASH)} has an "
            + f"implausibly small total size of {size} bytes - this is not a usable AI model$"
        )):
            resolve_revision(_REPO_ID, _REVISION_HASH)


def test_resolve_revision_fail_unchanged():
    # A genuine network failure (an unreachable HF endpoint) is not translated to
    # AIModelResolutionError - proving the except clause doesn't over-broadly swallow every
    # failure mode, only the ones that mean "the caller supplied a bad (repo_id, revision)".
    # patch is used only to inject an unreachable endpoint into the real HfApi client
    # constructed by resolve_revision - the HTTP call itself is real, not mocked.
    bad_endpoint = "https://this-definitely-does-not-exist-hf-endpoint.example"
    bad_api = HfApi(endpoint=bad_endpoint)
    with patch("cdmtaskservice.ai_models.huggingface_client.HfApi", return_value=bad_api):
        with pytest.raises(httpx2.ConnectError) as got:
            resolve_revision(_REPO_ID, _REVISION_HASH)
    assert str(got.value.request.url).startswith(bad_endpoint)


@pytest.mark.parametrize("bad_repo_id", [None, "", "   "])
def test_stage_snapshot_fail_no_repo_id(bad_repo_id, tmp_path):
    with pytest.raises(ValueError, match="^repo_id is required$"):
        stage_snapshot(bad_repo_id, _REVISION_HASH, tmp_path / "snapshot")


@pytest.mark.parametrize("bad_revision_hash", [None, "", "   "])
def test_stage_snapshot_fail_no_revision_hash(bad_revision_hash, tmp_path):
    with pytest.raises(ValueError, match="^revision_hash is required$"):
        stage_snapshot(_REPO_ID, bad_revision_hash, tmp_path / "snapshot")


def test_stage_snapshot_fail_no_dest_dir():
    with pytest.raises(ValueError, match="^dest_dir is required$"):
        stage_snapshot(_REPO_ID, _REVISION_HASH, None)


def test_stage_snapshot(tmp_path):
    dest = tmp_path / "snapshot"

    stage_snapshot(_REPO_ID, _REVISION_HASH, dest)

    # the repo's known file list, by name -> expected exact byte size
    # (see https://huggingface.co/julien-c/dummy-unknown/tree/main)
    expected_sizes = {
        ".gitattributes": 391,
        "README.md": 1114,
        "config.json": 496,
        "flax_model.msgpack": 58499,
        "merges.txt": 36,
        "pytorch_model.bin": 65074,
        "tf_model.h5": 157312,
        "vocab.json": 239,
    }
    for fname, expected_size in expected_sizes.items():
        fpath = dest / fname
        assert fpath.is_file(), f"missing expected file {fname}"
        assert not fpath.is_symlink(), f"{fname} unexpectedly a symlink"
        assert fpath.stat().st_size == expected_size
