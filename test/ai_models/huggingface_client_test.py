"""
Tests for the HuggingFace client against the real huggingface.co service - no mocking.

These tests download a tiny, stable, public test model (`julien-c/dummy-unknown`, ~280kB) and
so require network access to huggingface.co. They're kept fast/small enough to run as part of
the normal automated suite rather than test_manual, mirroring how image_remote_lookup_test.py
exercises the real ghcr.io/Docker Hub registries rather than mocking them.
"""

from unittest.mock import patch

import pytest
from huggingface_hub import HfApi
from huggingface_hub.errors import HFValidationError, RepositoryNotFoundError, RevisionNotFoundError
import httpx2

from cdmtaskservice.ai_models.huggingface_client import (
    AIModelResolutionError,
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


def test_resolve_revision_with_hash():
    assert resolve_revision(_REPO_ID, _REVISION_HASH) == _REVISION_HASH


def test_resolve_revision_with_branch():
    assert resolve_revision(_REPO_ID, "main") == _REVISION_HASH


def test_resolve_revision_with_older_hash():
    assert resolve_revision(_REPO_ID, _OLD_REVISION_HASH) == _OLD_REVISION_HASH


def test_resolve_revision_with_different_repo():
    assert resolve_revision(_REPO_ID_2, _REVISION_HASH_2) == _REVISION_HASH_2


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
