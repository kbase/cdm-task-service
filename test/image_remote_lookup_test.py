import pytest
import os
from pathlib import Path

from cdmtaskservice.image_remote_lookup import (
    DockerImageInfo,
    CompleteImageName,
    ImageNameParseError,
    CranePathError,
    ImageInfoFetchError,
    ImageInfoError,
)
from test_common import config as testcfg
from conftest import assert_exception_correct

# no longer pushing to dockerhub so this shouldn't change, unlike ghcr
_SHA_WS_DOCKER_LATEST = "sha256:8a3eaf76ce3506da4113510221218cab0867462986c61025f0b50d18b08de7b6"
_SHA_WS_DOCkER_0_10_2 = "sha256:285b1229730192eac5b502c44bfffff98e9840057821d1a26bed47a05bc44874"
_SHA_WS_0_14_1 = "sha256:66f6bb4a503e83867d1160f34b9b0349f54c33ca04f61ccd30ebd997976b46b3"
_SHA_WS_0_14_2 = "sha256:e3d1db28c88f08cfbe632f0184da0a03e2016d8d11f1e800ea0ec98c6ccb2082"
_SHA_WS_0_15_0 = "sha256:7e41821daf50abcd511654ddd9bdf66ed6374a30a1d62547631e79dcbe4ad92e"


def test_CompleteImageName():
    nim = CompleteImageName(name="foo", tag="bar", digest="sha256:stuff")
    assert nim.name == "foo"
    assert nim.tag == "bar"
    assert nim.digest == "sha256:stuff"
    assert nim.name_with_digest == "foo@sha256:stuff"


@pytest.mark.asyncio
async def test_create_fail_invalid_crane_path():
    badcrane = os.path.normpath((Path(__file__) / ".." / "testfiles" / "badcrane").absolute())
    testset = {
        None: CranePathError("crane_absolute_path cannot be None"),
        "fake/../../crane/path": CranePathError("crane_absolute_path must be absolute"),
        "/fake/crane/path": CranePathError(
            "Configured crane executable path /fake/crane/path is invalid"),
        badcrane: ValueError("crane executable version call failed, retcode: 123 stderr:\noopsie"),
    }
    for k, v in testset.items():
        with pytest.raises(Exception) as got:
            await DockerImageInfo.create(k)
        assert_exception_correct(got.value, v)


@pytest.mark.asyncio
async def test_narmalize_image_name():
    testset = {
        "ghcr.io/kbase/workspace_deluxe:0.15.0":
            CompleteImageName(
                name="ghcr.io/kbase/workspace_deluxe", digest=_SHA_WS_0_15_0, tag="0.15.0"
            ),
        "ghcr.io/kbase/workspace_deluxe@" + _SHA_WS_0_15_0:
            CompleteImageName(name="ghcr.io/kbase/workspace_deluxe", digest=_SHA_WS_0_15_0),
        "ghcr.io/kbase/workspace_deluxe:0.15.0@" + _SHA_WS_0_15_0:
            CompleteImageName(
                name="ghcr.io/kbase/workspace_deluxe",
                digest=_SHA_WS_0_15_0,
                tag="0.15.0",
            ),
        # this tests all non alphanum chars other than @ and uppercase
        "kbase/workspace_deluxe:0.10.2-hotfix":
            CompleteImageName(
                name="docker.io/kbase/workspace_deluxe",
                digest=_SHA_WS_DOCkER_0_10_2,
                tag="0.10.2-hotfix"
            ),
        "kbase/workspace_deluxe": 
            CompleteImageName(
                name="docker.io/kbase/workspace_deluxe", digest=_SHA_WS_DOCKER_LATEST
            ),
        "kbase/workspace_deluxe:latest":
            CompleteImageName(
                name="docker.io/kbase/workspace_deluxe", digest=_SHA_WS_DOCKER_LATEST, tag="latest"
            ),
        "ubuntu:noble-20240827.1":
            CompleteImageName(
                name="docker.io/library/ubuntu",
                digest="sha256:dfc10878be8d8fc9c61cbff33166cb1d1fe44391539243703c72766894fa834a",
                tag="noble-20240827.1"
            ),
    }
    dii = await DockerImageInfo.create(testcfg.CRANE_EXE_PATH)
    for k, v in testset.items():
        dig = await dii.normalize_image_name(k)
        assert dig == v


@pytest.mark.asyncio
async def test_get_entrypoint_from_name():
    ws_entrypoint = ["/kb/deployment/bin/dockerize"]
    testset = {
        "kbase/workspace_deluxe": ws_entrypoint,
        "kbase/workspace_deluxe:latest": ws_entrypoint,
        "ghcr.io/kbase/workspace_deluxe:0.15.0": ws_entrypoint,
        "ghcr.io/kbase/workspace_deluxe@" + _SHA_WS_0_15_0: ws_entrypoint,
        "ghcr.io/kbase/workspace_deluxe:0.15.0@" + _SHA_WS_0_15_0: ws_entrypoint,
        "ghcr.io/kbaseapps/kb_quast:pr-36": ["./scripts/entrypoint.sh"],
        "library/ubuntu:24.04": None,
        "ghcr.io/kbase/collections:checkm2_0.1.6":
            ["/app/collections/src/loaders/compute_tools/entrypoint.sh"],
    }
    dii = await DockerImageInfo.create(testcfg.CRANE_EXE_PATH)
    for k, v in testset.items():
        ep = await dii.get_entrypoint_from_name(k)
        assert ep == v


@pytest.mark.asyncio
async def test_image_methods_fail():
    testset = {
        None: ImageNameParseError("No image name provided"),
        "gc_hr.io/kbase/workspace_deluxe:0.15.0": ImageNameParseError(
            "Unable to parse image name 'gc_hr.io/kbase/workspace_deluxe:0.15.0': "
            + "invalid reference format"),
        "ghcr.io/kbase/ɰorkspace_deluxe:0.15.0": ImageNameParseError(
            "Illegal character in image name 'ghcr.io/kbase/ɰorkspace_deluxe:0.15.0': 'ɰ'"),
        "ghcr.io/kbase/Workspace_deluxe:0.15.0": ImageNameParseError(
            "Unable to parse image name 'ghcr.io/kbase/Workspace_deluxe:0.15.0': "
            + "invalid reference format: repository name must be lowercase"),
        "ghcr.io/kbase/workspace_deluxe:0.15ɰ.0": ImageNameParseError(
            "Illegal character in image name 'ghcr.io/kbase/workspace_deluxe:0.15ɰ.0': 'ɰ'"),
        "ghcr.io/kbase/workspace_deluxe:.0.15.0": ImageNameParseError(
            "Unable to parse image name 'ghcr.io/kbase/workspace_deluxe:.0.15.0': "
            + "invalid reference format"),
        "ghcr.io/kbase/workspace_deluxe:-0.15.0": ImageNameParseError(
            "Unable to parse image name 'ghcr.io/kbase/workspace_deluxe:-0.15.0': "
            + "invalid reference format"),
        "workspace_deluxe:0.15.0": ImageInfoFetchError(
            "Failed to access information for image docker.io/library/workspace_deluxe:0.15.0. "
            + "Unauthorized to access image"),
        "ghcr.io/kb/ase/workspace_deluxe:0.15.0": ImageInfoFetchError(
            "Failed to access information for image ghcr.io/kb/ase/workspace_deluxe:0.15.0. "
            + "Error code was: requested access to the resource is denied"),
        "superfakehostforrealihope.io/kbase/workspace_deluxe:0.15.0": ImageInfoFetchError(
            "Failed to access information for image "
            + "superfakehostforrealihope.io/kbase/workspace_deluxe:"
            + "0.15.0. Error code was: no such host"),
        # note misspelling of ghcr
        "gchr.io/kbase/workspace_deluxe:0.15.0": ImageInfoFetchError(
            "Failed to access information for image gchr.io/kbase/workspace_deluxe:0.15.0. "
            + "Error code was: handshake failure"),
        "hcr.io/kbase/workspace_deluxe:0.15.0": ImageInfoFetchError(
            "Failed to access information for image hcr.io/kbase/workspace_deluxe:0.15.0. "
            + "Image was not found on the host"),
        "ghcr.io/kbase/workspace_not_deluxe:0.15.0": ImageInfoFetchError(
            "Failed to access information for image ghcr.io/kbase/workspace_not_deluxe:0.15.0. "
            + "Error code was: requested access to the resource is denied"),
    }
    dii = await DockerImageInfo.create(testcfg.CRANE_EXE_PATH)
    for k, v in testset.items():
        await _image_methods_fail(dii, k, v)


@pytest.mark.asyncio
async def test_normalize_image_name_fail():
    """Tests failure modes that only occur with this method rather than all methods. """ 
    testset = {
        "kbase/workspace_deluxe@" + _SHA_WS_0_14_2: ImageInfoFetchError(
            "Failed to access information for image docker.io/kbase/workspace_deluxe@"
            + f"{_SHA_WS_0_14_2}. Image was not found on the host"),
        "kbase/workspace_deluxe:latest@" + _SHA_WS_0_14_2: ImageInfoFetchError(
            "The digest for image docker.io/kbase/workspace_deluxe:latest, "
            + f"{_SHA_WS_DOCKER_LATEST}, does not equal the expected digest, {_SHA_WS_0_14_2}"),
        "ghcr.io/kbase/workspace_deluxe:0.14.3@" + _SHA_WS_0_14_2: ImageInfoFetchError(
            "Failed to access information for image ghcr.io/kbase/workspace_deluxe:0.14.3. "
            + "Image was not found on the host"),
        "ghcr.io/kbase/workspace_deluxe:0.14.1@" + _SHA_WS_0_14_2: ImageInfoFetchError(
            f"The digest for image ghcr.io/kbase/workspace_deluxe:0.14.1, {_SHA_WS_0_14_1}, "
            + f"does not equal the expected digest, {_SHA_WS_0_14_2}"),
    }
    dii = await DockerImageInfo.create(testcfg.CRANE_EXE_PATH)
    for k, v in testset.items():
        with pytest.raises(ImageInfoError) as got:
            await dii.normalize_image_name(k)
        assert_exception_correct(got.value, v)



@pytest.mark.asyncio
async def test_get_entrypoint_from_name_fail():
    """Tests failure modes that only occur with this method rather than all methods. """
    testset = {
        "kbase/workspace_deluxe@" + _SHA_WS_0_14_2: ImageInfoFetchError(
            "Failed to access information for image docker.io/kbase/workspace_deluxe@"
            + f"{_SHA_WS_0_14_2}. Manifest is unknown"),
        "ghcr.io/kbase/workspace_deluxe:0.14.3@" + _SHA_WS_0_14_2: ImageInfoFetchError(
            "Failed to access information for image ghcr.io/kbase/workspace_deluxe:0.14.3. "
            + "Manifest is unknown"),
    }
    dii = await DockerImageInfo.create(testcfg.CRANE_EXE_PATH)
    for k, v in testset.items():
        with pytest.raises(ImageInfoError) as got:
            await dii.get_entrypoint_from_name(k)
        assert_exception_correct(got.value, v)

@pytest.mark.asyncio
async def test_image_methods_fail_bad_chars():
    # test some of the scary chars for shell injection
    dii = await DockerImageInfo.create(testcfg.CRANE_EXE_PATH)
    for c in "[]$()`&|<>;,\n{}'\"":
        name = f"kbase/workspace{c}deluxe"
        err = ImageNameParseError(f"Illegal character in image name '{name}': '{c}'")
        await _image_methods_fail(dii, name, err)
        
        name = f"kbase_worksspace_deluxe:0.15{c}.0"
        err = ImageNameParseError(f"Illegal character in image name '{name}': '{c}'")
        await _image_methods_fail(dii, name, err)


async def _image_methods_fail(dii: DockerImageInfo, name: str, err: Exception):
    with pytest.raises(ImageInfoError) as got:
        await dii.normalize_image_name(name)
    assert_exception_correct(got.value, err)
    with pytest.raises(ImageInfoError) as got:
        await dii.get_entrypoint_from_name(name)
    assert_exception_correct(got.value, err)
