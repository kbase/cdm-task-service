import pytest
import os
from pathlib import Path

from cdmtaskservice.dockerimages import (
    DockerImageInfo,
    NormedImageName,
    ImageNameParseError,
    CranePathError,
    ImageInfoFetchError,
    ImageInfoError,
)
import config as testcfg
from conftest import assert_exception_correct


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
    ws0_15_0_sha = "sha256:7e41821daf50abcd511654ddd9bdf66ed6374a30a1d62547631e79dcbe4ad92e"
    ws0_10_2_sha = "sha256:285b1229730192eac5b502c44bfffff98e9840057821d1a26bed47a05bc44874"
    # no longer pushing to dockerhub so this shouldn't change, unlike ghcr
    ws_docker_latest = "sha256:8a3eaf76ce3506da4113510221218cab0867462986c61025f0b50d18b08de7b6"
    testset = {
        "ghcr.io/kbase/workspace_deluxe:0.15.0":
            NormedImageName(
                normedname="ghcr.io/kbase/workspace_deluxe@" + ws0_15_0_sha, tag="0.15.0"
            ),
        "ghcr.io/kbase/workspace_deluxe@" + ws0_15_0_sha:
            NormedImageName(
                normedname="ghcr.io/kbase/workspace_deluxe@" + ws0_15_0_sha,
                olddigest=ws0_15_0_sha
            ),
        "ghcr.io/kbase/workspace_deluxe:0.15.0@" + ws0_15_0_sha:
            NormedImageName(
                normedname="ghcr.io/kbase/workspace_deluxe@" + ws0_15_0_sha,
                tag="0.15.0",
                olddigest=ws0_15_0_sha
            ),
        # this tests all non alphanum chars other than @ and uppercase
        "kbase/workspace_deluxe:0.10.2-hotfix":
            NormedImageName(
                normedname="docker.io/kbase/workspace_deluxe@" + ws0_10_2_sha, tag="0.10.2-hotfix"
            ),
        "kbase/workspace_deluxe": 
            NormedImageName(normedname="docker.io/kbase/workspace_deluxe@" + ws_docker_latest),
        "kbase/workspace_deluxe:latest":
            NormedImageName(
                normedname="docker.io/kbase/workspace_deluxe@" + ws_docker_latest, tag="latest"
            ),
        "ubuntu:noble-20240827.1":
            NormedImageName(
                normedname="docker.io/library/ubuntu@sha256:"
                    +"dfc10878be8d8fc9c61cbff33166cb1d1fe44391539243703c72766894fa834a",
                tag="noble-20240827.1"
            ),
    }
    dii = await DockerImageInfo.create(testcfg.CRANE_EXE_PATH)
    for k, v in testset.items():
        dig = await dii.normalize_image_name(k)
        assert dig == v


@pytest.mark.asyncio
async def test_get_entrypoint_from_name():
    testset = {
        "ghcr.io/kbase/workspace_deluxe:0.15.0": ["/kb/deployment/bin/dockerize"],
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
