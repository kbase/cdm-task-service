import pytest
from cdmtaskservice.dockerimages import DockerImageInfo


@pytest.mark.asyncio
async def test_get_digest_from_name():
    dig = DockerImageInfo("fake_path").get_digest_from_name("fake_image")

    assert dig == "sha256:fake"
