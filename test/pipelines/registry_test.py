from pathlib import Path

import pytest
import semver

from cdmtaskservice.pipelines.definition import PipelineDefinition, PipelineInput
from cdmtaskservice.pipelines.registry import (
    NoSuchPipelineError,
    PipelineExistsError,
    PipelineRegistry,
)


class _FakeInput(PipelineInput):

    def get_s3_files(self):
        return []

    def set_s3_files(self, resolved):
        return self

    def get_input_json(self, file_locations):
        raise NotImplementedError()


def _fake_pipeline(name: str, version: str) -> PipelineDefinition:
    return PipelineDefinition(
        name=name,
        version=semver.Version.parse(version),
        description="a fake pipeline",
        input_model=_FakeInput,
        nersc_path=Path("/fake"),
        main_wdl="fake.wdl",
        file_md5s={"fake.wdl": "abc123"},
        output_keys={"fake.out"},
    )


def test_register_and_get():
    reg = PipelineRegistry()
    p1 = _fake_pipeline("foo", "0.1.0")
    reg.register(p1)

    assert reg.get("foo", semver.Version.parse("0.1.0")) is p1
    assert reg.get("foo") is p1
    assert reg.list_versions("foo") == [p1]
    assert reg.list_latest() == [p1]


def test_register_multiple():
    p1 = _fake_pipeline("foo", "0.1.0")
    p2 = _fake_pipeline("bar", "1.0.0")
    reg = PipelineRegistry()
    reg.register(p1)
    reg.register(p2)

    assert reg.get("foo") is p1
    assert reg.get("bar") is p2
    assert reg.list_latest() == [p2, p1]


def test_get_latest_version_by_semver():
    p1 = _fake_pipeline("foo", "0.1.0")
    p2 = _fake_pipeline("foo", "0.2.0")
    p3 = _fake_pipeline("foo", "0.10.0")
    p4 = _fake_pipeline("foo", "0.9.0")
    reg = PipelineRegistry()
    for p in (p1, p2, p3, p4):
        reg.register(p)

    assert reg.get("foo") is p3
    assert reg.list_versions("foo") == [p1, p2, p4, p3]


def test_list_latest():
    p1 = _fake_pipeline("foo", "0.1.0")
    p2 = _fake_pipeline("foo", "0.2.0")
    p3 = _fake_pipeline("bar", "1.0.0")
    reg = PipelineRegistry()
    for p in (p1, p2, p3):
        reg.register(p)

    assert reg.list_latest() == [p3, p2]


def test_register_fail_duplicate():
    reg = PipelineRegistry()
    reg.register(_fake_pipeline("foo", "0.1.0"))

    with pytest.raises(
        PipelineExistsError,
        match="A pipeline named 'foo' with version '0.1.0' is already registered",
    ):
        reg.register(_fake_pipeline("foo", "0.1.0"))


def test_get_fail_no_such_pipeline_name():
    reg = PipelineRegistry()
    reg.register(_fake_pipeline("foo", "0.1.0"))
    with pytest.raises(NoSuchPipelineError, match="No pipeline named 'bar' is registered"):
        reg.get("bar")
    with pytest.raises(NoSuchPipelineError, match="No pipeline named 'bar' is registered"):
        reg.list_versions("bar")


def test_get_fail_no_such_version():
    reg = PipelineRegistry()
    reg.register(_fake_pipeline("foo", "0.1.0"))
    with pytest.raises(
        NoSuchPipelineError, match="No version '1.0.0' of pipeline 'foo' is registered"
    ):
        reg.get("foo", semver.Version.parse("1.0.0"))
