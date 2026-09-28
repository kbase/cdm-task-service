import types
from pathlib import Path

import pytest
import semver
from pydantic import ValidationError

from cdmtaskservice import models
from cdmtaskservice.pipelines.definition import (
    PipelineDefinition,
    PipelineInputValidationError,
    PipelineInput,
)


class _FakeInput(PipelineInput):

    def get_s3_files(self):
        return []

    def set_s3_files(self, resolved):
        return self

    def get_input_json(self, file_locations):
        return {"foo": "bar"}


class _FakeInputWithField(PipelineInput):
    value: int

    def get_s3_files(self):
        return []

    def set_s3_files(self, resolved):
        return self

    def get_input_json(self, file_locations):
        return {"value": self.value}


class _FakeInputWithFiles(PipelineInput):
    files: list[models.S3File]

    def get_s3_files(self):
        return self.files

    def set_s3_files(self, resolved):
        return self.model_copy(update={"files": [resolved[f.file] for f in self.files]})

    def get_input_json(self, file_locations):
        return {}


def _def(**overrides) -> PipelineDefinition:
    kwargs = dict(
        name="foo",
        version=semver.Version.parse("0.1.0"),
        description="a fake pipeline",
        input_model=_FakeInput,
        nersc_path=Path("/foo/bar"),
        main_wdl="main.wdl",
        file_md5s={"main.wdl": "abc123", "imports/sub.wdl": "def456"},
    )
    kwargs.update(overrides)
    return PipelineDefinition(**kwargs)


def test_pipeline_definition():
    d = _def()

    assert d.name == "foo"
    assert d.version == semver.Version.parse("0.1.0")
    assert d.description == "a fake pipeline"
    assert d.input_model is _FakeInput
    assert d.nersc_path == Path("/foo/bar")
    assert d.main_wdl == "main.wdl"
    assert d.file_md5s == {"main.wdl": "abc123", "imports/sub.wdl": "def456"}
    assert isinstance(d.file_md5s, types.MappingProxyType)
    assert d.doc_urls == []


def test_pipeline_definition_doc_urls():
    d = _def(doc_urls=["https://example.com/docs"])

    assert d.doc_urls == ["https://example.com/docs"]


def test_pipeline_definition_build():
    d = _def()
    inp = _FakeInput()

    run = d.validate_input(inp).get_input_json({})

    assert run == {"foo": "bar"}


def test_pipeline_input_fail_duplicate_files():
    f1 = models.S3File(file="mybucket/reads_1.fastq.gz")

    with pytest.raises(
        ValidationError, match=r"Duplicate files in input: \['mybucket/reads_1.fastq.gz'\]"
    ):
        _FakeInputWithFiles(files=[f1, f1])


def test_pipeline_definition_validate_input_dict():
    d = _def(input_model=_FakeInputWithField)

    result = d.validate_input({"value": 5})

    assert result == _FakeInputWithField(value=5)


def test_pipeline_definition_validate_input_already_typed():
    d = _def(input_model=_FakeInputWithField)
    inp = _FakeInputWithField(value=5)

    result = d.validate_input(inp)

    assert result is inp


def test_pipeline_definition_validate_input_fail():
    d = _def(input_model=_FakeInputWithField)

    with pytest.raises(PipelineInputValidationError) as e:
        d.validate_input({})

    assert len(e.value.errors) == 1
    assert e.value.errors[0]["type"] == "missing"
    assert e.value.errors[0]["loc"] == ("value",)


def test_pipeline_definition_validate_input_then_build():
    d = _def(input_model=_FakeInputWithField)

    run = d.validate_input({"value": 5}).get_input_json({})

    assert run == {"value": 5}


def test_pipeline_definition_fail_no_name():
    with pytest.raises(ValueError, match="name is required"):
        _def(name=None)


def test_pipeline_definition_fail_no_version():
    with pytest.raises(ValueError, match="version is required"):
        _def(version=None)


def test_pipeline_definition_fail_version_wrong_type():
    with pytest.raises(ValueError, match="version must be a semver.Version instance"):
        _def(version="0.1.0")


def test_pipeline_definition_fail_no_description():
    with pytest.raises(ValueError, match="description is required"):
        _def(description=None)


def test_pipeline_definition_fail_no_input_model():
    with pytest.raises(ValueError, match="input_model is required"):
        _def(input_model=None)


def test_pipeline_definition_fail_no_nersc_path():
    with pytest.raises(ValueError, match="nersc_path is required"):
        _def(nersc_path=None)


def test_pipeline_definition_fail_no_main_wdl():
    with pytest.raises(ValueError, match="main_wdl is required"):
        _def(main_wdl=None)


def test_pipeline_definition_fail_empty_md5s():
    with pytest.raises(ValueError, match="file_md5s is required and may not be empty"):
        _def(file_md5s={})


def test_pipeline_definition_fail_main_wdl_not_in_md5s():
    with pytest.raises(ValueError, match="main_wdl 'main.wdl' must have an entry in file_md5s"):
        _def(file_md5s={"other.wdl": "abc123"})
