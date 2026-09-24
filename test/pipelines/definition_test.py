import types
from pathlib import Path

import pytest
import semver
from pydantic import ConfigDict, ValidationError

from cdmtaskservice import models
from cdmtaskservice.pipelines.definition import (
    PipelineDefinition,
    PipelineInputValidationError,
    PipelineInput,
    PipelineInputFiles,
    PipelineInputParams,
)
from cdmtaskservice.pipelines import models as pipe_models


class _EmptyParams(PipelineInputParams):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _EmptyFiles(PipelineInputFiles):
    model_config = ConfigDict(extra="forbid", frozen=True)

    def get_s3_files(self):
        return []

    def set_s3_files(self, resolved):
        return self


class _ValueParams(PipelineInputParams):
    model_config = ConfigDict(extra="forbid", frozen=True)

    value: int


class _NotABaseModel:
    pass


class _FakeInput(PipelineInput):
    input: _EmptyParams = _EmptyParams()
    files: _EmptyFiles = _EmptyFiles()

    def get_input_json(self, file_locations):
        return {"foo": "bar"}


class _FakeInputWithField(PipelineInput):
    input: _ValueParams
    files: _EmptyFiles = _EmptyFiles()

    def get_input_json(self, file_locations):
        return {"value": self.input.value}


def _def(**overrides) -> PipelineDefinition:
    kwargs = dict(
        name="foo",
        version=semver.Version.parse("0.1.0"),
        description="a fake pipeline",
        input_model=_FakeInput,
        nersc_path=Path("/foo/bar"),
        main_wdl="main.wdl",
        file_md5s={"main.wdl": "abc123", "imports/sub.wdl": "def456"},
        output_keys={"main.wdl.out"},
    )
    kwargs.update(overrides)
    return PipelineDefinition(**kwargs)


def test_pipeline_input_field_names_match_pipeline_job_input():
    # PipelineDefinition.validate_input reassembles a raw (input, files) pair into a dict keyed
    # by these two literal strings and validates it against a PipelineInput subclass, relying on
    # PipelineInput declaring fields with exactly these names so the resulting pydantic error
    # locs line up with PipelineJobInput's own field names. Nothing else enforces this, so guard
    # it here.
    assert set(PipelineInput.model_fields) == {
        pipe_models.FLD_PIPELINE_JOB_INPUT_INPUT, pipe_models.FLD_PIPELINE_JOB_INPUT_FILES
    }


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
    assert d.output_keys == {"main.wdl.out"}
    assert isinstance(d.output_keys, frozenset)
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
    class _FilesOnly(PipelineInputFiles):
        model_config = ConfigDict(extra="forbid", frozen=True)

        files: list[models.S3File]

        def get_s3_files(self):
            return self.files

        def set_s3_files(self, resolved):
            return self.model_copy(update={"files": [resolved[f.file] for f in self.files]})

    class _FakeInputWithFiles(PipelineInput):
        input: _EmptyParams = _EmptyParams()
        files: _FilesOnly

        def get_input_json(self, file_locations):
            return {}

    f1 = models.S3File(file="mybucket/reads_1.fastq.gz")

    with pytest.raises(
        ValidationError, match=r"Duplicate files in input: \['mybucket/reads_1.fastq.gz'\]"
    ):
        _FakeInputWithFiles(files=_FilesOnly(files=[f1, f1]))


def test_pipeline_input_fail_too_many_files():
    class _FilesOnly(PipelineInputFiles):
        model_config = ConfigDict(extra="forbid", frozen=True)

        files: list[models.S3File]

        def get_s3_files(self):
            return self.files

        def set_s3_files(self, resolved):
            return self.model_copy(update={"files": [resolved[f.file] for f in self.files]})

    class _FakeInputWithFiles(PipelineInput):
        input: _EmptyParams = _EmptyParams()
        files: _FilesOnly

        def get_input_json(self, file_locations):
            return {}

    files = [
        models.S3File(file=f"mybucket/reads_{i}.fastq.gz")
        for i in range(10001)
    ]

    with pytest.raises(
        ValidationError,
        match=f"Too many input files: 10001 > 10000",
    ):
        _FakeInputWithFiles(files=_FilesOnly(files=files))


def test_pipeline_definition_validate_input_dict():
    d = _def(input_model=_FakeInputWithField)

    result = d.validate_input({"input": {"value": 5}, "files": {}})

    assert result == _FakeInputWithField(input=_ValueParams(value=5))


def test_pipeline_definition_validate_input_already_typed():
    d = _def(input_model=_FakeInputWithField)
    inp = _FakeInputWithField(input=_ValueParams(value=5))

    result = d.validate_input(inp)

    assert result is inp


def test_pipeline_definition_validate_input_fail():
    d = _def(input_model=_FakeInputWithField)

    with pytest.raises(PipelineInputValidationError) as e:
        d.validate_input({"files": {}})

    assert len(e.value.errors) == 1
    assert e.value.errors[0]["type"] == "missing"
    assert e.value.errors[0]["loc"] == ("input",)


def test_pipeline_definition_validate_input_then_build():
    d = _def(input_model=_FakeInputWithField)

    run = d.validate_input({"input": {"value": 5}, "files": {}}).get_input_json({})

    assert run == {"value": 5}


def test_pipeline_definition_validate_input_and_files_split():
    d = _def(input_model=_FakeInputWithField)

    result = d.validate_input({"value": 5}, {})

    assert result == _FakeInputWithField(input=_ValueParams(value=5))


def test_pipeline_definition_validate_input_and_files_split_fail():
    d = _def(input_model=_FakeInputWithField)

    with pytest.raises(PipelineInputValidationError) as e:
        d.validate_input({}, {})

    assert len(e.value.errors) == 1
    assert e.value.errors[0]["type"] == "missing"
    assert e.value.errors[0]["loc"] == ("input", "value")


def test_pipeline_definition_fail_input_model_not_subclass():
    class _NotAPipelineInput:
        pass

    with pytest.raises(ValueError, match="input_model must be a subclass of PipelineInput"):
        _def(input_model=_NotAPipelineInput)


def test_pipeline_definition_fail_input_field_not_pipeline_input_params():
    class _FilesModel(PipelineInputFiles):
        def get_s3_files(self):
            return []

        def set_s3_files(self, resolved):
            return self

    class _FakeInputBadInputType(PipelineInput):
        input: _FilesModel = _FilesModel()
        files: _EmptyFiles = _EmptyFiles()

        def get_input_json(self, file_locations):
            return {}

    with pytest.raises(
        ValueError,
        match="input_model's 'input' field must be a subclass of PipelineInputParams",
    ):
        _def(input_model=_FakeInputBadInputType)


def test_pipeline_definition_fail_files_field_not_pipeline_input_files():
    class _ParamsModel(PipelineInputParams):
        pass

    class _FakeInputBadFilesType(PipelineInput):
        input: _EmptyParams = _EmptyParams()
        files: _ParamsModel = _ParamsModel()

        def get_input_json(self, file_locations):
            return {}

    with pytest.raises(
        ValueError,
        match="input_model's 'files' field must be a subclass of PipelineInputFiles",
    ):
        _def(input_model=_FakeInputBadFilesType)


def test_pipeline_definition_fail_s3_file_in_input():
    class _S3FileParams(PipelineInputParams):
        model_config = ConfigDict(extra="forbid", frozen=True)

        bad: models.S3File

    class _FakeInputWithS3FileInInput(PipelineInput):
        input: _S3FileParams
        files: _EmptyFiles = _EmptyFiles()

        def get_input_json(self, file_locations):
            return {}

    with pytest.raises(
        ValueError, match=r"input model may not contain S3 file references: input\.bad"
    ):
        _def(input_model=_FakeInputWithS3FileInInput)


def test_pipeline_definition_fail_non_basemodel_field_in_input():
    class _NonBaseModelFieldParams(PipelineInputParams):
        model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

        bad: _NotABaseModel

    class _FakeInputWithNonBaseModelFieldInInput(PipelineInput):
        input: _NonBaseModelFieldParams
        files: _EmptyFiles = _EmptyFiles()

        def get_input_json(self, file_locations):
            return {}

    with pytest.raises(
        ValueError,
        match=r"field types must be pydantic BaseModels, enums, or JSON primitive "
            r"types: input\.bad",
    ):
        _def(input_model=_FakeInputWithNonBaseModelFieldInInput)


def test_pipeline_definition_fail_non_basemodel_field_in_files():
    class _NonBaseModelFieldFiles(PipelineInputFiles):
        model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

        bad: _NotABaseModel

        def get_s3_files(self):
            return []

        def set_s3_files(self, resolved):
            return self

    class _FakeInputWithNonBaseModelFieldInFiles(PipelineInput):
        input: _EmptyParams = _EmptyParams()
        files: _NonBaseModelFieldFiles

        def get_input_json(self, file_locations):
            return {}

    with pytest.raises(
        ValueError,
        match=r"field types must be pydantic BaseModels, enums, or JSON primitive "
            r"types: files\.bad",
    ):
        _def(input_model=_FakeInputWithNonBaseModelFieldInFiles)


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


def test_pipeline_definition_fail_empty_output_keys():
    with pytest.raises(ValueError, match="output_keys is required and may not be empty"):
        _def(output_keys=set())


def test_pipeline_definition_fail_output_keys_bad_value():
    with pytest.raises(ValueError, match="output_keys must contain only non-empty strings"):
        _def(output_keys={""})
