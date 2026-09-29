"""
The contract each version of a pre-written pipeline implements in order to be runnable by CTS
via JAWS at NERSC.

Pipelines differ from registered Docker images in that they are not registered dynamically -
each version is a PipelineDefinition instance built by a small `init()` function living under
cdmtaskservice/pipelines/<pipeline name>/, and the WDL bundle it points to is pre-staged at
NERSC out of band (e.g. by an admin), not uploaded by CTS.
"""

from __future__ import annotations

import abc
import dataclasses
import types
from pathlib import Path
from typing import Any, Self

import semver
from pydantic import BaseModel, ConfigDict, ValidationError, model_validator

from cdmtaskservice import models, pydantic_type_walker
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string
from cdmtaskservice.json_schema_readable import simplify_schema


class PipelineInputValidationError(Exception):
    """
    Thrown when a pipeline's input fails validation against its input model. Carries error
    detail in the same shape as pydantic.ValidationError.errors() /
    fastapi.exceptions.RequestValidationError.errors() - a list of dicts with, at minimum,
    `type`, `loc`, and `msg` keys - so it can be converted directly into a
    RequestValidationError for a client-facing response.
    """

    def __init__(self, errors: list[dict[str, Any]]):
        super().__init__(f"Pipeline input validation failed: {len(errors)} error(s)")
        self.errors = errors


class PipelineInputParams(BaseModel):
    """
    Base class for a pipeline version's non-file input parameters. Each pipeline version defines
    a concrete subclass describing exactly the parameters it needs. May not contain an
    `models.S3File` anywhere in its structure, so that it's always safe to include in a job
    preview - checked at pipeline registration time.
    """
    model_config = ConfigDict(extra="forbid", frozen=True)


class PipelineInputFiles(BaseModel, abc.ABC):
    """
    Base class for a pipeline version's file references. Each pipeline version defines a
    concrete subclass describing exactly the file references it needs. May contain
    `models.S3File`(s) anywhere in its structure. Omitted from job previews.
    """
    model_config = ConfigDict(extra="forbid", frozen=True)

    @abc.abstractmethod
    def get_s3_files(self) -> list[models.S3File]:
        """
        Return every S3 file referenced anywhere in this instance. Order is insignificant but
        must be stable.
        """

    @abc.abstractmethod
    def set_s3_files(self, resolved: dict[str, models.S3File]) -> Self:
        """
        Return a new instance with every S3 file returned by get_s3_files replaced by its
        counterpart in resolved, e.g. to fill in a checksum CTS resolved from S3. Does not
        modify this instance.

        resolved - a mapping of S3 path (the `file` field of models.S3File) to the resolved
            file. Contains an entry for every file returned by get_s3_files.
        """

    @model_validator(mode="after")
    def _check_no_duplicate_files(self) -> Self:
        paths = [f.file for f in self.get_s3_files()]
        dupes = {p for p in paths if paths.count(p) > 1}
        if dupes:
            raise ValueError(f"Duplicate files in input: {sorted(dupes)}")
        return self

    @model_validator(mode="after")
    def _check_max_files(self) -> Self:
        count = len(self.get_s3_files())
        if count > models.MAX_INPUT_FILES_PER_JOB:
            raise ValueError(
                f"Too many input files: {count} > {models.MAX_INPUT_FILES_PER_JOB}"
            )
        return self


class PipelineInput(BaseModel, abc.ABC):
    """
    Base class for a pipeline version's typed job input. Each pipeline version defines a
    concrete subclass describing exactly the inputs it needs, declaring exactly two fields:

    * `input` - a subclass of PipelineInputParams.
    * `files` - a subclass of PipelineInputFiles.
    """
    model_config = ConfigDict(extra="forbid", frozen=True)

    input: PipelineInputParams
    files: PipelineInputFiles

    def get_s3_files(self) -> list[models.S3File]:
        """
        Return every S3 file referenced anywhere in this input, so CTS can resolve their
        checksums and stage them at NERSC before the pipeline runs. Order is insignificant but
        must be stable enough to zip back up with the corresponding file locations.
        """
        return self.files.get_s3_files()

    def set_s3_files(self, resolved: dict[str, models.S3File]) -> Self:
        """
        Return a new instance of this input with every S3 file returned by get_s3_files
        replaced by its counterpart in resolved, e.g. to fill in a checksum CTS resolved from
        S3. Does not modify this instance.

        resolved - a mapping of S3 path (the `file` field of models.S3File) to the resolved
            file. Contains an entry for every file returned by get_s3_files.
        """
        return self.model_copy(update={"files": self.files.set_s3_files(resolved)})

    @abc.abstractmethod
    def get_input_json(self, file_locations: dict[str, Path]) -> dict[str, Any]:
        """
        Build the WDL / Cromwell / JAWS input.json for a run of this pipeline version
        with this input.

        file_locations - a mapping of S3 path (the `file` field of models.S3File) to the
            absolute path at which the file will be available at NERSC. Contains an entry for
            every file returned by get_s3_files().

        May raise ValueError, e.g. if file_locations is missing an entry for one of this input's
        S3 files.
        """


@dataclasses.dataclass(frozen=True)
class PipelineDefinition:
    """
    Describes how to run one version of a pre-written pipeline via JAWS at NERSC, including the
    pre-staged WDL bundle it runs. Built by each pipeline version's `init()` function - see e.g.
    cdmtaskservice/pipelines/readsqc/v0_1_0.py.
    """

    name: str
    """ The pipeline's name. Combined with version, forms its unique identifier. """

    version: semver.Version
    """ This pipeline definition's version, as a semver.Version, e.g. semver.Version(0, 1, 0). """

    description: str
    """ A human readable description of the pipeline version. """

    input_model: type[PipelineInput]
    """ The pydantic model describing this pipeline version's input shape. """

    nersc_path: Path
    """
    The directory at NERSC containing the pre-staged WDL bundle for this pipeline version. CTS
    never writes to this path - it's staged out of band and only ever read from and checksummed
    by CTS.
    """

    main_wdl: str
    """ The filename, relative to nersc_path, of the top level WDL to submit to JAWS. """

    file_md5s: types.MappingProxyType[str, str]
    """
    An immutable mapping of every relevant file in the pipeline (typically the main and imported
    WDLs), relative to nersc_path, to its expected MD5. Checked
    against the remote files prior to every JAWS submission so silent drift in the pre-staged
    bundle (e.g. a file replaced or edited outside the normal staging process) is caught early
    rather than producing a confusing downstream JAWS failure.
    """
    # TODO PIPELINES could also md5 reference data

    output_keys: frozenset[str]
    """
    The set of top-level outputs.json keys that are File outputs to checksum and upload to S3.
    JAWS/Cromwell serializes File and String/Array[String] outputs identically as JSON, so this
    allow-list is the only way to distinguish them. Only scalar File / File? outputs are
    supported; an Array[File] output will cause an error if allow-listed.
    """

    doc_urls: list[str] = dataclasses.field(default_factory=list)
    """ URLs to documentation for this pipeline version, if any. Defaults to empty. """

    input_schema: dict[str, Any] = dataclasses.field(init=False)
    """
    The shape of this pipeline version's input parameters, excluding file references. The input
    must match the shape described by this schema.
    """

    files_schema: dict[str, Any] = dataclasses.field(init=False)
    """
    The shape of this pipeline version's file references. The file input must match the shape
    described by this schema.
    """

    def __post_init__(self):
        _require_string(self.name, "name")
        _not_falsy(self.version, "version")
        if not isinstance(self.version, semver.Version):
            raise ValueError("version must be a semver.Version instance")
        _require_string(self.description, "description")
        _not_falsy(self.input_model, "input_model")
        if not (isinstance(self.input_model, type) and issubclass(self.input_model, PipelineInput)):
            raise ValueError("input_model must be a subclass of PipelineInput")
        input_field = self.input_model.model_fields["input"].annotation
        if not (isinstance(input_field, type) and issubclass(input_field, PipelineInputParams)):
            raise ValueError("input_model's 'input' field must be a subclass of PipelineInputParams")
        files_field = self.input_model.model_fields["files"].annotation
        if not (isinstance(files_field, type) and issubclass(files_field, PipelineInputFiles)):
            raise ValueError("input_model's 'files' field must be a subclass of PipelineInputFiles")
        try:
            pydantic_type_walker.check_annotation(
                input_field, "input", disallowed_types=(models.S3File,)
            )
        except pydantic_type_walker.DisallowedTypeError as e:
            raise ValueError(f"input model may not contain S3 file references: {e.path}") from e
        pydantic_type_walker.check_annotation(files_field, "files")
        object.__setattr__(self, "input_schema", simplify_schema(input_field.model_json_schema()))
        object.__setattr__(self, "files_schema", simplify_schema(files_field.model_json_schema()))
        _not_falsy(self.nersc_path, "nersc_path")
        _require_string(self.main_wdl, "main_wdl")
        if not self.file_md5s:
            raise ValueError("file_md5s is required and may not be empty")
        if self.main_wdl not in self.file_md5s:
            raise ValueError(f"main_wdl '{self.main_wdl}' must have an entry in file_md5s")
        object.__setattr__(self, "file_md5s", types.MappingProxyType(dict(self.file_md5s)))
        if not self.output_keys:
            raise ValueError("output_keys is required and may not be empty")
        if not all(isinstance(k, str) and k.strip() for k in self.output_keys):
            raise ValueError("output_keys must contain only non-empty strings")
        object.__setattr__(self, "output_keys", frozenset(self.output_keys))

    def validate_input(
        self,
        pipeline_input: dict[str, Any] | PipelineInput,
        files: dict[str, Any] | None = None,
    ) -> PipelineInput:
        """
        Validate untyped input against this pipeline version's input model.

        pipeline_input - the raw input to validate, e.g. a dict parsed from a request body. An
            already-validated instance of input_model is also accepted and is used as-is. If
            files is provided, this is instead just the contents of the input model's `input`
            field, and the two are reassembled into the full input prior to validation.
        files - the contents of the input model's `files` field. If provided, pipeline_input
            must not already be a full input dict or an input_model instance.

        Returns the validated input.

        Raises PipelineInputValidationError, with detail in the same shape as a Pydantic
        validation error, if pipeline_input does not conform to input_model.
        """
        if files is not None:
            pipeline_input = {"input": pipeline_input, "files": files}
        try:
            return self.input_model.model_validate(pipeline_input)
        except ValidationError as e:
            # Wrapped rather than left as a bare pydantic.ValidationError so the HTTP layer can
            # convert *this specific* failure into a client-facing request validation error
            # without having to treat every ValidationError anywhere in the app the same way -
            # e.g. one raised while validating internal data would be a server bug, not bad input.
            raise PipelineInputValidationError(e.errors()) from e
