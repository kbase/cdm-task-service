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

from cdmtaskservice import models
from cdmtaskservice.arg_checkers import not_falsy as _not_falsy, require_string as _require_string


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


class PipelineInput(BaseModel, abc.ABC):
    """
    Base class for a pipeline version's typed job input. Each pipeline version defines a
    concrete subclass describing exactly the inputs it needs.
    """
    model_config = ConfigDict(extra="forbid", frozen=True)

    @abc.abstractmethod
    def get_s3_files(self) -> list[models.S3File]:
        """
        Return every S3 file referenced anywhere in this input, so CTS can resolve their
        checksums and stage them at NERSC before the pipeline runs. Order is insignificant but
        must be stable enough to zip back up with the corresponding file locations.
        """

    @abc.abstractmethod
    def set_s3_files(self, resolved: dict[str, models.S3File]) -> Self:
        """
        Return a new instance of this input with every S3 file returned by get_s3_files
        replaced by its counterpart in resolved, e.g. to fill in a checksum CTS resolved from
        S3. Does not modify this instance.

        resolved - a mapping of S3 path (the `file` field of models.S3File) to the resolved
            file. Contains an entry for every file returned by get_s3_files.
        """

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

    @model_validator(mode="after")
    def _check_no_duplicate_files(self) -> Self:
        paths = [f.file for f in self.get_s3_files()]
        dupes = {p for p in paths if paths.count(p) > 1}
        if dupes:
            raise ValueError(f"Duplicate files in input: {sorted(dupes)}")
        return self


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

    doc_urls: list[str] = dataclasses.field(default_factory=list)
    """ URLs to documentation for this pipeline version, if any. Defaults to empty. """

    def __post_init__(self):
        _require_string(self.name, "name")
        _not_falsy(self.version, "version")
        if not isinstance(self.version, semver.Version):
            raise ValueError("version must be a semver.Version instance")
        _require_string(self.description, "description")
        _not_falsy(self.input_model, "input_model")
        _not_falsy(self.nersc_path, "nersc_path")
        _require_string(self.main_wdl, "main_wdl")
        if not self.file_md5s:
            raise ValueError("file_md5s is required and may not be empty")
        if self.main_wdl not in self.file_md5s:
            raise ValueError(f"main_wdl '{self.main_wdl}' must have an entry in file_md5s")
        object.__setattr__(self, "file_md5s", types.MappingProxyType(dict(self.file_md5s)))

    def validate_input(
        self, pipeline_input: dict[str, Any] | PipelineInput
    ) -> PipelineInput:
        """
        Validate untyped input against this pipeline version's input model.

        pipeline_input - the raw input to validate, e.g. a dict parsed from a request body. An
            already-validated instance of input_model is also accepted and is used as-is.

        Returns the validated input.

        Raises PipelineInputValidationError, with detail in the same shape as a Pydantic
        validation error, if pipeline_input does not conform to input_model.
        """
        try:
            return self.input_model.model_validate(pipeline_input)
        except ValidationError as e:
            # Wrapped rather than left as a bare pydantic.ValidationError so the HTTP layer can
            # convert *this specific* failure into a client-facing request validation error
            # without having to treat every ValidationError anywhere in the app the same way -
            # e.g. one raised while validating internal data would be a server bug, not bad input.
            raise PipelineInputValidationError(e.errors()) from e
