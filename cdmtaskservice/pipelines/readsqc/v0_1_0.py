"""
ReadsQC (rqcfilter) pipeline, version 0.1.0.

Source: https://github.com/kbaseIncubator/ReadsQC
WDL MD5s below are for commit deb2c9fd63af51f8d512f38ed089c0d41da8325e of that repo. If the
WDLs are updated before being (re)staged at NERSC, recompute these and bump the version here.

rqcfilter.wdl routes to one of two sub-workflows based on `shortRead`/`interleaved`. This version
only supports the short read (Illumina) path, in one of two layouts selected via `read_mode`:
  * interleaved: one or more interleaved files via input_files
  * paired: forward reads via input_files, reverse reads via input_files2

Long read (PacBio) input, SRA accession input, and the optional long-read `reference` input are
not supported by this version.

CTS does not track or manage the RQCFilterData reference database used by this pipeline - see
_RQCFILTERDATA_PATH below. That path must already be present inside the container at runtime,
which for this WDL means it must be visible via a site-wide mount at the JAWS/NERSC site the
pipeline runs at; it is not requested per-job the way CTS-generated WDLs request refdata mounts.
"""

import enum
from pathlib import Path
from typing import Self

import semver
from pydantic import Field, model_validator

from cdmtaskservice import models
from cdmtaskservice.pipelines.definition import PipelineDefinition, PipelineInput, PipelineRun

_WORKFLOW = "rqcfilter"


_NERSC_WDL_PATH = Path("/global/cfs/cdirs/kbase/cdm_task_service/pipelines/readsqc/0.1.0/ReadsQC/")


_FILE_MD5S = {
    "rqcfilter.wdl": "29e87aaf8f0b419fc499d64bfed71ba0",
    "shortReadsqc.wdl": "03a4e9541e521bc48917d2439243ce54",
    "longReadsqc.wdl": "380d0c4bfbd51e7202d5ebd51bb108d1",
    "sra2fastq.wdl": "6a0442dda8a60543a76b3398ca1e2c65",
}


_RQCFILTERDATA_PATH = Path("/cdm_task_service/_pipelines/RQCFilterData/2026_09_10/")


_DESCRIPTION = (
    "Illumina short read QC (adapter/contaminant/host removal, quality trimming) via BBTools, "
    "replicating the JGI QA protocol."
)

_DOC_URLS = [
    "https://github.com/kbaseincubator/ReadsQC",
    "https://docs.microbiomedata.org/workflows/chapters/3_Metagenome_Reads_QC/",
]


class ReadMode(str, enum.Enum):
    """ The mode in which the pipeline should run. """

    INTERLEAVED = "interleaved"
    PAIRED = "paired"


# WDL rqcfilter.interleaved value for each ReadMode. rqcfilter.shortRead is always True since
# long read support is not currently offered by this pipeline version.
_READ_MODE_TO_INTERLEAVED = {
    ReadMode.INTERLEAVED: True,
    ReadMode.PAIRED: False,
    # add long reads later
}


class ReadsQCInput(PipelineInput):
    """ Input for the ReadsQC (rqcfilter) pipeline, version 0.1.0. """

    read_mode: ReadMode = Field(
        description="The mode in which the pipeline should run. "
            "interleaved takes one or more interleaved "
            "files via input_files; paired takes forward reads via input_files and reverse "
            "reads via input_files2. "
            "Files are concatenated together prior to analysis.",
    )
    input_files: list[models.S3File] = Field(
        description="For read_mode interleaved, one or more interleaved paired-end read files. "
            "For read_mode paired, the forward read files, paired index-for-index with "
            "input_files2. Either a list of file path strings or a list of data structures "
            "including the file path and optionally a CRC64/NVME checksum. When returned from "
            "the service, always returned as data structures with a checksum.",
        min_length=1,
    )
    input_files2: list[models.S3File] | None = Field(
        default=None,
        description="Reverse read files, paired index-for-index with input_files. Only valid, "
            "and required, for read_mode paired. Either a list of file path strings or a list "
            "of data structures including the file path and optionally a CRC64/NVME checksum. "
            "When returned from the service, always returned as data structures with a "
            "checksum.",
    )
    output_prefix: str = Field(
        description="A prefix for output files from the pipeline. Restricted to word "
            "characters, dots, and hyphens to keep the resulting file names sane.",
        min_length=1,
        max_length=256,
        pattern=r"^[\w][\w.-]*$",
    )

    @model_validator(mode="after")
    def _check_input_mode(self) -> Self:
        has_rev = bool(self.input_files2)
        if self.read_mode == ReadMode.PAIRED:
            if not has_rev:
                raise ValueError("read_mode paired requires input_files2")
            if len(self.input_files) != len(self.input_files2):
                raise ValueError(
                    "input_files and input_files2 must have equal length for read_mode paired"
                )
        elif has_rev:
            raise ValueError("input_files2 is only valid for read_mode paired")
        return self

    def get_s3_files(self) -> list[models.S3File]:
        files = list(self.input_files)
        files.extend(self.input_files2 or [])
        return files

    def set_s3_files(self, resolved: dict[str, models.S3File]) -> Self:
        update = {"input_files": [resolved[f.file] for f in self.input_files]}
        if self.input_files2:
            update["input_files2"] = [resolved[f.file] for f in self.input_files2]
        return self.model_copy(update=update)


def build(pipeline_input: ReadsQCInput, file_locations: dict[str, Path]) -> PipelineRun:
    """ Build the JAWS input.json for a ReadsQC (rqcfilter) 0.1.0 run. """
    def loc(f: models.S3File) -> str:
        if f.file not in file_locations:
            raise ValueError(f"No file location provided for S3 file '{f.file}'")
        return str(file_locations[f.file])

    input_json = {
        f"{_WORKFLOW}.proj": pipeline_input.output_prefix,
        f"{_WORKFLOW}.interleaved": _READ_MODE_TO_INTERLEAVED[pipeline_input.read_mode],
        f"{_WORKFLOW}.shortRead": True,
        f"{_WORKFLOW}.rqcfilterdata": str(_RQCFILTERDATA_PATH),
    }
    if pipeline_input.read_mode == ReadMode.PAIRED:
        input_json[f"{_WORKFLOW}.input_fq1"] = [loc(f) for f in pipeline_input.input_files]
        input_json[f"{_WORKFLOW}.input_fq2"] = [loc(f) for f in pipeline_input.input_files2]
    else:
        input_json[f"{_WORKFLOW}.input_files"] = [loc(f) for f in pipeline_input.input_files]
    return PipelineRun(input_json=input_json)


def init() -> PipelineDefinition:
    """ Build the PipelineDefinition for ReadsQC (rqcfilter), version 0.1.0. """
    return PipelineDefinition(
        name="readsqc",
        version=semver.Version(0, 1, 0),
        description=_DESCRIPTION,
        input_model=ReadsQCInput,
        nersc_path=_NERSC_WDL_PATH,
        main_wdl="rqcfilter.wdl",
        file_md5s=_FILE_MD5S,
        _build_fn=build,
        doc_urls=_DOC_URLS,
    )
