"""
Metagenome Assembly (jgi_metaAssembly) pipeline, version 0.1.0.

Source: https://github.com/kbaseincubator/metaAssembly
WDL MD5s below are for commit 5e11066d7c9d47022fa6948dbc8637b172f59488 of that repo.

jgi_assembly.wdl (workflow jgi_metaAssembly) routes to a short-read (BBTools + metaSPAdes) or
long-read (Flye) sub-workflow based on the `shortRead` boolean input. This version only supports
the short-read (Illumina) path (`shortRead` is always set to `true`), in one of two layouts
selected via `read_mode`:
  * interleaved: a single interleaved paired-end file via input_file
  * paired: a single forward read file via input_file, paired with a single reverse read file
    via input_file2

The short-read WDL path only handles these two shapes safely - a lone file is used as-is, and
exactly two files are interleaved positionally (input_files[0]=forward, input_files[1]=reverse)
by make_interleaved_reads.wdl, with any further files silently ignored - so this version enforces
those exact file counts rather than passing that risk on to callers.

Long read (PacBio) input is not supported by this version.

`threads` is always hardcoded to "16" below and `memory` is always left unset in the generated
input.json - see README.md in this package for why.
"""

import enum
from pathlib import Path
from typing import Any, Self

import semver
from pydantic import ConfigDict, Field, model_validator

from cdmtaskservice import models
from cdmtaskservice.pipelines.definition import (
    PipelineDefinition,
    PipelineInput,
    PipelineInputFiles,
    PipelineInputParams,
)

_WORKFLOW = "jgi_metaAssembly"


_NERSC_WDL_PATH = Path(
    "/global/cfs/cdirs/kbase/cdm_task_service/pipelines/metaassembly/0.1.0/metaAssembly/"
)


_FILE_MD5S = {
    "jgi_assembly.wdl": "c84194b4d7c763b5bd7496b3b8930686",
    "shortReads_assembly.wdl": "2d8060df4e2027e55947f1c2fe9345e8",
    "make_interleaved_reads.wdl": "5a70ad09514da5845af6ba79af1856f4",
}


_OUTPUT_KEYS = frozenset({
    f"{_WORKFLOW}.sr_contig",
    f"{_WORKFLOW}.sr_scaffold",
    f"{_WORKFLOW}.sr_agp",
    f"{_WORKFLOW}.sr_bam",
    f"{_WORKFLOW}.sr_samgz",
    f"{_WORKFLOW}.sr_covstats",
    f"{_WORKFLOW}.sr_asminfo",
    f"{_WORKFLOW}.sr_bbcms_fq",
    f"{_WORKFLOW}.stats",
})


_THREADS = "16"
""" Always passed as the workflow's `threads` input - see README.md in this package. """


_DESCRIPTION = (
    "Metagenome assembly of Illumina short reads: error correction via BBTools (bbcms), "
    "assembly via metaSPAdes, and coverage mapping of reads back to the assembly via bbmap."
)

_DOC_URLS = [
    "https://github.com/kbaseincubator/metaAssembly",
    "https://docs.microbiomedata.org/workflows/chapters/4_Metagenome_Assembly/",
]


class ReadMode(str, enum.Enum):
    """ The mode in which the pipeline should run. """

    INTERLEAVED = "interleaved"
    PAIRED = "paired"


class MetaAssemblyParams(PipelineInputParams):
    """ Non-file input parameters for the Metagenome Assembly (jgi_metaAssembly) pipeline. """
    model_config = ConfigDict(extra="forbid", frozen=True)

    read_mode: ReadMode = Field(
        description="The mode in which the pipeline should run. interleaved takes a single "
            "interleaved paired-end file via input_file; paired takes a single forward "
            "read file via input_file and a single reverse read file via input_file2.",
    )
    output_prefix: str = Field(
        description="A prefix for output files from the pipeline. Restricted to word "
            "characters, dots, and hyphens to keep the resulting file names sane.",
        min_length=1,
        max_length=256,
        pattern=r"^[\w][\w.-]*$",
    )


class MetaAssemblyFiles(PipelineInputFiles):
    """ File inputs for the Metagenome Assembly (jgi_metaAssembly) pipeline. """
    model_config = ConfigDict(extra="forbid", frozen=True)

    input_file: models.S3File = Field(
        description="For read_mode interleaved, the single interleaved paired-end read "
            "file (e.g. the output of the ReadsQC pipeline). For read_mode paired, the single "
            "forward read file, paired with input_file2. Either a file path string or a data "
            "structure including the file path and optionally a CRC64/NVME checksum. When "
            "returned from the service, always returned as a data structure with a checksum.",
    )
    input_file2: models.S3File | None = Field(
        default=None,
        description="The single reverse read file, paired with input_file. Only valid, and "
            "required, for read_mode paired. Either a file path string or a data structure "
            "including the file path and optionally a CRC64/NVME checksum. When returned from "
            "the service, always returned as a data structure with a checksum.",
    )

    def get_s3_files(self) -> list[models.S3File]:
        files = [self.input_file]
        if self.input_file2:
            files.append(self.input_file2)
        return files

    def set_s3_files(self, resolved: dict[str, models.S3File]) -> Self:
        update = {"input_file": resolved[self.input_file.file]}
        if self.input_file2:
            update["input_file2"] = resolved[self.input_file2.file]
        return self.model_copy(update=update)


class MetaAssemblyInput(PipelineInput):
    """ Input for the Metagenome Assembly (jgi_metaAssembly) pipeline. """

    input: MetaAssemblyParams
    files: MetaAssemblyFiles

    @model_validator(mode="after")
    def _check_input_mode(self) -> Self:
        has_rev = bool(self.files.input_file2)
        if self.input.read_mode == ReadMode.PAIRED:
            if not has_rev:
                raise ValueError("read_mode paired requires input_file2")
        elif has_rev:
            raise ValueError("input_file2 is only valid for read_mode paired")
        return self

    def get_input_json(self, file_locations: dict[str, Path]) -> dict[str, Any]:
        def loc(f: models.S3File) -> str:
            if f.file not in file_locations:
                raise ValueError(f"No file location provided for S3 file '{f.file}'")
            return str(file_locations[f.file])

        if self.input.read_mode == ReadMode.PAIRED:
            input_files = [loc(self.files.input_file), loc(self.files.input_file2)]
        else:
            input_files = [loc(self.files.input_file)]
        return {
            f"{_WORKFLOW}.proj": self.input.output_prefix,
            f"{_WORKFLOW}.shortRead": True,
            f"{_WORKFLOW}.threads": _THREADS,
            f"{_WORKFLOW}.input_files": input_files,
        }


def init() -> PipelineDefinition:
    """ Build the PipelineDefinition for Metagenome Assembly (jgi_metaAssembly), version 0.1.0. """
    return PipelineDefinition(
        name="metaassembly",
        version=semver.Version(0, 1, 0),
        description=_DESCRIPTION,
        input_model=MetaAssemblyInput,
        nersc_path=_NERSC_WDL_PATH,
        main_wdl="jgi_assembly.wdl",
        file_md5s=_FILE_MD5S,
        output_keys=_OUTPUT_KEYS,
        doc_urls=_DOC_URLS,
    )
