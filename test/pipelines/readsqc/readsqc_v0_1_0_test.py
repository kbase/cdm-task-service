from pathlib import Path

import pytest
import semver
from pydantic import ValidationError

from cdmtaskservice import models
from cdmtaskservice.pipelines.readsqc import v0_1_0
from cdmtaskservice.pipelines.readsqc.v0_1_0 import (
    ReadMode, ReadsQCFiles, ReadsQCInput, ReadsQCParams,
)

_F1 = models.S3File(file="bucket/reads_1.fastq.gz")
_F2 = models.S3File(file="bucket/reads_2.fastq.gz")


def _inp(read_mode, output_prefix, input_files, input_files2=None):
    files_kwargs = {"input_files2": input_files2}
    if input_files is not None:
        files_kwargs["input_files"] = input_files
    return ReadsQCInput(
        input=ReadsQCParams(read_mode=read_mode, output_prefix=output_prefix),
        files=ReadsQCFiles(**files_kwargs),
    )


def test_init():
    p = v0_1_0.init()

    assert p.name == "readsqc"
    assert p.version == semver.Version.parse("0.1.0")
    assert p.description == (
        "Illumina short read QC (adapter/contaminant/host removal, quality trimming) via "
        "BBTools, replicating the JGI QA protocol."
    )
    assert p.input_model is ReadsQCInput
    assert p.nersc_path == Path(
        "/global/cfs/cdirs/kbase/cdm_task_service/pipelines/readsqc/0.1.0/ReadsQC/"
    )
    assert p.main_wdl == "rqcfilter.wdl"
    assert set(p.file_md5s) == {
        "rqcfilter.wdl", "shortReadsqc.wdl", "longReadsqc.wdl", "sra2fastq.wdl",
    }
    assert p.output_keys == {
        "rqcfilter.filtered_final",
        "rqcfilter.filtered_stats_final",
        "rqcfilter.filtered_stats2_final",
        "rqcfilter.rqc_info",
        "rqcfilter.stats",
    }
    assert p.doc_urls == [
        "https://github.com/kbaseincubator/ReadsQC",
        "https://docs.microbiomedata.org/workflows/chapters/3_Metagenome_Reads_QC/",
    ]


def test_input_interleaved_minimal():
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", [_F1])

    assert inp.get_s3_files() == [_F1]


def test_input_interleaved_accepts_string_paths():
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", ["bucket/reads_1.fastq.gz"])

    assert inp.get_s3_files() == [_F1]
    assert inp.files.input_files == [_F1]


def test_input_paired_accepts_string_paths():
    inp = _inp(
        ReadMode.PAIRED,
        "proj-xyz",
        ["bucket/reads_1.fastq.gz"],
        ["bucket/reads_2.fastq.gz"],
    )

    assert inp.get_s3_files() == [_F1, _F2]
    assert inp.files.input_files == [_F1]
    assert inp.files.input_files2 == [_F2]


def test_input_interleaved_multiple_files():
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", [_F1, _F2])

    assert inp.get_s3_files() == [_F1, _F2]


def test_input_paired():
    inp = _inp(ReadMode.PAIRED, "proj-xyz", [_F1], [_F2])

    assert inp.get_s3_files() == [_F1, _F2]


def test_input_paired_multiple_files():
    f3 = models.S3File(file="bucket/reads_3.fastq.gz")
    f4 = models.S3File(file="bucket/reads_4.fastq.gz")
    inp = _inp(ReadMode.PAIRED, "proj-xyz", [_F1, f3], [_F2, f4])

    assert inp.get_s3_files() == [_F1, f3, _F2, f4]


def test_set_s3_files_interleaved():
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", [_F1])
    resolved_f1 = models.S3File(file=_F1.file, crc64nvme="cccccccccccc")

    new_inp = inp.set_s3_files({_F1.file: resolved_f1})

    assert new_inp.files.input_files == [resolved_f1]
    assert new_inp.files.input_files2 is None
    assert inp.files.input_files == [_F1]  # original is unchanged


def test_set_s3_files_paired():
    inp = _inp(ReadMode.PAIRED, "proj-xyz", [_F1], [_F2])
    resolved_f1 = models.S3File(file=_F1.file, crc64nvme="aaaaaaaaaaaa")
    resolved_f2 = models.S3File(file=_F2.file, crc64nvme="bbbbbbbbbbbb")

    new_inp = inp.set_s3_files({_F1.file: resolved_f1, _F2.file: resolved_f2})

    assert new_inp.files.input_files == [resolved_f1]
    assert new_inp.files.input_files2 == [resolved_f2]
    assert inp.files.input_files == [_F1]  # original is unchanged
    assert inp.files.input_files2 == [_F2]


def test_input_fail_no_input_files():
    with pytest.raises(ValidationError, match="input_files"):
        _inp(ReadMode.INTERLEAVED, "proj-xyz", None)


def test_input_fail_bad_path_reports_index_in_loc():
    with pytest.raises(ValidationError) as e:
        _inp(
            ReadMode.INTERLEAVED,
            "proj-xyz",
            ["bucket/reads_1.fastq.gz", "bad bucket!/reads_2.fastq.gz"],
        )

    assert e.value.errors()[0]["loc"] == ("input_files", 1, "file")


def test_input_fail_duplicate_files():
    with pytest.raises(
        ValidationError, match=r"Duplicate files in input: \['bucket/reads_2.fastq.gz'\]"
    ):
        _inp(ReadMode.PAIRED, "proj-xyz", [_F1, _F2], [_F2])


def test_input_fail_mismatched_paired_lengths():
    f3 = models.S3File(file="bucket/reads_3.fastq.gz")
    with pytest.raises(
        ValidationError, match="input_files and input_files2 must have equal length"
    ):
        _inp(ReadMode.PAIRED, "proj-xyz", [_F1, f3], [_F2])


def test_input_fail_paired_mode_without_paired_files():
    with pytest.raises(ValidationError, match="read_mode paired requires input_files2"):
        _inp(ReadMode.PAIRED, "proj-xyz", [_F1])


def test_input_fail_paired_files_wrong_mode():
    with pytest.raises(ValidationError, match="only valid for read_mode paired"):
        _inp(ReadMode.INTERLEAVED, "proj-xyz", [_F1], [_F2])


def test_input_fail_output_prefix_bad_chars():
    with pytest.raises(ValidationError, match="output_prefix"):
        _inp(ReadMode.INTERLEAVED, "proj:xyz", [_F1])


def test_build_interleaved():
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", [_F1])
    locs = {_F1.file: Path("/scratch/cache/aa/reads_1.fastq.gz")}

    run = inp.get_input_json(locs)

    assert run == {
        "rqcfilter.proj": "proj-xyz",
        "rqcfilter.interleaved": True,
        "rqcfilter.shortRead": True,
        "rqcfilter.rqcfilterdata": "/refdata/cdm_task_service/_pipelines/RQCFilterData/2026_09_10",
        "rqcfilter.input_files": ["/scratch/cache/aa/reads_1.fastq.gz"],
    }


def test_build_paired():
    inp = _inp(ReadMode.PAIRED, "proj-xyz", [_F1], [_F2])
    locs = {
        _F1.file: Path("/scratch/cache/aa/reads_1.fastq.gz"),
        _F2.file: Path("/scratch/cache/bb/reads_2.fastq.gz"),
    }

    run = inp.get_input_json(locs)

    assert run == {
        "rqcfilter.proj": "proj-xyz",
        "rqcfilter.interleaved": False,
        "rqcfilter.shortRead": True,
        "rqcfilter.rqcfilterdata": "/refdata/cdm_task_service/_pipelines/RQCFilterData/2026_09_10",
        "rqcfilter.input_fq1": ["/scratch/cache/aa/reads_1.fastq.gz"],
        "rqcfilter.input_fq2": ["/scratch/cache/bb/reads_2.fastq.gz"],
    }


def test_build_fail_missing_file_location():
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", [_F1])

    with pytest.raises(
        ValueError, match="No file location provided for S3 file 'bucket/reads_1.fastq.gz'"
    ):
        inp.get_input_json({})


def test_init_validate_input_then_get_input_json():
    p = v0_1_0.init()
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", [_F1])
    locs = {_F1.file: Path("/scratch/cache/aa/reads_1.fastq.gz")}

    run = p.validate_input(inp).get_input_json(locs)

    assert run == {
        "rqcfilter.proj": "proj-xyz",
        "rqcfilter.interleaved": True,
        "rqcfilter.shortRead": True,
        "rqcfilter.rqcfilterdata": "/refdata/cdm_task_service/_pipelines/RQCFilterData/2026_09_10",
        "rqcfilter.input_files": ["/scratch/cache/aa/reads_1.fastq.gz"],
    }
