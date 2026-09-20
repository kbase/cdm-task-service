from pathlib import Path

import pytest
import semver
from pydantic import ValidationError

from cdmtaskservice import models
from cdmtaskservice.pipelines.readsqc import v0_1_0
from cdmtaskservice.pipelines.readsqc.v0_1_0 import ReadMode, ReadsQCInput

_F1 = models.S3File(file="bucket/reads_1.fastq.gz")
_F2 = models.S3File(file="bucket/reads_2.fastq.gz")


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
    assert p.doc_urls == [
        "https://github.com/kbaseincubator/ReadsQC",
        "https://docs.microbiomedata.org/workflows/chapters/3_Metagenome_Reads_QC/",
    ]


def test_input_interleaved_minimal():
    inp = ReadsQCInput(
        input_files=[_F1], output_prefix="proj-xyz", read_mode=ReadMode.INTERLEAVED,
    )

    assert inp.get_s3_files() == [_F1]


def test_input_interleaved_accepts_string_paths():
    inp = ReadsQCInput(
        input_files=["bucket/reads_1.fastq.gz"],
        output_prefix="proj-xyz",
        read_mode=ReadMode.INTERLEAVED,
    )

    assert inp.get_s3_files() == [_F1]
    assert inp.input_files == [_F1]


def test_input_paired_accepts_string_paths():
    inp = ReadsQCInput(
        input_files=["bucket/reads_1.fastq.gz"],
        input_files2=["bucket/reads_2.fastq.gz"],
        output_prefix="proj-xyz",
        read_mode=ReadMode.PAIRED,
    )

    assert inp.get_s3_files() == [_F1, _F2]
    assert inp.input_files == [_F1]
    assert inp.input_files2 == [_F2]


def test_input_interleaved_multiple_files():
    inp = ReadsQCInput(
        input_files=[_F1, _F2], output_prefix="proj-xyz", read_mode=ReadMode.INTERLEAVED,
    )

    assert inp.get_s3_files() == [_F1, _F2]


def test_input_paired():
    inp = ReadsQCInput(
        input_files=[_F1], input_files2=[_F2], output_prefix="proj-xyz", read_mode=ReadMode.PAIRED,
    )

    assert inp.get_s3_files() == [_F1, _F2]


def test_input_paired_multiple_files():
    f3 = models.S3File(file="bucket/reads_3.fastq.gz")
    f4 = models.S3File(file="bucket/reads_4.fastq.gz")
    inp = ReadsQCInput(
        input_files=[_F1, f3],
        input_files2=[_F2, f4],
        output_prefix="proj-xyz",
        read_mode=ReadMode.PAIRED,
    )

    assert inp.get_s3_files() == [_F1, f3, _F2, f4]


def test_set_s3_files_interleaved():
    inp = ReadsQCInput(
        input_files=[_F1], output_prefix="proj-xyz", read_mode=ReadMode.INTERLEAVED,
    )
    resolved_f1 = models.S3File(file=_F1.file, crc64nvme="cccccccccccc")

    new_inp = inp.set_s3_files({_F1.file: resolved_f1})

    assert new_inp.input_files == [resolved_f1]
    assert new_inp.input_files2 is None
    assert inp.input_files == [_F1]  # original is unchanged


def test_set_s3_files_paired():
    inp = ReadsQCInput(
        input_files=[_F1], input_files2=[_F2], output_prefix="proj-xyz", read_mode=ReadMode.PAIRED,
    )
    resolved_f1 = models.S3File(file=_F1.file, crc64nvme="aaaaaaaaaaaa")
    resolved_f2 = models.S3File(file=_F2.file, crc64nvme="bbbbbbbbbbbb")

    new_inp = inp.set_s3_files({_F1.file: resolved_f1, _F2.file: resolved_f2})

    assert new_inp.input_files == [resolved_f1]
    assert new_inp.input_files2 == [resolved_f2]
    assert inp.input_files == [_F1]  # original is unchanged
    assert inp.input_files2 == [_F2]


def test_input_fail_no_input_files():
    with pytest.raises(ValidationError, match="input_files"):
        ReadsQCInput(output_prefix="proj-xyz", read_mode=ReadMode.INTERLEAVED)


def test_input_fail_bad_path_reports_index_in_loc():
    with pytest.raises(ValidationError) as e:
        ReadsQCInput(
            input_files=["bucket/reads_1.fastq.gz", "bad bucket!/reads_2.fastq.gz"],
            output_prefix="proj-xyz",
            read_mode=ReadMode.INTERLEAVED,
        )

    assert e.value.errors()[0]["loc"] == ("input_files", 1, "file")


def test_input_fail_duplicate_files():
    with pytest.raises(
        ValidationError, match=r"Duplicate files in input: \['bucket/reads_2.fastq.gz'\]"
    ):
        ReadsQCInput(
            input_files=[_F1, _F2],
            input_files2=[_F2],
            output_prefix="proj-xyz",
            read_mode=ReadMode.PAIRED,
        )


def test_input_fail_mismatched_paired_lengths():
    f3 = models.S3File(file="bucket/reads_3.fastq.gz")
    with pytest.raises(
        ValidationError, match="input_files and input_files2 must have equal length"
    ):
        ReadsQCInput(
            input_files=[_F1, f3],
            input_files2=[_F2],
            output_prefix="proj-xyz",
            read_mode=ReadMode.PAIRED,
        )


def test_input_fail_paired_mode_without_paired_files():
    with pytest.raises(ValidationError, match="read_mode paired requires input_files2"):
        ReadsQCInput(
            input_files=[_F1], output_prefix="proj-xyz", read_mode=ReadMode.PAIRED,
        )


def test_input_fail_paired_files_wrong_mode():
    with pytest.raises(ValidationError, match="only valid for read_mode paired"):
        ReadsQCInput(
            input_files=[_F1],
            input_files2=[_F2],
            output_prefix="proj-xyz",
            read_mode=ReadMode.INTERLEAVED,
        )


def test_input_fail_output_prefix_bad_chars():
    with pytest.raises(ValidationError, match="output_prefix"):
        ReadsQCInput(
            input_files=[_F1], output_prefix="proj:xyz", read_mode=ReadMode.INTERLEAVED,
        )


def test_build_interleaved():
    inp = ReadsQCInput(
        input_files=[_F1],
        output_prefix="proj-xyz",
        read_mode=ReadMode.INTERLEAVED,
    )
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
    inp = ReadsQCInput(
        input_files=[_F1],
        input_files2=[_F2],
        output_prefix="proj-xyz",
        read_mode=ReadMode.PAIRED,
    )
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
    inp = ReadsQCInput(
        input_files=[_F1], output_prefix="proj-xyz", read_mode=ReadMode.INTERLEAVED,
    )

    with pytest.raises(
        ValueError, match="No file location provided for S3 file 'bucket/reads_1.fastq.gz'"
    ):
        inp.get_input_json({})


def test_init_validate_input_then_get_input_json():
    p = v0_1_0.init()
    inp = ReadsQCInput(
        input_files=[_F1], output_prefix="proj-xyz", read_mode=ReadMode.INTERLEAVED,
    )
    locs = {_F1.file: Path("/scratch/cache/aa/reads_1.fastq.gz")}

    run = p.validate_input(inp).get_input_json(locs)

    assert run == {
        "rqcfilter.proj": "proj-xyz",
        "rqcfilter.interleaved": True,
        "rqcfilter.shortRead": True,
        "rqcfilter.rqcfilterdata": "/refdata/cdm_task_service/_pipelines/RQCFilterData/2026_09_10",
        "rqcfilter.input_files": ["/scratch/cache/aa/reads_1.fastq.gz"],
    }
