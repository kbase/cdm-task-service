from pathlib import Path

import pytest
import semver
from pydantic import ValidationError

from cdmtaskservice import models
from cdmtaskservice.pipelines.metaassembly import v0_1_0
from cdmtaskservice.pipelines.metaassembly.v0_1_0 import (
    MetaAssemblyFiles, MetaAssemblyInput, MetaAssemblyParams, ReadMode,
)

_F1 = models.S3File(file="bucket/reads_1.fastq.gz")
_F2 = models.S3File(file="bucket/reads_2.fastq.gz")


def _inp(read_mode, output_prefix, input_file, input_file2=None):
    files_kwargs = {"input_file2": input_file2}
    if input_file is not None:
        files_kwargs["input_file"] = input_file
    return MetaAssemblyInput(
        input=MetaAssemblyParams(read_mode=read_mode, output_prefix=output_prefix),
        files=MetaAssemblyFiles(**files_kwargs),
    )


def test_init():
    p = v0_1_0.init()

    assert p.name == "metaassembly"
    assert p.version == semver.Version.parse("0.1.0")
    assert p.description == (
        "Metagenome assembly of Illumina short reads: error correction via BBTools (bbcms), "
        "assembly via metaSPAdes, and coverage mapping of reads back to the assembly via bbmap."
    )
    assert p.input_model is MetaAssemblyInput
    assert p.nersc_path == Path(
        "/global/cfs/cdirs/kbase/cdm_task_service/pipelines/metaassembly/0.1.0/metaAssembly/"
    )
    assert p.main_wdl == "jgi_assembly.wdl"
    assert set(p.file_md5s) == {
        "jgi_assembly.wdl", "shortReads_assembly.wdl", "make_interleaved_reads.wdl",
    }
    assert p.output_keys == {
        "jgi_metaAssembly.sr_contig",
        "jgi_metaAssembly.sr_scaffold",
        "jgi_metaAssembly.sr_agp",
        "jgi_metaAssembly.sr_bam",
        "jgi_metaAssembly.sr_samgz",
        "jgi_metaAssembly.sr_covstats",
        "jgi_metaAssembly.sr_asminfo",
        "jgi_metaAssembly.sr_bbcms_fq",
        "jgi_metaAssembly.stats",
    }
    assert p.doc_urls == [
        "https://github.com/kbaseincubator/metaAssembly",
        "https://docs.microbiomedata.org/workflows/chapters/4_Metagenome_Assembly/",
    ]


def test_input_interleaved_minimal():
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", _F1)

    assert inp.get_s3_files() == [_F1]


def test_input_interleaved_accepts_string_path():
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", "bucket/reads_1.fastq.gz")

    assert inp.get_s3_files() == [_F1]
    assert inp.files.input_file == _F1


def test_input_paired_accepts_string_paths():
    inp = _inp(
        ReadMode.PAIRED,
        "proj-xyz",
        "bucket/reads_1.fastq.gz",
        "bucket/reads_2.fastq.gz",
    )

    assert inp.get_s3_files() == [_F1, _F2]
    assert inp.files.input_file == _F1
    assert inp.files.input_file2 == _F2


def test_input_paired():
    inp = _inp(ReadMode.PAIRED, "proj-xyz", _F1, _F2)

    assert inp.get_s3_files() == [_F1, _F2]


def test_set_s3_files_interleaved():
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", _F1)
    resolved_f1 = models.S3File(file=_F1.file, crc64nvme="cccccccccccc")

    new_inp = inp.set_s3_files({_F1.file: resolved_f1})

    assert new_inp.files.input_file == resolved_f1
    assert new_inp.files.input_file2 is None
    assert inp.files.input_file == _F1  # original is unchanged


def test_set_s3_files_paired():
    inp = _inp(ReadMode.PAIRED, "proj-xyz", _F1, _F2)
    resolved_f1 = models.S3File(file=_F1.file, crc64nvme="aaaaaaaaaaaa")
    resolved_f2 = models.S3File(file=_F2.file, crc64nvme="bbbbbbbbbbbb")

    new_inp = inp.set_s3_files({_F1.file: resolved_f1, _F2.file: resolved_f2})

    assert new_inp.files.input_file == resolved_f1
    assert new_inp.files.input_file2 == resolved_f2
    assert inp.files.input_file == _F1  # original is unchanged
    assert inp.files.input_file2 == _F2


def test_input_fail_no_input_file():
    with pytest.raises(ValidationError) as e:
        _inp(ReadMode.INTERLEAVED, "proj-xyz", None)

    assert e.value.errors(include_url=False) == [{
        "type": "missing",
        "loc": ("input_file",),
        "msg": "Field required",
        "input": {"input_file2": None},
    }]


def test_input_fail_bad_path():
    with pytest.raises(ValidationError) as e:
        _inp(ReadMode.INTERLEAVED, "proj-xyz", "bad bucket!/reads_1.fastq.gz")

    err = e.value.errors(include_url=False)[0]
    ex = err["ctx"]["error"]
    assert isinstance(ex, ValueError)
    assert str(ex) == (
        "Bucket name may only contain '-' and lowercase ascii alphanumerics: bad bucket!"
    )
    assert err == {
        "type": "value_error",
        "loc": ("input_file", "file"),
        "msg": "Value error, Bucket name may only contain '-' and lowercase ascii "
            "alphanumerics: bad bucket!",
        "input": "bad bucket!/reads_1.fastq.gz",
        "ctx": {"error": ex},
    }


def test_input_fail_duplicate_files():
    with pytest.raises(ValidationError) as e:
        _inp(ReadMode.PAIRED, "proj-xyz", _F1, _F1)

    err = e.value.errors(include_url=False)[0]
    ex = err["ctx"]["error"]
    assert isinstance(ex, ValueError)
    assert str(ex) == "Duplicate files in input: ['bucket/reads_1.fastq.gz']"
    assert err == {
        "type": "value_error",
        "loc": (),
        "msg": "Value error, Duplicate files in input: ['bucket/reads_1.fastq.gz']",
        "input": {"input_file": _F1, "input_file2": _F1},
        "ctx": {"error": ex},
    }


def test_input_fail_paired_mode_without_paired_files():
    inp_files = MetaAssemblyFiles(input_file=_F1)
    with pytest.raises(ValidationError) as e:
        _inp(ReadMode.PAIRED, "proj-xyz", _F1)

    err = e.value.errors(include_url=False)[0]
    ex = err["ctx"]["error"]
    assert isinstance(ex, ValueError)
    assert str(ex) == "read_mode paired requires input_file2"
    assert err == {
        "type": "value_error",
        "loc": (),
        "msg": "Value error, read_mode paired requires input_file2",
        "input": {
            "input": MetaAssemblyParams(read_mode=ReadMode.PAIRED, output_prefix="proj-xyz"),
            "files": inp_files,
        },
        "ctx": {"error": ex},
    }


def test_input_fail_paired_files_wrong_mode():
    inp_files = MetaAssemblyFiles(input_file=_F1, input_file2=_F2)
    with pytest.raises(ValidationError) as e:
        _inp(ReadMode.INTERLEAVED, "proj-xyz", _F1, _F2)

    err = e.value.errors(include_url=False)[0]
    ex = err["ctx"]["error"]
    assert isinstance(ex, ValueError)
    assert str(ex) == "input_file2 is only valid for read_mode paired"
    assert err == {
        "type": "value_error",
        "loc": (),
        "msg": "Value error, input_file2 is only valid for read_mode paired",
        "input": {
            "input": MetaAssemblyParams(read_mode=ReadMode.INTERLEAVED, output_prefix="proj-xyz"),
            "files": inp_files,
        },
        "ctx": {"error": ex},
    }


def test_input_fail_output_prefix_bad_chars():
    with pytest.raises(ValidationError) as e:
        _inp(ReadMode.INTERLEAVED, "proj:xyz", _F1)

    assert e.value.errors(include_url=False) == [{
        "type": "string_pattern_mismatch",
        "loc": ("output_prefix",),
        "msg": r"String should match pattern '^[\w][\w.-]*$'",
        "input": "proj:xyz",
        "ctx": {"pattern": r"^[\w][\w.-]*$"},
    }]


def test_build_interleaved():
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", _F1)
    locs = {_F1.file: Path("/scratch/cache/aa/reads_1.fastq.gz")}

    run = inp.get_input_json(locs)

    assert run == {
        "jgi_metaAssembly.proj": "proj-xyz",
        "jgi_metaAssembly.shortRead": True,
        "jgi_metaAssembly.threads": "16",
        "jgi_metaAssembly.input_files": ["/scratch/cache/aa/reads_1.fastq.gz"],
    }


def test_build_paired():
    inp = _inp(ReadMode.PAIRED, "proj-xyz", _F1, _F2)
    locs = {
        _F1.file: Path("/scratch/cache/aa/reads_1.fastq.gz"),
        _F2.file: Path("/scratch/cache/bb/reads_2.fastq.gz"),
    }

    run = inp.get_input_json(locs)

    assert run == {
        "jgi_metaAssembly.proj": "proj-xyz",
        "jgi_metaAssembly.shortRead": True,
        "jgi_metaAssembly.threads": "16",
        "jgi_metaAssembly.input_files": [
            "/scratch/cache/aa/reads_1.fastq.gz", "/scratch/cache/bb/reads_2.fastq.gz",
        ],
    }


def test_build_fail_missing_file_location():
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", _F1)

    with pytest.raises(
        ValueError, match="No file location provided for S3 file 'bucket/reads_1.fastq.gz'"
    ):
        inp.get_input_json({})


def test_init_validate_input_then_get_input_json():
    p = v0_1_0.init()
    inp = _inp(ReadMode.INTERLEAVED, "proj-xyz", _F1)
    locs = {_F1.file: Path("/scratch/cache/aa/reads_1.fastq.gz")}

    run = p.validate_input(inp).get_input_json(locs)

    assert run == {
        "jgi_metaAssembly.proj": "proj-xyz",
        "jgi_metaAssembly.shortRead": True,
        "jgi_metaAssembly.threads": "16",
        "jgi_metaAssembly.input_files": ["/scratch/cache/aa/reads_1.fastq.gz"],
    }
