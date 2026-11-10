import json
from pathlib import Path
import pytest

from conftest import assert_exception_correct
from cdmtaskservice.jaws.remote import OUTPUTS_JSON_FILE
from cdmtaskservice.nersc.remote import calculate_checksums, calculate_pipeline_checksums


# TODO TEST add more tests


@pytest.fixture
def tmp_path(tmp_path):
    # Resolved up front so paths built from it are already canonical, matching what the
    # production code's own resolve(strict=True) calls produce - without this, a test
    # environment where tmp_path itself traverses a symlink (e.g. macOS's /tmp) would require
    # resolving every individual path derived from it before comparing against expected values.
    return tmp_path.resolve()


def _write_outputs_json(
    jawsdir: Path, output_files=(), output_file_groups=None, stdouts=(), stderrs=()
):
    if output_file_groups is None:
        output_file_groups = [list(output_files)] if output_files else []
    (jawsdir / OUTPUTS_JSON_FILE).write_text(json.dumps({
        "output_files": output_file_groups,
        "stdouts": list(stdouts),
        "stderrs": list(stderrs),
    }))


def test_calculate_checksums_symlink_inside_same_container_output_dir_is_allowed(tmp_path):
    # target.txt is the kind of entry `find -type f` would really produce; link.txt is a
    # symlink, which `find -type f` would never declare - it's here to prove that if a symlink
    # entry shows up anyway (defense in depth), one resolving inside the same container's own
    # output dir is still allowed alongside the real file.
    jawsdir = tmp_path / "jawsout"
    shard0_dir = "cromwell-exec/shard-0/execution/__output__"
    shard_output = jawsdir / shard0_dir
    shard_output.mkdir(parents=True)
    target = shard_output / "target.txt"
    target.write_bytes(b"hello world")
    link = shard_output / "link.txt"
    link.symlink_to(target)
    target_result_path = f"{shard0_dir}/target.txt"
    link_result_path = f"{shard0_dir}/link.txt"
    _write_outputs_json(jawsdir, output_files=[target_result_path, link_result_path])
    checksum_file = tmp_path / "checksums.json"

    calculate_checksums(str(jawsdir), str(checksum_file))

    assert json.loads(checksum_file.read_text()) == {
        "files": [
            {
                "crc64nvme": "jSnVw/bqjr4=",
                "s3path": "target.txt",
                "respath": "cromwell-exec/shard-0/execution/__output__/target.txt",
                "size": 11,
            },
            {
                "crc64nvme": "jSnVw/bqjr4=",
                "s3path": "link.txt",
                "respath": "cromwell-exec/shard-0/execution/__output__/link.txt",
                "size": 11,
            },
        ],
        "stdouts": [],
        "stderrs": [],
    }


def test_calculate_checksums_symlink_across_subdirs_in_same_container_output_dir_is_allowed(
    tmp_path
):
    # A container's own output directory boundary is the "__output__" directory itself, not
    # just the subdirectory a single file happens to live in - a symlink crossing subdirectories
    # within that same container's output area must still be allowed, regardless of whether its
    # target is itself separately declared as one of the container's own output files.
    jawsdir = tmp_path / "jawsout"
    shard0_dir = "cromwell-exec/shard-0/execution/__output__"
    output_root = jawsdir / shard0_dir
    sub1 = output_root / "sub1"
    sub1.mkdir(parents=True)
    sub2 = output_root / "sub2"
    sub2.mkdir(parents=True)
    b_file = sub2 / "b.txt"
    b_file.write_bytes(b"hello world")
    link = sub1 / "link.txt"
    link.symlink_to(b_file)
    result_path_b = f"{shard0_dir}/sub2/b.txt"
    result_path_link = f"{shard0_dir}/sub1/link.txt"
    _write_outputs_json(jawsdir, output_files=[result_path_b, result_path_link])
    checksum_file = tmp_path / "checksums.json"

    calculate_checksums(str(jawsdir), str(checksum_file))

    assert json.loads(checksum_file.read_text()) == {
        "files": [
            {
                "crc64nvme": "jSnVw/bqjr4=",
                "s3path": "sub2/b.txt",
                "respath": "cromwell-exec/shard-0/execution/__output__/sub2/b.txt",
                "size": 11,
            },
            {
                "crc64nvme": "jSnVw/bqjr4=",
                "s3path": "sub1/link.txt",
                "respath": "cromwell-exec/shard-0/execution/__output__/sub1/link.txt",
                "size": 11,
            },
        ],
        "stdouts": [],
        "stderrs": [],
    }


def test_calculate_checksums_symlink_outside_output_dir_in_output_files_is_rejected(tmp_path):
    jawsdir = tmp_path / "jawsout"
    shard0_dir = "cromwell-exec/shard-0/execution/__output__"
    output_dir = jawsdir / shard0_dir
    output_dir.mkdir(parents=True)
    secret = tmp_path / "secret.txt"
    secret.write_bytes(b"TOP SECRET")
    escape_link = output_dir / "escape.txt"
    escape_link.symlink_to(secret)
    result_path = f"{shard0_dir}/escape.txt"
    _write_outputs_json(jawsdir, output_files=[result_path])
    checksum_file = tmp_path / "checksums.json"

    with pytest.raises(Exception) as got:
        calculate_checksums(str(jawsdir), str(checksum_file))
    assert_exception_correct(got.value, ValueError(
        f"Output file '{result_path}' resolves to '{secret}', which is outside the "
        + "job's output directory; this is not allowed"
    ))


def test_calculate_checksums_declared_output_dir_escapes_jawsdir_is_rejected(tmp_path):
    # A container's declared path could contain '..' segments that, while internally
    # consistent and passing the __output__ marker check in _file_group, derive an output_dir
    # that itself resolves outside jawsdir. _resolve_group_boundary is the last line of defense
    # against this, since no file has even been opened yet at this point.
    jawsdir = tmp_path / "a" / "b" / "jawsout"
    jawsdir.mkdir(parents=True)
    escape_dir = tmp_path / "a" / "etc" / "__output__"
    escape_dir.mkdir(parents=True)
    result_path = "../../etc/__output__/evil.txt"
    _write_outputs_json(jawsdir, output_files=[result_path])
    checksum_file = tmp_path / "checksums.json"

    with pytest.raises(Exception) as got:
        calculate_checksums(str(jawsdir), str(checksum_file))
    assert_exception_correct(got.value, ValueError(
        f"Output directory '../../etc/__output__' (resolves to '{escape_dir}') is outside the "
        + "job's output directory, which is not allowed"
    ))


def test_calculate_checksums_symlink_escaping_to_another_container_output_dir_is_rejected(
    tmp_path
):
    # A single job's containers all share one JAWS output directory, but each container's own
    # __output__ directory is the real boundary - a symlink escaping into a sibling container's
    # __output__ directory must be rejected even though it's still inside the JAWS output dir.
    jawsdir = tmp_path / "jawsout"
    shard0_dir = "cromwell-exec/shard-0/execution/__output__"
    shard1_dir = "cromwell-exec/shard-1/execution/__output__"
    shard0_output = jawsdir / shard0_dir
    shard0_output.mkdir(parents=True)
    shard1_output = jawsdir / shard1_dir
    shard1_output.mkdir(parents=True)
    other_container_file = shard1_output / "other.txt"
    other_container_file.write_bytes(b"from other container")
    escape_link = shard0_output / "escape.txt"
    escape_link.symlink_to(other_container_file)
    result_path = f"{shard0_dir}/escape.txt"
    _write_outputs_json(jawsdir, output_files=[result_path])
    checksum_file = tmp_path / "checksums.json"

    with pytest.raises(Exception) as got:
        calculate_checksums(str(jawsdir), str(checksum_file))
    assert_exception_correct(got.value, ValueError(
        f"Output file '{result_path}' resolves to '{other_container_file}', which "
        + "is outside the job's output directory; this is not allowed"
    ))


def test_calculate_checksums_dest_collision_across_containers_keeps_one(tmp_path):
    # Two containers declaring the same destination name is expected (e.g. non-declobber multi-
    # container jobs); exactly one of them ends up in the final output, not both. Which one is
    # not a guaranteed behavior - this pins down the current result for regression purposes only.
    jawsdir = tmp_path / "jawsout"
    shard0_dir = "cromwell-exec/shard-0/execution/__output__"
    shard1_dir = "cromwell-exec/shard-1/execution/__output__"
    shard0_output = jawsdir / shard0_dir
    shard0_output.mkdir(parents=True)
    shard1_output = jawsdir / shard1_dir
    shard1_output.mkdir(parents=True)
    shard0_file = shard0_output / "result.txt"
    shard0_file.write_bytes(b"shard0 content, should be discarded")
    shard1_file = shard1_output / "result.txt"
    shard1_file.write_bytes(b"hello world")
    result_path_0 = f"{shard0_dir}/result.txt"
    result_path_1 = f"{shard1_dir}/result.txt"
    _write_outputs_json(jawsdir, output_file_groups=[[result_path_0], [result_path_1]])
    checksum_file = tmp_path / "checksums.json"

    calculate_checksums(str(jawsdir), str(checksum_file))

    assert json.loads(checksum_file.read_text()) == {
        "files": [{
            "crc64nvme": "jSnVw/bqjr4=",
            "s3path": "result.txt",
            "respath": "cromwell-exec/shard-1/execution/__output__/result.txt",
            "size": 11,
        }],
        "stdouts": [],
        "stderrs": [],
    }


def test_calculate_checksums_two_containers_with_distinct_files_both_succeed(tmp_path):
    # Each container's files must be checked against its OWN boundary, not another container's -
    # shard1's symlink resolves inside shard1's own output dir but outside shard0's, so a bug
    # that mixed up per-container boundaries would wrongly reject it.
    jawsdir = tmp_path / "jawsout"
    shard0_dir = "cromwell-exec/shard-0/execution/__output__"
    shard1_dir = "cromwell-exec/shard-1/execution/__output__"
    shard0_output = jawsdir / shard0_dir
    shard0_output.mkdir(parents=True)
    shard1_output = jawsdir / shard1_dir
    shard1_output.mkdir(parents=True)
    a_file = shard0_output / "a.txt"
    a_file.write_bytes(b"hello world")
    target_file = shard1_output / "target.txt"
    target_file.write_bytes(b"hello world")
    b_link = shard1_output / "b.txt"
    b_link.symlink_to(target_file)
    result_path_a = f"{shard0_dir}/a.txt"
    result_path_b = f"{shard1_dir}/b.txt"
    _write_outputs_json(jawsdir, output_file_groups=[[result_path_a], [result_path_b]])
    checksum_file = tmp_path / "checksums.json"

    calculate_checksums(str(jawsdir), str(checksum_file))

    assert json.loads(checksum_file.read_text()) == {
        "files": [
            {
                "crc64nvme": "jSnVw/bqjr4=",
                "s3path": "a.txt",
                "respath": "cromwell-exec/shard-0/execution/__output__/a.txt",
                "size": 11,
            },
            {
                "crc64nvme": "jSnVw/bqjr4=",
                "s3path": "b.txt",
                "respath": "cromwell-exec/shard-1/execution/__output__/b.txt",
                "size": 11,
            },
        ],
        "stdouts": [],
        "stderrs": [],
    }


def test_calculate_checksums_container_with_no_output_files_is_skipped(tmp_path):
    # A container that produced no output files at all gets an empty group (output_dir=None)
    # from the parser - calculate_checksums must skip it rather than try to resolve a boundary
    # for it, while still correctly processing any other container's files.
    jawsdir = tmp_path / "jawsout"
    shard1_dir = "cromwell-exec/shard-1/execution/__output__"
    shard1_output = jawsdir / shard1_dir
    shard1_output.mkdir(parents=True)
    a_file = shard1_output / "a.txt"
    a_file.write_bytes(b"hello world")
    result_path_a = f"{shard1_dir}/a.txt"
    _write_outputs_json(jawsdir, output_file_groups=[[], [result_path_a]])
    checksum_file = tmp_path / "checksums.json"

    calculate_checksums(str(jawsdir), str(checksum_file))

    assert json.loads(checksum_file.read_text()) == {
        "files": [{
            "crc64nvme": "jSnVw/bqjr4=",
            "s3path": "a.txt",
            "respath": "cromwell-exec/shard-1/execution/__output__/a.txt",
            "size": 11,
        }],
        "stdouts": [],
        "stderrs": [],
    }


def test_calculate_checksums_stdout_stderr_plain_files_succeed(tmp_path):
    jawsdir = tmp_path / "jawsout"
    jawsdir.mkdir()
    (jawsdir / "stdout.txt").write_bytes(b"hello world")
    (jawsdir / "stderr.txt").write_bytes(b"hello world")
    _write_outputs_json(jawsdir, stdouts=["stdout.txt"], stderrs=["stderr.txt"])
    checksum_file = tmp_path / "checksums.json"

    calculate_checksums(str(jawsdir), str(checksum_file))

    assert json.loads(checksum_file.read_text()) == {
        "files": [],
        "stdouts": [{"crc64nvme": "jSnVw/bqjr4=", "respath": "stdout.txt"}],
        "stderrs": [{"crc64nvme": "jSnVw/bqjr4=", "respath": "stderr.txt"}],
    }


def test_calculate_checksums_stdout_symlink_inside_jawsdir_is_rejected(tmp_path):
    # stdout/stderr are written directly by JAWS, not the job's container, so there's no
    # legitimate reason for either to ever be a symlink - even one that resolves within jawsdir,
    # unlike job output files which the container controls.
    jawsdir = tmp_path / "jawsout"
    jawsdir.mkdir()
    out_target = jawsdir / "real_stdout.txt"
    out_target.write_bytes(b"hello world")
    stdout_link = jawsdir / "stdout.txt"
    stdout_link.symlink_to(out_target)
    _write_outputs_json(jawsdir, stdouts=["stdout.txt"])
    checksum_file = tmp_path / "checksums.json"

    with pytest.raises(Exception) as got:
        calculate_checksums(str(jawsdir), str(checksum_file))
    assert_exception_correct(got.value, ValueError(
        f"Container 0 stdout 'stdout.txt' resolves to '{out_target}', which is not allowed"
    ))


def test_calculate_checksums_symlink_outside_output_dir_in_stdout_is_rejected(tmp_path):
    jawsdir = tmp_path / "jawsout"
    jawsdir.mkdir()
    secret = tmp_path / "secret.txt"
    secret.write_bytes(b"TOP SECRET")
    escape_link = jawsdir / "stdout.txt"
    escape_link.symlink_to(secret)
    _write_outputs_json(jawsdir, stdouts=["stdout.txt"])
    checksum_file = tmp_path / "checksums.json"

    with pytest.raises(Exception) as got:
        calculate_checksums(str(jawsdir), str(checksum_file))
    assert_exception_correct(got.value, ValueError(
        f"Container 0 stdout 'stdout.txt' resolves to '{secret}', which is not allowed"
    ))


def test_calculate_checksums_stdout_absolute_path_outside_jawsdir_is_rejected(tmp_path):
    # An absolute stdout path discards jawsdir entirely when joined (Path./ semantics), so a
    # plain (non-symlink) absolute path outside jawsdir would pass a resolve-to-itself check
    # alone - the explicit containment check is what actually catches this.
    jawsdir = tmp_path / "jawsout"
    jawsdir.mkdir()
    secret = tmp_path / "secret.txt"
    secret.write_bytes(b"TOP SECRET")
    _write_outputs_json(jawsdir, stdouts=[str(secret)])
    checksum_file = tmp_path / "checksums.json"

    with pytest.raises(Exception) as got:
        calculate_checksums(str(jawsdir), str(checksum_file))
    assert_exception_correct(got.value, ValueError(
        f"Container 0 stdout '{secret}' is outside the job's output directory, which is not "
        + "allowed"
    ))


def test_calculate_checksums_fail_jaws_output_dir_is_a_symlink(tmp_path):
    # jaws_output_dir is assigned by JAWS/the service, not the container - it should never be a
    # symlink, and if it somehow is, that's treated as an error rather than silently resolved
    # through.
    real_dir = tmp_path / "real_jawsout"
    real_dir.mkdir()
    jawsdir = tmp_path / "jawsout"
    jawsdir.symlink_to(real_dir)
    checksum_file = tmp_path / "checksums.json"

    with pytest.raises(Exception) as got:
        calculate_checksums(str(jawsdir), str(checksum_file))
    assert_exception_correct(got.value, ValueError(
        f"JAWS output directory '{jawsdir}' is not a canonical path; it resolves to "
        + f"'{real_dir}'"
    ))


def test_calculate_checksums_fail_jaws_output_dir_missing(tmp_path):
    checksum_file = tmp_path / "checksums.json"

    with pytest.raises(Exception) as got:
        calculate_checksums("  \t  ", str(checksum_file))
    assert_exception_correct(got.value, ValueError("jaws_output_dir is required"))


def test_calculate_pipeline_checksums_allows_absolute_path_inside_output_dir(tmp_path):
    # Pipeline (Cromwell) output paths are absolute, but must still resolve inside jawsdir.
    jawsdir = tmp_path / "jawsout"
    jawsdir.mkdir()
    result_file = jawsdir / "nested" / "result.txt"
    result_file.parent.mkdir(parents=True)
    result_file.write_bytes(b"hello world")
    (jawsdir / OUTPUTS_JSON_FILE).write_text(json.dumps({
        "my_pipeline.result": str(result_file),
    }))
    output_keys_file = tmp_path / "output_keys.json"
    output_keys_file.write_text(json.dumps(["my_pipeline.result"]))
    checksum_file = tmp_path / "checksums.json"

    calculate_pipeline_checksums(str(jawsdir), str(output_keys_file), str(checksum_file))

    assert json.loads(checksum_file.read_text()) == {
        "files": [{
            "crc64nvme": "jSnVw/bqjr4=",
            "s3path": "result.txt",
            "respath": str(tmp_path / "jawsout" / "nested" / "result.txt"),
            "size": 11,
        }],
        "stdouts": [],
        "stderrs": [],
    }


def test_calculate_pipeline_checksums_allows_symlink_inside_output_dir(tmp_path):
    jawsdir = tmp_path / "jawsout"
    jawsdir.mkdir()
    target = jawsdir / "nested" / "target.txt"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"hello world")
    link = jawsdir / "link.txt"
    link.symlink_to(target)
    (jawsdir / OUTPUTS_JSON_FILE).write_text(json.dumps({
        "my_pipeline.result": str(link),
    }))
    output_keys_file = tmp_path / "output_keys.json"
    output_keys_file.write_text(json.dumps(["my_pipeline.result"]))
    checksum_file = tmp_path / "checksums.json"

    calculate_pipeline_checksums(str(jawsdir), str(output_keys_file), str(checksum_file))

    assert json.loads(checksum_file.read_text()) == {
        "files": [{
            "crc64nvme": "jSnVw/bqjr4=",
            "s3path": "link.txt",
            "respath": str(tmp_path / "jawsout" / "link.txt"),
            "size": 11,
        }],
        "stdouts": [],
        "stderrs": [],
    }


def test_calculate_pipeline_checksums_rejects_symlink_pointing_outside_output_dir(tmp_path):
    jawsdir = tmp_path / "jawsout"
    jawsdir.mkdir()
    secret = tmp_path / "secret.txt"
    secret.write_bytes(b"TOP SECRET")
    escape_link = jawsdir / "escape_link.txt"
    escape_link.symlink_to(secret)
    (jawsdir / OUTPUTS_JSON_FILE).write_text(json.dumps({
        "my_pipeline.result": str(escape_link),
    }))
    output_keys_file = tmp_path / "output_keys.json"
    output_keys_file.write_text(json.dumps(["my_pipeline.result"]))
    checksum_file = tmp_path / "checksums.json"

    with pytest.raises(Exception) as got:
        calculate_pipeline_checksums(str(jawsdir), str(output_keys_file), str(checksum_file))
    assert_exception_correct(got.value, ValueError(
        f"Output file '{escape_link}' resolves to '{secret}', which is outside the job's "
        + "output directory; this is not allowed"
    ))


def test_calculate_pipeline_checksums_rejects_absolute_path_outside_output_dir(tmp_path):
    jawsdir = tmp_path / "jawsout"
    jawsdir.mkdir()
    elsewhere = tmp_path / "cromwell-executions" / "result.txt"
    elsewhere.parent.mkdir(parents=True)
    elsewhere.write_bytes(b"hello world")
    (jawsdir / OUTPUTS_JSON_FILE).write_text(json.dumps({
        "my_pipeline.result": str(elsewhere),
    }))
    output_keys_file = tmp_path / "output_keys.json"
    output_keys_file.write_text(json.dumps(["my_pipeline.result"]))
    checksum_file = tmp_path / "checksums.json"

    with pytest.raises(Exception) as got:
        calculate_pipeline_checksums(str(jawsdir), str(output_keys_file), str(checksum_file))
    assert_exception_correct(got.value, ValueError(
        f"Output file '{elsewhere}' is outside the job's output directory; this is not allowed"
    ))


def test_calculate_pipeline_checksums_fail_jaws_output_dir_is_a_symlink(tmp_path):
    real_dir = tmp_path / "real_jawsout"
    real_dir.mkdir()
    jawsdir = tmp_path / "jawsout"
    jawsdir.symlink_to(real_dir)
    output_keys_file = tmp_path / "output_keys.json"
    output_keys_file.write_text(json.dumps([]))
    checksum_file = tmp_path / "checksums.json"

    with pytest.raises(Exception) as got:
        calculate_pipeline_checksums(str(jawsdir), str(output_keys_file), str(checksum_file))
    assert_exception_correct(got.value, ValueError(
        f"JAWS output directory '{jawsdir}' is not a canonical path; it resolves to "
        + f"'{real_dir}'"
    ))


def test_calculate_pipeline_checksums_fail_jaws_output_dir_missing(tmp_path):
    output_keys_file = tmp_path / "output_keys.json"
    output_keys_file.write_text(json.dumps([]))
    checksum_file = tmp_path / "checksums.json"

    with pytest.raises(Exception) as got:
        calculate_pipeline_checksums("  \t  ", str(output_keys_file), str(checksum_file))
    assert_exception_correct(got.value, ValueError("jaws_output_dir is required"))
