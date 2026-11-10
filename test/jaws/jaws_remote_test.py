import io
import json
import pytest

from conftest import assert_exception_correct
from cdmtaskservice.jaws.remote import OutputFileGroup, parse_outputs_json


# TODO TEST add more tests


def _outputs_json(output_files=(), stdouts=(), stderrs=()):
    return io.BytesIO(json.dumps({
        "wf.output_files": [list(g) for g in output_files],
        "wf.stdouts": list(stdouts),
        "wf.stderrs": list(stderrs),
    }).encode())


def test_parse_outputs_json_single_container_single_file():
    got = parse_outputs_json(_outputs_json(
        output_files=[["cromwell-exec/shard-0/execution/__output__/result.txt"]],
        stdouts=["cromwell-exec/shard-0/execution/stdout"],
        stderrs=["cromwell-exec/shard-0/execution/stderr"],
    ))

    assert got.output_file_groups == [
        OutputFileGroup(
            output_dir="cromwell-exec/shard-0/execution/__output__",
            files={"result.txt": "cromwell-exec/shard-0/execution/__output__/result.txt"},
        ),
    ]
    assert got.stdout == ["cromwell-exec/shard-0/execution/stdout"]
    assert got.stderr == ["cromwell-exec/shard-0/execution/stderr"]


def test_parse_outputs_json_single_container_multiple_subdirs():
    # The derived output_dir is the shared "__output__" directory itself, even when the
    # container's own files are nested under different subdirectories of it.
    got = parse_outputs_json(_outputs_json(output_files=[[
        "cromwell-exec/shard-0/execution/__output__/sub1/a.txt",
        "cromwell-exec/shard-0/execution/__output__/sub2/b.txt",
    ]]))

    assert got.output_file_groups == [
        OutputFileGroup(
            output_dir="cromwell-exec/shard-0/execution/__output__",
            files={
                "sub1/a.txt": "cromwell-exec/shard-0/execution/__output__/sub1/a.txt",
                "sub2/b.txt": "cromwell-exec/shard-0/execution/__output__/sub2/b.txt",
            },
        ),
    ]


def test_parse_outputs_json_container_output_with_nested_dir_named_output_dir():
    # A container could legitimately (or maliciously) create its own subdirectory that happens
    # to also be named "__output__". The real output_dir must still be derived from the first
    # "/__output__/" occurrence - the actual `find ./__output__` search root - not the last,
    # or this file would compute a different (and wrong, narrower) output_dir than its sibling,
    # causing a false "different output directories" rejection for a single container's files.
    got = parse_outputs_json(_outputs_json(output_files=[[
        "cromwell-exec/shard-0/execution/__output__/a.txt",
        "cromwell-exec/shard-0/execution/__output__/nested/__output__/b.txt",
    ]]))

    assert got.output_file_groups == [
        OutputFileGroup(
            output_dir="cromwell-exec/shard-0/execution/__output__",
            files={
                "a.txt": "cromwell-exec/shard-0/execution/__output__/a.txt",
                "nested/__output__/b.txt":
                    "cromwell-exec/shard-0/execution/__output__/nested/__output__/b.txt",
            },
        ),
    ]


def test_parse_outputs_json_multiple_containers_are_separate_groups():
    got = parse_outputs_json(_outputs_json(output_files=[
        ["cromwell-exec/shard-0/execution/__output__/a.txt"],
        ["cromwell-exec/shard-1/execution/__output__/b.txt"],
    ]))

    assert got.output_file_groups == [
        OutputFileGroup(
            output_dir="cromwell-exec/shard-0/execution/__output__",
            files={"a.txt": "cromwell-exec/shard-0/execution/__output__/a.txt"},
        ),
        OutputFileGroup(
            output_dir="cromwell-exec/shard-1/execution/__output__",
            files={"b.txt": "cromwell-exec/shard-1/execution/__output__/b.txt"},
        ),
    ]


def test_parse_outputs_json_container_with_no_output_files():
    got = parse_outputs_json(_outputs_json(output_files=[[]]))

    assert got.output_file_groups == [OutputFileGroup(output_dir=None, files={})]


def test_parse_outputs_json_fail_file_not_a_string():
    with pytest.raises(Exception) as got:
        parse_outputs_json(_outputs_json(output_files=[[None]]))
    assert_exception_correct(got.value, ValueError("file is required"))


def test_parse_outputs_json_fail_file_not_under_output_dir():
    with pytest.raises(Exception) as got:
        parse_outputs_json(_outputs_json(output_files=[["cromwell-exec/shard-0/result.txt"]]))
    assert_exception_correct(got.value, ValueError(
        "Expected JAWS output file path to be under a '__output__' directory: "
        + "cromwell-exec/shard-0/result.txt"
    ))


def test_parse_outputs_json_fail_container_files_under_different_output_dirs():
    # All of a single container's own files should share the exact same "__output__" directory;
    # if they don't, that's a bug in JAWS rather than something to silently paper over.
    with pytest.raises(Exception) as got:
        parse_outputs_json(_outputs_json(output_files=[[
            "cromwell-exec/shard-0/execution/__output__/a.txt",
            "cromwell-exec/shard-1/execution/__output__/b.txt",
        ]]))
    assert_exception_correct(got.value, ValueError(
        "JAWS reported output files for a single container under more than one output "
        + "directory, which should never happen: ['cromwell-exec/shard-0/execution/__output__', "
        + "'cromwell-exec/shard-1/execution/__output__']"
    ))


def test_parse_outputs_json_fail_unexpected_key():
    with pytest.raises(Exception) as got:
        parse_outputs_json(io.BytesIO(json.dumps({"wf.bogus_key": []}).encode()))
    assert_exception_correct(got.value, ValueError("unexpected JAWS outputs.json key: wf.bogus_key"))
