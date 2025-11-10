import datetime
import inspect
from pathlib import Path
import pytest
import re

from cdmtaskservice.jaws import wdl
from cdmtaskservice import models
from cdmtaskservice import sites


# Note that a lot of stuff in the wdl code is shell quoted, but the Job model
# really locks down string inputs, so it's impossible to pass anything that needs quoting
# other than the image entrypoint and input files


BADCHARS = [
    "*", ";", ":", "&", "|", "\n", ">", "<", "`", "(", ")", "$", "'", '"', "#", "{", "}",
    "[", "]", "?", "~", "!"
]

_T1 = datetime.datetime(2025, 3, 31, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)

_JOB_BASIC = models.Job(
    id="foo",
    job_input=models.JobInput(
        cluster=sites.Cluster.PERLMUTTER_JAWS,
        image="some_image",
        params=models.Parameters(),
        input_files=[
            models.S3FileWithDataID(file="bucket/file1"),
        ],
        output_dir="bucket/output",
    ),
    user="user",
    image=models.JobImage(
        name="some_image",
        digest="digest",
        entrypoint=["main_command"],
        registered_by="someuser",
        registered_on=_T1,
    ),
    input_file_count=1,
    state=models.JobState.CREATED,
    transition_times=[
        models.JobStateTransition(
            state=models.JobState.CREATED,
            time=_T1,
        ),
    ]
)


def load_test_data(file: str):
    loc = Path(__file__).parent / "wdl_test_data" / file
    return loc.read_text()


def load_data_for_test(ext: str = "wdl") -> str:
    test_func = next(
        (frame.function for frame in inspect.stack() if frame.function.startswith("test_")),
        None
    )
    if not test_func:
        raise RuntimeError("Could not determine test name from call stack.")

    test_data_file = Path(__file__).parent / "wdl_test_data" / f"{test_func}.{ext}"

    if not test_data_file.exists():
        raise FileNotFoundError(f"Test data file not found: {test_data_file}")
    return test_data_file.read_text()


def test_wdl_basic():
    mapping = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    jawsinput = wdl.generate_wdl(_JOB_BASIC, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar"]],
        "some_image.file_locs_list": [["file1"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [["main_command"]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_multiple_containers():
    mapping = {
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar2"),
        models.S3FileWithDataID(file="bucket/file3"): Path("foo/bar3"),
    }
    ji = _JOB_BASIC.job_input.model_copy(update={
        "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
            models.S3FileWithDataID(file="bucket/file3"),
        ],
        "num_containers": 2,
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar2", "foo/bar"], ["foo/bar3"]],
        "some_image.file_locs_list": [["file1", "file2"], ["file3"]],
        "some_image.environment_list": [[], []],
        "some_image.cmdline_list": [["main_command"], ["main_command"]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_workflow_name_translation():
    mapping = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    im = _JOB_BASIC.image.model_copy(update={"name": "docker.io/foo/bar:stuff.thing-yay"})
    job = _JOB_BASIC.model_copy(update={"image": im})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_data_for_test()
    assert jawsinput.input_json == {
        "docker_io_foo_bar_stuff_thing_yay.input_files_list": [["foo/bar"]],
        "docker_io_foo_bar_stuff_thing_yay.file_locs_list": [["file1"]],
        "docker_io_foo_bar_stuff_thing_yay.environment_list": [[]],
        "docker_io_foo_bar_stuff_thing_yay.cmdline_list": [["main_command"]]
    }
    assert jawsinput.wdl == expwdl
    

def test_wdl_full_input_path():
    mapping = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    ji = _JOB_BASIC.job_input.model_copy(update={"input_roots": [""]})
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar"]],
        "some_image.file_locs_list": [["bucket/file1"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [["main_command"]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_partial_input_path():
    mapping = {models.S3FileWithDataID(file="bucket/dir1/dir2/file1"): Path("foo/bar")}
    ji = _JOB_BASIC.job_input.model_copy(update={
        "input_roots": ["bucket/dir1"],
        "input_files": [models.S3FileWithDataID(file="bucket/dir1/dir2/file1")],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar"]],
        "some_image.file_locs_list": [["dir2/file1"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [["main_command"]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_refdata():
    # can also specify via the job input, but that's abstracted by the Job class and
    # should be tested there
    mapping = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    im = _JOB_BASIC.image.model_copy(update={
        "default_refdata_mount_point": "/reffyref", "refdata_id": "refid"
    })
    job = _JOB_BASIC.model_copy(update={"image": im})
    jawsinput = wdl.generate_wdl(job, mapping, relative_refdata_path="some/ref/path")
    expwdl = load_data_for_test()
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar"]],
        "some_image.file_locs_list": [["file1"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [["main_command"]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_resources_and_io_mounts():
    mapping = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    p = models.Parameters(input_mount_point="/woot/thing", output_mount_point="/outyout")
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p,
        "cpus": 256,
        "runtime": datetime.timedelta(hours=11, minutes=59, seconds=1),  # test rounding up
        "memory": 1_000_000_000_006,
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_data_for_test()
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar"]],
        "some_image.file_locs_list": [["file1"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [["main_command"]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_entrypoint_quoted():
    # also tests a longer entrypoint
    mapping = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    im = _JOB_BASIC.image.model_copy(update={"entrypoint": ["prog;ram", "sub*com'mand"]})
    job = _JOB_BASIC.model_copy(update={"image": im})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar"]],
        "some_image.file_locs_list": [["file1"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [["'prog;ram'", '\'sub*com\'"\'"\'mand\'',]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_manifest_files():
    mapping = {
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar2"),
    }
    p = models.Parameters(
        args=[models.ParameterWithFlag(
            type=models.ParameterType.MANIFEST_FILE,
            manifest_file_format=models.ManifestFileFormat.FILES,
        ),
    ])
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p,
        "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
        "num_containers": 2,
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    # test absolute and relative paths and allowed special chars
    jawsinput = wdl.generate_wdl(
        job, mapping, manifest_file_list=[Path("m.-_1"), Path("/a.-_bs/m2")]
    )
    expwdl = load_test_data("basic_with_manifest.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar2"], ["foo/bar"]],
        "some_image.file_locs_list": [["file1"], ["file2"]],
        "some_image.environment_list": [[], []],
        "some_image.cmdline_list": [
            ["main_command", "/input_files/m.-_1"], ["main_command", "/input_files/m2"]
        ],
        "some_image.manifest_list": ["m.-_1", "/a.-_bs/m2"],
    }
    assert jawsinput.wdl == expwdl


def test_wdl_manifest_file_with_std_flag():
    mapping = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    p = models.Parameters(
        args=[models.ParameterWithFlag(
            type=models.ParameterType.MANIFEST_FILE,
            manifest_file_format=models.ManifestFileFormat.FILES,
            flag="-m",
        ),
    ])
    ji = _JOB_BASIC.job_input.model_copy(update={"params": p})
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping, manifest_file_list=[Path("relative/manipedi")])
    expwdl = load_test_data("basic_with_manifest.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar"]],
        "some_image.file_locs_list": [["file1"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [["main_command", "-m", "/input_files/manipedi"]],
        "some_image.manifest_list": ["relative/manipedi"],
    }
    assert jawsinput.wdl == expwdl


def test_wdl_manifest_file_with_equal_flag():
    mapping = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    p = models.Parameters(
        args=[models.ParameterWithFlag(
            type=models.ParameterType.MANIFEST_FILE,
            manifest_file_format=models.ManifestFileFormat.FILES,
            flag="-m=",
        ),
    ])
    ji = _JOB_BASIC.job_input.model_copy(update={"params": p})
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping, manifest_file_list=[Path("manipedi")])
    expwdl = load_test_data("basic_with_manifest.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar"]],
        "some_image.file_locs_list": [["file1"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [["main_command", "-m=/input_files/manipedi"]],
        "some_image.manifest_list": ["manipedi"],
    }
    assert jawsinput.wdl == expwdl


def test_wdl_manifest_file_in_env():
    mapping = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    p = models.Parameters(
        environment={"MANIFEST": models.ParameterWithFlag(
            type=models.ParameterType.MANIFEST_FILE,
            manifest_file_format=models.ManifestFileFormat.FILES,
        ),
    })
    ji = _JOB_BASIC.job_input.model_copy(update={"params": p})
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping, manifest_file_list=[Path("manipedi")])
    expwdl = load_test_data("basic_with_manifest.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar"]],
        "some_image.file_locs_list": [["file1"]],
        "some_image.environment_list": [["MANIFEST=/input_files/manipedi"]],
        "some_image.cmdline_list": [["main_command"]],
        "some_image.manifest_list": ["manipedi"],
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_env_simple():
    mapping = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    p = models.Parameters(args=["im_an_argument"], environment={"ENVKEY": "envval"})
    ji = _JOB_BASIC.job_input.model_copy(update={"params": p})
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar"]],
        "some_image.file_locs_list": [["file1"]],
        "some_image.environment_list": [["ENVKEY=envval"]],
        "some_image.cmdline_list": [["main_command", "im_an_argument"]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_space_sep_list_and_container_number():
    mapping = {
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    cn1 = models.ParameterWithFlag(type=models.ParameterType.CONTAINTER_NUMBER)
    cn2 = models.ParameterWithFlag(
        type=models.ParameterType.CONTAINTER_NUMBER,
        container_num_prefix="pref/",
        container_num_suffix="/suff",
    )
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.SPACE_SEPARATED_LIST,
            ),
            "--container_number", cn1,
            cn2,
        ],
        environment={"ENVKEY": "envval", "CNNUM1": cn1, "CNNUM2": cn2}
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["file1", "file2"]],
        "some_image.environment_list": [["ENVKEY=envval", "CNNUM1=0", "CNNUM2=pref/0/suff"]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument", "/input_files/file1", "/input_files/file2",
            "--container_number", "0", "pref/0/suff"
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_space_sep_list_quoted():
    mapping = {
        models.S3FileWithDataID(file="bucket/fi*le1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/fil]e2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.SPACE_SEPARATED_LIST,
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/fi*le1"),
            models.S3FileWithDataID(file="bucket/fil]e2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["'fi*le1'", "'fil]e2'"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument", "'/input_files/fi*le1'", "'/input_files/fil]e2'",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_space_sep_list_and_container_number_with_std_flag():
    mapping = {
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    cn1 = models.ParameterWithFlag(type=models.ParameterType.CONTAINTER_NUMBER, flag="--cn1")
    cn2 = models.ParameterWithFlag(
        type=models.ParameterType.CONTAINTER_NUMBER,
        container_num_prefix="pref/",
        container_num_suffix="/suff",
        flag="-c",
    )
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.SPACE_SEPARATED_LIST,
                flag="--input",
            ),
            cn1,
            cn2,
        ],
        environment={"ENVKEY": "envval", "CNNUM1": cn1, "CNNUM2": cn2}
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["file1", "file2"]],
        "some_image.environment_list": [["ENVKEY=envval", "CNNUM1=0", "CNNUM2=pref/0/suff"]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument",
            "--input", "/input_files/file1", "/input_files/file2",
            "--cn1", "0", "-c", "pref/0/suff"
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_space_sep_list_with_std_flag_quoted():
    mapping = {
        models.S3FileWithDataID(file="bucket/$file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/fi&le2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.SPACE_SEPARATED_LIST,
                flag="--input",
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/$file1"),
            models.S3FileWithDataID(file="bucket/fi&le2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["'$file1'", "'fi&le2'"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument",
            "--input", "'/input_files/$file1'", "'/input_files/fi&le2'",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_space_sep_list_and_container_number_with_equal_flag():
    mapping = {
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    cn1 = models.ParameterWithFlag(type=models.ParameterType.CONTAINTER_NUMBER, flag="--cn1=")
    cn2 = models.ParameterWithFlag(
        type=models.ParameterType.CONTAINTER_NUMBER,
        container_num_prefix="pref/",
        container_num_suffix="/suff",
        flag="-c=",
    )
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.SPACE_SEPARATED_LIST,
                flag="--input=",
            ),
            cn1,
            cn2,
        ],
        environment={"ENVKEY": "envval", "CNNUM1": cn1, "CNNUM2": cn2}
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["file1", "file2"]],
        "some_image.environment_list": [["ENVKEY=envval", "CNNUM1=0", "CNNUM2=pref/0/suff"]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument",
            "--input=/input_files/file1", "/input_files/file2",
            "--cn1=0", "-c=pref/0/suff"
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_space_sep_list_with_equal_flag_quoted():
    mapping = {
        models.S3FileWithDataID(file="bucket/f!ile1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file(2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.SPACE_SEPARATED_LIST,
                flag="--input=",
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/f!ile1"),
            models.S3FileWithDataID(file="bucket/file(2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["'f!ile1'", "'file(2'"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument",
            "'--input=/input_files/f!ile1'", "'/input_files/file(2'",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_comma_sep_list():
    mapping = {
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.COMMA_SEPARATED_LIST,
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["file1", "file2"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument", "/input_files/file1,/input_files/file2",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_comma_sep_list_quoted():
    mapping = {
        models.S3FileWithDataID(file="bucket/f[ile1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.COMMA_SEPARATED_LIST,
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/f[ile1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["'f[ile1'", "file2"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument", "'/input_files/f[ile1,/input_files/file2'",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_comma_sep_list_with_std_flag():
    mapping = {
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.COMMA_SEPARATED_LIST,
                flag="-i"
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["file1", "file2"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument", "-i", "/input_files/file1,/input_files/file2",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_comma_sep_list_with_std_flag_quoted():
    mapping = {
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/fi<le2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.COMMA_SEPARATED_LIST,
                flag="-i"
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/fi<le2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["file1", "'fi<le2'"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument", "-i", "'/input_files/file1,/input_files/fi<le2'",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_comma_sep_list_with_equals_flag():
    mapping = {
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.COMMA_SEPARATED_LIST,
                flag="-input="
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["file1", "file2"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument", "-input=/input_files/file1,/input_files/file2",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_comma_sep_list_with_equals_flag_quoted():
    mapping = {
        models.S3FileWithDataID(file="bucket/f|ile1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.COMMA_SEPARATED_LIST,
                flag="-input="
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/f|ile1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["'f|ile1'", "file2"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument", "'-input=/input_files/f|ile1,/input_files/file2'",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_repeated_param_with_std_flag():
    mapping = {
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.REPEAT_PARAMETER,
                flag="-i"
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["file1", "file2"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument",
            "-i", "/input_files/file1", "-i", "/input_files/file2",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_repeated_param_with_std_flag_quoted():
    mapping = {
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/fil{e2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.REPEAT_PARAMETER,
                flag="-i"
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/fil{e2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["file1", "'fil{e2'"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument",
            "-i", "/input_files/file1", "-i", "'/input_files/fil{e2'",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_repeated_param_with_equal_flag():
    mapping = {
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.REPEAT_PARAMETER,
                flag="--infiles="
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["file1", "file2"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument",
            "--infiles=/input_files/file1", "--infiles=/input_files/file2",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_args_repeated_param_with_equal_flag_quoted():
    mapping = {
        models.S3FileWithDataID(file="bucket/#file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    p = models.Parameters(args=[
            "im_an_argument",
            models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.REPEAT_PARAMETER,
                flag="--infiles="
            ),
        ],
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/#file1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["'#file1'", "file2"]],
        "some_image.environment_list": [[]],
        "some_image.cmdline_list": [[
            "main_command", "im_an_argument",
            "'--infiles=/input_files/#file1'", "--infiles=/input_files/file2",
        ]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_env_comma_sep_list():
    mapping = {
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    p = models.Parameters(
        args=["im_an_argument"],
        environment={
            "INFILES": models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.COMMA_SEPARATED_LIST,
            ),
        }
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [["file1", "file2"]],
        "some_image.environment_list": [["INFILES=/input_files/file1,/input_files/file2"]],
        "some_image.cmdline_list": [["main_command", "im_an_argument"]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_env_comma_sep_list_quoted():
    mapping = {
        models.S3FileWithDataID(file='bucket/fil"e1'): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    p = models.Parameters(
        args=["im_an_argument"],
        environment={
            "INFILES": models.ParameterWithFlag(
                type=models.ParameterType.INPUT_FILES,
                input_files_format=models.InputFilesFormat.COMMA_SEPARATED_LIST,
            ),
        }
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "input_files": [
            models.S3FileWithDataID(file='bucket/fil"e1'),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar", "foo/bar2"]],
        "some_image.file_locs_list": [['\'fil"e1\'', "file2"]],
        "some_image.environment_list": [['INFILES=\'/input_files/fil"e1,/input_files/file2\'',]],
        "some_image.cmdline_list": [["main_command", "im_an_argument"]]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_container_number_multiple_containers():
    mapping = {
        models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar"),
        models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar2"),
    }
    cn1 = models.ParameterWithFlag(type=models.ParameterType.CONTAINTER_NUMBER)
    cn2 = models.ParameterWithFlag(
        type=models.ParameterType.CONTAINTER_NUMBER,
        container_num_prefix="pref/",
        container_num_suffix="/suff",
    )
    p = models.Parameters(
        args=[cn1, cn2],
        environment={"ENVKEY": "envval", "CNNUM1": cn1, "CNNUM2": cn2}
    )
    ji = _JOB_BASIC.job_input.model_copy(update={
        "params": p, "num_containers": 2, "input_files": [
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
        ],
    })
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    jawsinput = wdl.generate_wdl(job, mapping)
    expwdl = load_test_data("basic.wdl")
    assert jawsinput.input_json == {
        "some_image.input_files_list": [["foo/bar"], ["foo/bar2"]],
        "some_image.file_locs_list": [["file1"], ["file2"]],
        "some_image.environment_list": [
            ["ENVKEY=envval", "CNNUM1=0", "CNNUM2=pref/0/suff"],
            ["ENVKEY=envval", "CNNUM1=1", "CNNUM2=pref/1/suff"]
        ],
        "some_image.cmdline_list": [
            ["main_command", "0", "pref/0/suff"],
            ["main_command", "1", "pref/1/suff"],
        ]
    }
    assert jawsinput.wdl == expwdl


def test_wdl_fail_missing_args():
    fm = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}

    _wdl_fail(None, fm, None, None, ValueError("job is required"))
    _wdl_fail(_JOB_BASIC, None, None, None, ValueError("file_mapping is required"))
    _wdl_fail(_JOB_BASIC, {}, None, None, ValueError("file_mapping is required"))


def test_wdl_fail_no_refdata_mount():
    fm = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    _wdl_fail(_JOB_BASIC, fm, None, Path("foo/bar"), ValueError(
        "If a refdata path is supplied, a mount point for the job must be supplied"
    ))


def test_wdl_fail_not_S3File():
    fm = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    ji = _JOB_BASIC.job_input.model_copy(update={"input_files": ["bucket/file1"]})
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    _wdl_fail(job, fm, None, None, ValueError("input files must be S3 files with a checksum"))


def test_wdl_fail_manifest_files():
    fm = {models.S3FileWithDataID(file="bucket/file1"): Path("foo/bar")}
    p = models.Parameters(args=[models.ParameterWithFlag(
        type=models.ParameterType.MANIFEST_FILE,
        manifest_file_format=models.ManifestFileFormat.FILES,
    )])
    ji = _JOB_BASIC.job_input.model_copy(update={"params": p})
    job = _JOB_BASIC.model_copy(update={"job_input": ji})
    _wdl_fail(job, fm, None, None, ValueError(
        "If a manifest file is specified in the job parameters, manifest_file_list "
        + "is required and its length must match the number of containers for the job"
    ))
    _wdl_fail(job, fm, [], None, ValueError(
        "If a manifest file is specified in the job parameters, manifest_file_list "
        + "is required and its length must match the number of containers for the job"
    ))
    _wdl_fail(job, fm, [Path("foo"), Path("bar")], None, ValueError(
        "If a manifest file is specified in the job parameters, manifest_file_list "
        + "is required and its length must match the number of containers for the job"
    ))
    for char in BADCHARS:
        err = re.escape(f"Disallowed manifest path: b{char}ar")
        _wdl_fail(job, fm, [Path(f"b{char}ar")], None, ValueError(err))
        _wdl_fail(job, fm, [Path(f"b{char}ar/foo")], None, ValueError(err))


def test_wdl_fail_file_mapping():
    fm = {models.S3FileWithDataID(file="bucket/file2"): Path("foo/bar")}
    _wdl_fail(_JOB_BASIC, fm, None, None, ValueError(
        "file_mapping missing file='bucket/file1' crc64nvme=None data_id=None"
    ))
    for char in BADCHARS:
        err = re.escape(
            "Disallowed local path for S3 object file='bucket/file2' crc64nvme=None data_id=None: "
            + f"fo{char}o"
        )
        fm = {models.S3FileWithDataID(file="bucket/file2"): Path(f"fo{char}o")}
        _wdl_fail(_JOB_BASIC, fm, None, None, ValueError(err))
        fm = {models.S3FileWithDataID(file="bucket/file2"): Path(f"fo{char}o/bar")}
        _wdl_fail(_JOB_BASIC, fm, None, None, ValueError(err))


def _wdl_fail(
    job: models.Job,
    file_mapping: dict[models.S3FileWithDataID: Path],
    manifest_file_list: list[Path],
    relative_refdata_path: Path,
    expected: Exception,
):
    with pytest.raises(type(expected), match=f"{expected.args[0]}"):
        wdl.generate_wdl(job, file_mapping, manifest_file_list, relative_refdata_path)
