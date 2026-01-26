# TODO TEST add  more tests

from pydantic import ValidationError
from pytest import raises

from cdmtaskservice import models
from cdmtaskservice import sites


def test_files_per_container():
    _files_per_container(1, 1, [[1]])
    _files_per_container(1, 2, [[1, 2]])
    _files_per_container(2, 2, [[1], [2]])
    _files_per_container(3, 3, [[1], [2], [3]])
    _files_per_container(3, 4, [[1, 2], [3], [4]])
    _files_per_container(3, 5, [[1, 2], [3, 4], [5]])
    _files_per_container(3, 6, [[1, 2], [3, 4], [5, 6]])
    _files_per_container(3, 7, [[1, 2, 3], [4, 5], [6, 7]])


def _files_per_container(containers: int, files: int, expfiles: list[list[int]]):
    ji = models.JobInput(
        cluster=sites.Cluster.PERLMUTTER_JAWS,
        image="fakeimage",
        params=models.Parameters(),
        num_containers=containers,
        input_files=[f"foo/bar{i}" for i in range(1, files + 1)],
        output_dir="foo/bar"
    )
    fpc = ji.get_files_per_container()
    exp = []
    for lst in expfiles:
        exp.append([f"foo/bar{i}" for i in lst])
    assert fpc == exp
    
    ji = models.JobInput(
        cluster=sites.Cluster.PERLMUTTER_JAWS,
        image="fakeimage",
        params=models.Parameters(),
        num_containers=containers,
        input_files=[models.S3FileWithDataID(file=f"foo/bar{i}") for i in range(1, files + 1)],
        output_dir="foo/bar"
    )
    fpc = ji.get_files_per_container()
    exp = []
    for lst in expfiles:
        exp.append([models.S3FileWithDataID(file=f"foo/bar{i}") for i in lst])
    assert fpc == exp


def test_job_input_fail_too_many_containers():
    err = ("1 validation error for JobInput\n  Value error, num_containers must be less than or "
           + "equal to the number of input files")
    with raises(ValidationError, match=err):
        models.JobInput(
            cluster=sites.Cluster.PERLMUTTER_JAWS,
            image="fakeimage",
            params=models.Parameters(),
            num_containers=3,
            input_files=[f"foo/bar{i}" for i in range(1, 3)],
            output_dir="foo/bar"
    )


def test_job_state_terminal_states():
    assert models.JobState.terminal_states() == {
        models.JobState.ERROR, models.JobState.COMPLETE, models.JobState.CANCELED
    }


def test_job_state_is_terminal():
    expected = {s: False for s in list(models.JobState)}
    expected[models.JobState.COMPLETE] = True
    expected[models.JobState.ERROR] = True
    expected[models.JobState.CANCELED] = True
    
    for s, term in expected.items():
        assert s.is_terminal() is term


def test_job_state_canceling_states():
    assert models.JobState.canceling_states() == {
        models.JobState.CANCELING, models.JobState.CANCELED
    }


def test_job_state_is_canceling():
    expected = {s: False for s in list(models.JobState)}
    expected[models.JobState.CANCELING] = True
    expected[models.JobState.CANCELED] = True
    
    for s, term in expected.items():
        assert s.is_canceling() is term


def test_refdata_state_terminal_states():
    assert models.ReferenceDataState.terminal_states() == {
        models.ReferenceDataState.ERROR, models.ReferenceDataState.COMPLETE
    }


def test_refdata_state_is_terminal():
    expected = {s: False for s in list(models.ReferenceDataState)}
    expected[models.ReferenceDataState.COMPLETE] = True
    expected[models.ReferenceDataState.ERROR] = True
    
    for s, term in expected.items():
        assert s.is_terminal() is term
