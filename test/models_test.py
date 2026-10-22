# TODO TEST add  more tests

from pydantic import ValidationError
import pytest
import re
from pytest import raises

from cdmtaskservice import models
from cdmtaskservice import sites

# These tests deliberately read deprecated fields (refdata_mount_point /
# default_refdata_mount_point) to confirm their backwards compatible behavior, so the
# resulting DeprecationWarning is expected rather than a signal of accidental deprecated
# field use elsewhere in the code.
_IGNORE_MOUNT_POINT_DEPRECATION = pytest.mark.filterwarnings("ignore::DeprecationWarning")


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
        exp.append([models.S3FileWithDataID(file=f"foo/bar{i}") for i in lst])
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


def test_job_state_active_states():
    assert models.JobState.active_states() == {
        models.JobState.CREATED,
        models.JobState.DOWNLOAD_SUBMITTED,
        models.JobState.JOB_SUBMITTING,
        models.JobState.JOB_SUBMITTED,
        models.JobState.UPLOAD_SUBMITTING,
        models.JobState.UPLOAD_SUBMITTED,
        models.JobState.ERROR_PROCESSING_SUBMITTING,
        models.JobState.ERROR_PROCESSING_SUBMITTED,
    }


def test_job_state_is_active():
    expected = {s: False for s in list(models.JobState)}
    expected[models.JobState.CREATED] = True
    expected[models.JobState.DOWNLOAD_SUBMITTED] = True
    expected[models.JobState.JOB_SUBMITTING] = True
    expected[models.JobState.JOB_SUBMITTED] = True
    expected[models.JobState.UPLOAD_SUBMITTING] = True
    expected[models.JobState.UPLOAD_SUBMITTED] = True
    expected[models.JobState.ERROR_PROCESSING_SUBMITTING] = True
    expected[models.JobState.ERROR_PROCESSING_SUBMITTED] = True

    for s, active in expected.items():
        assert s.is_active() is active


def test_refdata_state_terminal_states():
    assert models.StagingState.terminal_states() == {
        models.StagingState.ERROR, models.StagingState.COMPLETE
    }


def test_refdata_state_is_terminal():
    expected = {s: False for s in list(models.StagingState)}
    expected[models.StagingState.COMPLETE] = True
    expected[models.StagingState.ERROR] = True

    for s, term in expected.items():
        assert s.is_terminal() is term


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_parameters_staged_data_mount_point_new_field():
    p = models.Parameters(staged_data_mount_point="/foo")
    assert p.staged_data_mount_point == "/foo"
    assert p.refdata_mount_point == "/foo"


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_parameters_staged_data_mount_point_deprecated_field_promoted():
    p = models.Parameters(refdata_mount_point="/foo")
    assert p.staged_data_mount_point == "/foo"
    assert p.refdata_mount_point == "/foo"


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_parameters_staged_data_mount_point_new_field_takes_precedence():
    p = models.Parameters(staged_data_mount_point="/new", refdata_mount_point="/old")
    assert p.staged_data_mount_point == "/new"
    assert p.refdata_mount_point == "/new"


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_parameters_staged_data_mount_point_unset():
    p = models.Parameters()
    assert p.staged_data_mount_point is None
    assert p.refdata_mount_point is None


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_parameters_null_deprecated_fields():
    p = models.Parameters(staged_data_mount_point="/foo")
    p2 = p.null_deprecated_fields()
    assert p2.staged_data_mount_point == "/foo"
    assert p2.refdata_mount_point is None
    # the original instance is unmodified
    assert p.staged_data_mount_point == "/foo"
    assert p.refdata_mount_point == "/foo"


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_parameters_staged_data_mount_point_promotion_handles_non_dict_input():
    # The before-validator's isinstance(v, dict) guard means non-dict input (an existing
    # instance, or invalid input like a bare string) is passed through unchanged rather than
    # crashing with an unrelated AttributeError from calling .get() on it.
    p = models.Parameters(staged_data_mount_point="/foo")
    p2 = models.Parameters.model_validate(p)
    assert p2.staged_data_mount_point == "/foo"
    assert p2.refdata_mount_point == "/foo"

    with raises(ValidationError, match="Input should be a valid dictionary or instance"):
        models.Parameters.model_validate("not a dict")


_JOB_IMAGE_REQUIRED_FIELDS = {
    "registered_by": "user",
    "registered_on": "2024-10-24T22:35:40Z",
    "name": "n",
    "digest": "sha256:" + "a" * 64,
    "entrypoint": ["run"],
}


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_job_image_default_staged_data_mount_point_new_field():
    ji = models.JobImage.model_validate(
        {**_JOB_IMAGE_REQUIRED_FIELDS, "default_staged_data_mount_point": "/foo"}
    )
    assert ji.default_staged_data_mount_point == "/foo"
    assert ji.default_refdata_mount_point == "/foo"


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_job_image_default_staged_data_mount_point_deprecated_field_promoted():
    ji = models.JobImage.model_validate(
        {**_JOB_IMAGE_REQUIRED_FIELDS, "default_refdata_mount_point": "/foo"}
    )
    assert ji.default_staged_data_mount_point == "/foo"
    assert ji.default_refdata_mount_point == "/foo"


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_job_image_default_staged_data_mount_point_new_field_takes_precedence():
    ji = models.JobImage.model_validate({
        **_JOB_IMAGE_REQUIRED_FIELDS,
        "default_staged_data_mount_point": "/new",
        "default_refdata_mount_point": "/old",
    })
    assert ji.default_staged_data_mount_point == "/new"
    assert ji.default_refdata_mount_point == "/new"


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_job_image_default_staged_data_mount_point_unset():
    ji = models.JobImage.model_validate(_JOB_IMAGE_REQUIRED_FIELDS)
    assert ji.default_staged_data_mount_point is None
    assert ji.default_refdata_mount_point is None


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_job_image_null_deprecated_fields():
    ji = models.JobImage.model_validate(
        {**_JOB_IMAGE_REQUIRED_FIELDS, "default_staged_data_mount_point": "/foo"}
    )
    ji2 = ji.null_deprecated_fields()
    assert ji2.default_staged_data_mount_point == "/foo"
    assert ji2.default_refdata_mount_point is None
    # the original instance is unmodified
    assert ji.default_staged_data_mount_point == "/foo"
    assert ji.default_refdata_mount_point == "/foo"


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_job_image_default_staged_data_mount_point_promotion_handles_non_dict_input():
    # See test_parameters_staged_data_mount_point_promotion_handles_non_dict_input for why
    # this matters.
    ji = models.JobImage.model_validate(
        {**_JOB_IMAGE_REQUIRED_FIELDS, "default_staged_data_mount_point": "/foo"}
    )
    ji2 = models.JobImage.model_validate(ji)
    assert ji2.default_staged_data_mount_point == "/foo"
    assert ji2.default_refdata_mount_point == "/foo"

    with raises(ValidationError, match="Input should be a valid dictionary or instance"):
        models.JobImage.model_validate("not a dict")


def _job_with_staged_data_mount_points(param_mount_point, image_default_mount_point):
    return models.Job.model_construct(
        image=models.JobImage.model_construct(
            default_staged_data_mount_point=image_default_mount_point
        ),
        job_input=models.JobInput.model_construct(
            params=models.Parameters.model_construct(
                staged_data_mount_point=param_mount_point
            )
        ),
    )


def test_job_get_staged_data_mount_point_from_params():
    job = _job_with_staged_data_mount_points("/params", "/image")
    assert job.get_staged_data_mount_point() == "/params"


def test_job_get_staged_data_mount_point_from_image_default():
    job = _job_with_staged_data_mount_points(None, "/image")
    assert job.get_staged_data_mount_point() == "/image"


def test_job_get_staged_data_mount_point_none():
    job = _job_with_staged_data_mount_points(None, None)
    assert job.get_staged_data_mount_point() is None


@_IGNORE_MOUNT_POINT_DEPRECATION
def test_job_null_deprecated_fields():
    # Job.null_deprecated_fields() delegates to JobInput.null_deprecated_fields() and
    # JobImage.null_deprecated_fields(); those methods' own promotion/sync behavior is covered
    # by test_parameters_null_deprecated_fields and test_job_image_null_deprecated_fields, so
    # here we only need real (validated) nested Parameters/JobImage instances to confirm the
    # delegation is wired correctly - model_construct would bypass the sync validators that
    # populate the deprecated fields in the first place.
    params = models.Parameters(staged_data_mount_point="/params")
    image = models.JobImage.model_validate(
        {**_JOB_IMAGE_REQUIRED_FIELDS, "default_staged_data_mount_point": "/image"}
    )
    job = models.Job.model_construct(
        image=image,
        job_input=models.JobInput.model_construct(params=params),
    )
    job2 = job.null_deprecated_fields()
    assert job2.job_input.params.staged_data_mount_point == "/params"
    assert job2.job_input.params.refdata_mount_point is None
    assert job2.image.default_staged_data_mount_point == "/image"
    assert job2.image.default_refdata_mount_point is None
    # the original instance is unmodified
    assert job.job_input.params.refdata_mount_point == "/params"
    assert job.image.default_refdata_mount_point == "/image"


_SAFE_TIME = "2024-10-24T22:35:40Z"


def test_ai_model_key_str():
    key = models.AIModelKey(models.AIModelSource.HUGGINGFACE, "org/name", "abc123def456")
    assert str(key) == "huggingface/org/name/abc123def456"


def test_ai_model_key_property():
    am = models.AIModel(
        source=models.AIModelSource.HUGGINGFACE,
        model="org/name",
        version="abc123def456",
        size_bytes=123456,
        registered_by="someuser",
        registered_on=_SAFE_TIME,
        statuses=[],
    )
    assert am.key == models.AIModelKey(
        models.AIModelSource.HUGGINGFACE, "org/name", "abc123def456"
    )
    adm = models.AdminAIModel(
        source=models.AIModelSource.HUGGINGFACE,
        model="org/name",
        version="abc123def456",
        size_bytes=123456,
        registered_by="someuser",
        registered_on=_SAFE_TIME,
        statuses=[],
    )
    assert adm.key == models.AIModelKey(
        models.AIModelSource.HUGGINGFACE, "org/name", "abc123def456"
    )


def test_ai_model_entity_type_and_is_pipeline():
    am = models.AIModel(
        source=models.AIModelSource.HUGGINGFACE,
        model="org/name",
        version="abc123def456",
        size_bytes=123456,
        registered_by="someuser",
        registered_on=_SAFE_TIME,
        statuses=[],
    )
    assert am.get_entity_type() == models.EntityType.AI_MODEL
    assert am.is_pipeline() is False
    adm = models.AdminAIModel(
        source=models.AIModelSource.HUGGINGFACE,
        model="org/name",
        version="abc123def456",
        size_bytes=123456,
        registered_by="someuser",
        registered_on=_SAFE_TIME,
        statuses=[],
    )
    assert adm.get_entity_type() == models.EntityType.AI_MODEL
    assert adm.is_pipeline() is False


def test_ai_model_tags_unset_by_default():
    am = models.AIModel(
        source=models.AIModelSource.HUGGINGFACE,
        model="org/name",
        version="abc123def456",
        size_bytes=123456,
        registered_by="someuser",
        registered_on=_SAFE_TIME,
        statuses=[],
    )
    assert am.tags is None
    assert "tags" not in am.model_dump(exclude_none=True)


def test_ai_model_tags_populated():
    am = models.AIModel(
        source=models.AIModelSource.HUGGINGFACE,
        model="org/name",
        version="abc123def456",
        size_bytes=123456,
        registered_by="someuser",
        registered_on=_SAFE_TIME,
        statuses=[],
        tags=["main", "latest"],
    )
    assert am.tags == ["main", "latest"]


def _ai_model_status(cluster: sites.Cluster, state: models.StagingState) -> models.StagingStatus:
    return models.StagingStatus(
        cluster=cluster,
        state=state,
        transition_times=[models.StagingStateTransition(state=state, time=_SAFE_TIME)],
    )


def test_ai_model_get_status_for_cluster():
    perl_status = _ai_model_status(sites.Cluster.PERLMUTTER_JAWS, models.StagingState.CREATED)
    kbase_status = _ai_model_status(sites.Cluster.KBASE, models.StagingState.COMPLETE)
    am = models.AIModel(
        source=models.AIModelSource.HUGGINGFACE,
        model="org/name",
        version="abc123def456",
        size_bytes=123456,
        registered_by="someuser",
        registered_on=_SAFE_TIME,
        statuses=[perl_status, kbase_status],
    )
    assert am.get_status_for_cluster(sites.Cluster.PERLMUTTER_JAWS) == perl_status
    assert am.get_status_for_cluster(sites.Cluster.KBASE) == kbase_status


def test_ai_model_get_status_for_cluster_fail_no_status():
    am = models.AIModel(
        source=models.AIModelSource.HUGGINGFACE,
        model="org/name",
        version="abc123def456",
        size_bytes=123456,
        registered_by="someuser",
        registered_on=_SAFE_TIME,
        statuses=[_ai_model_status(sites.Cluster.KBASE, models.StagingState.COMPLETE)],
    )
    with raises(models.NoAIModelClusterStatusError, match=(
        "^No status for cluster perlmutter-jaws for AI model "
        + re.escape("huggingface/org/name/abc123def456")
        + "$"
    )):
        am.get_status_for_cluster(sites.Cluster.PERLMUTTER_JAWS)
