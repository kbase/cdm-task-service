import semver
from pydantic import ValidationError
import pytest

from cdmtaskservice import models, sites
from cdmtaskservice.pipelines.models import (
    AdminPipelineJob,
    PipelineJob,
    PipelineJobInput,
    PipelineJobPreview,
)


def _job_input(**overrides) -> PipelineJobInput:
    kwargs = dict(
        cluster=sites.Cluster.PERLMUTTER_JAWS,
        input={"output_prefix": "proj-xyz"},
        pipeline="readsqc",
        version=semver.Version.parse("0.1.0"),
        output_dir="mybucket/out",
    )
    kwargs.update(overrides)
    return PipelineJobInput(**kwargs)


def test_pipeline_job_input():
    pji = _job_input()

    assert pji.cluster == sites.Cluster.PERLMUTTER_JAWS
    assert pji.input == {"output_prefix": "proj-xyz"}
    assert pji.pipeline == "readsqc"
    assert pji.version == semver.Version.parse("0.1.0")
    assert pji.output_dir == "mybucket/out/"


def test_pipeline_job_input_version_accepts_string():
    pji = _job_input(version="0.1.0")

    assert pji.version == semver.Version.parse("0.1.0")
    assert isinstance(pji.version, semver.Version)


def test_pipeline_job_input_serializes_version_as_string():
    pji = _job_input()

    assert pji.model_dump() == {
        "cluster": sites.Cluster.PERLMUTTER_JAWS.value,
        "input": {"output_prefix": "proj-xyz"},
        "pipeline": "readsqc",
        "version": "0.1.0",
        "output_dir": "mybucket/out/",
    }


def test_pipeline_job_input_fail_bad_output_dir():
    with pytest.raises(ValidationError, match="output_dir"):
        _job_input(output_dir="")


def test_pipeline_job_input_fail_bad_cluster():
    with pytest.raises(ValidationError, match="cluster"):
        _job_input(cluster="not-a-cluster")


def test_pipeline_job_input_fail_extra_field():
    with pytest.raises(ValidationError, match="foo"):
        _job_input(foo="bar")


def test_pipeline_job_preview():
    pjp = PipelineJobPreview(
        id="jobid",
        state=models.JobState.COMPLETE,
        transition_times=[],
        user="user1",
        pipeline_input=_job_input(),
    )

    assert pjp.id == "jobid"
    assert pjp.state == models.JobState.COMPLETE
    assert pjp.transition_times == []
    assert pjp.user == "user1"
    assert pjp.admin_meta == {}
    assert pjp.pipeline_input == _job_input()
    assert not hasattr(pjp, "outputs")
    assert not hasattr(pjp, "trans_history")


def test_pipeline_job():
    pj = PipelineJob(
        id="jobid",
        state=models.JobState.COMPLETE,
        transition_times=[],
        user="user1",
        pipeline_input=_job_input(),
    )

    assert pj.id == "jobid"
    assert pj.state == models.JobState.COMPLETE
    assert pj.transition_times == []
    assert pj.user == "user1"
    assert pj.admin_meta == {}
    assert pj.pipeline_input == _job_input()
    assert pj.outputs is None
    assert pj.trans_history is None


def test_pipeline_job_with_outputs():
    outputs = [models.S3File(file="mybucket/out/results.txt")]
    pj = PipelineJob(
        id="jobid",
        state=models.JobState.COMPLETE,
        transition_times=[],
        user="user1",
        pipeline_input=_job_input(),
        outputs=outputs,
    )

    assert pj.outputs == outputs


def test_admin_pipeline_job():
    apj = AdminPipelineJob(
        id="jobid",
        state=models.JobState.COMPLETE,
        transition_times=[],
        user="user1",
        pipeline_input=_job_input(),
    )

    assert apj.cleaned is False
    assert apj.nersc_details is None
    assert apj.jaws_details is None
    assert apj.admin_error is None
    assert apj.admin_error_history is None
    assert apj.traceback is None
    assert apj.trans_history is None
    assert not hasattr(apj, "htcondor_details")


def test_admin_pipeline_job_transition_times_are_admin_variant():
    apj = AdminPipelineJob(
        id="jobid",
        state=models.JobState.COMPLETE,
        transition_times=[models.AdminJobStateTransition(
            state=models.JobState.COMPLETE,
            time="2024-10-24T22:35:40Z",
            trans_id="foo",
            notif_sent=True,
        )],
        user="user1",
        pipeline_input=_job_input(),
    )

    assert apj.transition_times == [models.AdminJobStateTransition(
        state=models.JobState.COMPLETE,
        time="2024-10-24T22:35:40Z",
        trans_id="foo",
        notif_sent=True,
    )]

    with pytest.raises(ValidationError, match="trans_id"):
        AdminPipelineJob(
            id="jobid",
            state=models.JobState.COMPLETE,
            # missing trans_id / notif_sent required by AdminJobStateTransition
            transition_times=[models.JobStateTransition(
                state=models.JobState.COMPLETE, time="2024-10-24T22:35:40Z"
            ).model_dump()],
            user="user1",
            pipeline_input=_job_input(),
        )
