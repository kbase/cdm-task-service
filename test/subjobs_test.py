import datetime
import pytest
import re
from unittest.mock import create_autospec, patch

from cdmtaskservice import models, sites
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.subjobs import get_job_update, JobUpdate


_T1 = datetime.datetime(2025, 3, 31, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
_T2 = datetime.datetime(2025, 3, 31, 12, 1, 0, 345000, tzinfo=datetime.timezone.utc)
_JOB = models.Job(
    id="foo",
    job_input=models.JobInput(
        cluster=sites.Cluster.PERLMUTTER_JAWS,
        image="some_image",
        params=models.Parameters(),
        input_files=[
            models.S3FileWithDataID(file="bucket/file1"),
            models.S3FileWithDataID(file="bucket/file2"),
            models.S3FileWithDataID(file="bucket/file3"),
        ],
        output_dir="bucket/output",
        num_containers=3,
    ),
    user="user",
    image=models.JobImage(
        name="some_image",
        digest="digest",
        entrypoint=["arg1"],
        registered_by="someuser",
        registered_on=_T1,
    ),
    input_file_count=1,
    state=models.JobState.DOWNLOAD_SUBMITTED,
    transition_times=[
        models.AdminJobStateTransition(
            state=models.JobState.CREATED,
            time=_T2,
            trans_id="trans1",
            notif_sent=False,
        ),
    ]
)


@pytest.mark.asyncio
async def test_get_job_update_fail_bad_args():
    md = create_autospec(MongoDAO, spec_set=True, instance=True)
    s = models.JobState.COMPLETE
    
    await _get_job_update_fail(None, _JOB, s, ValueError("md is required"))
    await _get_job_update_fail(md, None, s, ValueError("job is required"))
    await _get_job_update_fail(md, _JOB, None, ValueError("subjob_transition is required"))
    
    with patch.object(models.JobState, "is_terminal", return_value=False):
        await _get_job_update_fail(md, _JOB, s, ValueError(
            "Seems like someone added a state without updating this method, oops"
        ))


async def _get_job_update_fail(md, job, state, expected):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await get_job_update(md, job, state)


@pytest.mark.asyncio
async def test_get_job_update_fail_single_states():
    for s in list(models.JobState):
        await _run_get_job_fail(s, {s: (0, None)}, ValueError(
            f"You reported that a subjob transitioned to state {s.value} but no subjobs are "
            + "in that state"
        ))
        await _run_get_job_fail(s, {s: (4, _T2)}, ValueError(
            "More subjobs found (4) than containers (3)"
        ))


@pytest.mark.asyncio
async def test_get_job_update_fail_multiple_states():
    usg = models.JobState.UPLOAD_SUBMITTING
    esg = models.JobState.ERROR_PROCESSING_SUBMITTING
    usd = models.JobState.UPLOAD_SUBMITTED
    esd = models.JobState.ERROR_PROCESSING_SUBMITTED
    e = models.JobState.ERROR
    c = models.JobState.COMPLETE
    
    await _run_get_job_fail(usg, {usg: (0, None), esg: (2, _T1)}, ValueError(
        f"You reported that a subjob transitioned to state upload_submitting "
        + "but no subjobs are in that state"
    ))
    await _run_get_job_fail(esg, {usg: (2, _T2), esg: (2, _T1)}, ValueError(
        "More subjobs found (4) than containers (3)"
    ))
    await _run_get_job_fail(esd, {usd: (1, _T2), esd: (0, None)}, ValueError(
        f"You reported that a subjob transitioned to state error_processing_submitted "
        + "but no subjobs are in that state"
    ))
    await _run_get_job_fail(usd, {usd: (3, _T2), esd: (2, _T1)}, ValueError(
        "More subjobs found (5) than containers (3)"
    ))
    await _run_get_job_fail(c, {c: (0, None), e: (0, None)}, ValueError(
        f"You reported that a subjob transitioned to state complete "
        + "but no subjobs are in that state"
    ))
    await _run_get_job_fail(e, {c: (0, None), e: (26, _T1)}, ValueError(
        "More subjobs found (26) than containers (3)"
    ))


async def _run_get_job_fail(
    state: models.JobState,
    ret: dict[models.JobState: tuple[int, datetime.datetime]],
    expected: Exception,
):
    md = create_autospec(MongoDAO, spec_set=True, instance=True)
    
    md.have_subjobs_reached_state.return_value = ret
 
    with pytest.raises(type(expected), match=f"^{re.escape(expected.args[0])}$"):
        await get_job_update(md, _JOB, state)


@pytest.mark.asyncio
async def test_get_job_update_basic_states():
    states = [
        models.JobState.CREATED,
        models.JobState.DOWNLOAD_SUBMITTED,
        models.JobState.JOB_SUBMITTING,
        models.JobState.JOB_SUBMITTED
    ]
    for s in states:
        await _run_get_job_update_basic_states(s, {s: (1, _T2)}, None)
        await _run_get_job_update_basic_states(s, {s: (2, _T1)}, None)
        await _run_get_job_update_basic_states(s, {s: (3, _T1)}, JobUpdate(s, _T1))


async def _run_get_job_update_basic_states(
    state: models.JobState,
    ret: dict[models.JobState: tuple[int, datetime.datetime]],
    expected: JobUpdate | None,
):
    md = create_autospec(MongoDAO, spec_set=True, instance=True)
    
    md.have_subjobs_reached_state.return_value = ret
    
    assert await get_job_update(md, _JOB, state) == expected
    
    md.have_subjobs_reached_state.assert_called_once_with("foo", state)


@pytest.mark.asyncio
async def test_get_job_update_paired_states():
    await _get_job_update_paired_states_cases(
        models.JobState.UPLOAD_SUBMITTING, models.JobState.ERROR_PROCESSING_SUBMITTING
    )
    await _get_job_update_paired_states_cases(
        models.JobState.UPLOAD_SUBMITTED, models.JobState.ERROR_PROCESSING_SUBMITTED
    )
    await _get_job_update_paired_states_cases(
        models.JobState.COMPLETE, models.JobState.ERROR
    )


async def _get_job_update_paired_states_cases(state: models.JobState, err: models.JobState):
    states = [state, err]
    
    ret = {state: (1, _T1), err: (0, None)}
    await _run_get_job_update_paired_states(state, ret, states, None)
    
    ret = {state: (0, None), err: (1, _T1)}
    await _run_get_job_update_paired_states(err, ret, states, None)
    
    ret = {state: (1, _T1), err: (1, _T2)}
    await _run_get_job_update_paired_states(state, ret, states, None)
    await _run_get_job_update_paired_states(err, ret, states, None)
    
    ret = {state: (3, _T1), err: (0, None)}
    await _run_get_job_update_paired_states(state, ret, states, JobUpdate(state, _T1))
    
    ret = {state: (2, _T1), err: (1, _T2)}
    await _run_get_job_update_paired_states(state, ret, states, JobUpdate(err, _T2))
    await _run_get_job_update_paired_states(err, ret, states, JobUpdate(err, _T2))
    
    ret = {state: (2, _T2), err: (1, _T1)}
    await _run_get_job_update_paired_states(state, ret, states, JobUpdate(err, _T2))
    await _run_get_job_update_paired_states(err, ret, states, JobUpdate(err, _T2))

    ret = {state: (0, None), err: (3, _T2)}
    await _run_get_job_update_paired_states(err, ret, states, JobUpdate(err, _T2))


async def _run_get_job_update_paired_states(
    state: models.JobState,
    ret: dict[models.JobState: tuple[int, datetime.datetime]],
    states: list[models.JobState],
    expected: JobUpdate | None,
):
    md = create_autospec(MongoDAO, spec_set=True, instance=True)
    
    md.have_subjobs_reached_state.return_value = ret
    
    assert await get_job_update(md, _JOB, state) == expected
    
    md.have_subjobs_reached_state.assert_called_once_with("foo", *states)
