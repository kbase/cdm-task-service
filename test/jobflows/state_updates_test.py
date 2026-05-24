import datetime
import logging
import pytest
import re
from unittest.mock import ANY, create_autospec, patch

from cdmtaskservice import logfields, models, sites, update_state
from cdmtaskservice.jobflows.state_updates import (
    JobFlowStateUpdates,
    SubjobFlowStateUpdates,
    ParentJobUpdate,
)
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice.timestamp import utcdatetime
from cdmtaskservice.update_state import UpdateField


_T = utcdatetime()
_T1 = datetime.datetime(2025, 3, 31, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
_T2 = datetime.datetime(2025, 3, 31, 12, 1, 0, 345000, tzinfo=datetime.timezone.utc)
_EXPLICIT_T = datetime.datetime(2025, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
_TRANS_ID = "test-trans-id"
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


def _make_sfsu():
    mongo = create_autospec(MongoDAO, spec_set=True, instance=True)
    kafka = create_autospec(KafkaNotifier, spec_set=True, instance=True)
    sfsu = SubjobFlowStateUpdates(sites.Cluster.KBASE, mongo, kafka)
    return mongo, sfsu


def _make_jfsu():
    mongo = create_autospec(MongoDAO, spec_set=True, instance=True)
    kafka = create_autospec(KafkaNotifier, spec_set=True, instance=True)
    jfsu = JobFlowStateUpdates(
        sites.Cluster.KBASE, mongo, kafka,
        _timestamp_fn=lambda: _T,
        _trans_id_fn=lambda: _TRANS_ID,
    )
    return mongo, kafka, jfsu


def test_constructor_bad_args():
    mongo = create_autospec(MongoDAO, spec_set=True, instance=True)
    kafka = create_autospec(KafkaNotifier, spec_set=True, instance=True)

    with pytest.raises(ValueError, match="^cluster is required$"):
        JobFlowStateUpdates(None, mongo, kafka)
    with pytest.raises(ValueError, match="^mongo is required$"):
        JobFlowStateUpdates(sites.Cluster.KBASE, None, kafka)
    with pytest.raises(ValueError, match="^kafka is required$"):
        JobFlowStateUpdates(sites.Cluster.KBASE, mongo, None)


async def test_update_job_state():
    mongo, kafka, jfsu = _make_jfsu()
    upd = update_state.submitting_job()

    await jfsu.update_job_state("jid", upd)

    mongo.update_job_state.assert_called_once_with(
        "jid", upd, _T, _TRANS_ID, recovery_cooldown=None
    )
    kafka.update_job_state.assert_called_once_with(
        "jid", upd.new_state, _T, _TRANS_ID, callback=ANY
    )
    await kafka.update_job_state.call_args.kwargs["callback"]
    mongo.job_update_sent.assert_called_once_with("jid", _TRANS_ID)


async def test_update_job_state_explicit_time():
    mongo, kafka, jfsu = _make_jfsu()
    upd = update_state.submitting_job()

    await jfsu.update_job_state("jid", upd, update_time=_EXPLICIT_T)

    mongo.update_job_state.assert_called_once_with(
        "jid", upd, _EXPLICIT_T, _TRANS_ID, recovery_cooldown=None
    )
    kafka.update_job_state.assert_called_once_with(
        "jid", upd.new_state, _EXPLICIT_T, _TRANS_ID, callback=ANY
    )
    await kafka.update_job_state.call_args.kwargs["callback"]
    mongo.job_update_sent.assert_called_once_with("jid", _TRANS_ID)


async def test_update_job_state_with_recovery_cooldown():
    mongo, kafka, jfsu = _make_jfsu()
    upd = update_state.recovering()
    cooldown = datetime.timedelta(minutes=10)

    await jfsu.update_job_state("jid", upd, recovery_cooldown=cooldown)

    mongo.update_job_state.assert_called_once_with(
        "jid", upd, _T, _TRANS_ID, recovery_cooldown=cooldown
    )
    kafka.update_job_state.assert_called_once_with(
        "jid", upd.new_state, _T, _TRANS_ID, callback=ANY
    )
    await kafka.update_job_state.call_args.kwargs["callback"]
    mongo.job_update_sent.assert_called_once_with("jid", _TRANS_ID)


async def test_update_job_state_bad_args():
    _, _, jfsu = _make_jfsu()
    upd = update_state.submitting_job()

    for bad in [None, "   \t  "]:
        with pytest.raises(ValueError, match="^job_id is required$"):
            await jfsu.update_job_state(bad, upd)
    with pytest.raises(ValueError, match="^update is required$"):
        await jfsu.update_job_state("jid", None)


async def test_update_refdata_state():
    mongo, kafka, jfsu = _make_jfsu()
    upd = update_state.submitted_refdata_download()

    await jfsu.update_refdata_state("rid", upd)

    mongo.update_refdata_state.assert_called_once_with(sites.Cluster.KBASE, "rid", upd, _T)
    kafka.update_job_state.assert_not_called()


async def test_update_refdata_state_bad_args():
    _, _, jfsu = _make_jfsu()
    upd = update_state.submitted_refdata_download()

    for bad in [None, "   \t  "]:
        with pytest.raises(ValueError, match="^refdata_id is required$"):
            await jfsu.update_refdata_state(bad, upd)
    with pytest.raises(ValueError, match="^update is required$"):
        await jfsu.update_refdata_state("rid", None)


async def test_save_error_job():
    mongo, kafka, jfsu = _make_jfsu()

    await jfsu.save_error("jid", "user err", "admin err")

    mongo.update_job_state.assert_called_once_with(
        "jid",
        update_state.error("admin err", user_error="user err"),
        _T, _TRANS_ID,
        recovery_cooldown=None,
    )
    kafka.update_job_state.assert_called_once_with(
        "jid", models.JobState.ERROR, _T, _TRANS_ID, callback=ANY
    )
    await kafka.update_job_state.call_args.kwargs["callback"]
    mongo.job_update_sent.assert_called_once_with("jid", _TRANS_ID)


async def test_save_error_job_with_traceback_and_logpath():
    mongo, kafka, jfsu = _make_jfsu()

    await jfsu.save_error("jid", "user err", "admin err", traceback="tb", logpath="logs/jid")

    mongo.update_job_state.assert_called_once_with(
        "jid",
        update_state.error(
            "admin err", user_error="user err", traceback="tb", log_files_path="logs/jid"
        ),
        _T, _TRANS_ID,
        recovery_cooldown=None,
    )
    kafka.update_job_state.assert_called_once_with(
        "jid", models.JobState.ERROR, _T, _TRANS_ID, callback=ANY
    )
    await kafka.update_job_state.call_args.kwargs["callback"]
    mongo.job_update_sent.assert_called_once_with("jid", _TRANS_ID)


async def test_save_error_refdata():
    mongo, kafka, jfsu = _make_jfsu()

    await jfsu.save_error("rid", "user err", "admin err", refdata=True)

    mongo.update_refdata_state.assert_called_once_with(
        sites.Cluster.KBASE,
        "rid",
        update_state.refdata_error("user err", "admin err"),
        _T,
    )
    kafka.update_job_state.assert_not_called()


async def test_save_error_refdata_with_traceback():
    mongo, _, jfsu = _make_jfsu()

    await jfsu.save_error("rid", "user err", "admin err", traceback="tb", refdata=True)

    mongo.update_refdata_state.assert_called_once_with(
        sites.Cluster.KBASE,
        "rid",
        update_state.refdata_error("user err", "admin err", traceback="tb"),
        _T,
    )


async def test_save_error_bad_args():
    _, _, jfsu = _make_jfsu()

    for bad in [None, "   \t  "]:
        with pytest.raises(ValueError, match="^entity_id is required$"):
            await jfsu.save_error(bad, "user err", "admin err")
        with pytest.raises(ValueError, match="^user_err is required$"):
            await jfsu.save_error("jid", bad, "admin err")
        with pytest.raises(ValueError, match="^admin_err is required$"):
            await jfsu.save_error("jid", "user err", bad)


async def test_handle_exception_job(caplog):
    mongo, kafka, jfsu = _make_jfsu()

    with caplog.at_level(logging.ERROR, logger="cdmtaskservice.jobflows.state_updates"):
        try:
            raise ValueError("test error")
        except ValueError as e:
            await jfsu.handle_exception(e, "jid", "testing")

    tb = mongo.update_job_state.call_args.args[1].update_fields[UpdateField.TRACEBACK]
    assert "ValueError: test error" in tb
    mongo.update_job_state.assert_called_once_with(
        "jid",
        update_state.error("test error", user_error="An unexpected error occurred", traceback=tb),
        _T,
        _TRANS_ID,
        recovery_cooldown=None,
    )
    kafka.update_job_state.assert_called_once_with(
        "jid", models.JobState.ERROR, _T, _TRANS_ID, callback=ANY
    )
    await kafka.update_job_state.call_args.kwargs["callback"]
    mongo.job_update_sent.assert_called_once_with("jid", _TRANS_ID)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].message == "Error testing job."
    assert caplog.records[0].__dict__[logfields.JOB_ID] == "jid"


async def test_handle_exception_refdata(caplog):
    mongo, kafka, jfsu = _make_jfsu()

    with caplog.at_level(logging.ERROR, logger="cdmtaskservice.jobflows.state_updates"):
        try:
            raise ValueError("refdata error")
        except ValueError as e:
            await jfsu.handle_exception(e, "rid", "test test testing", refdata=True)

    tb = mongo.update_refdata_state.call_args.args[2].update_fields[UpdateField.TRACEBACK]
    assert "ValueError: refdata error" in tb
    mongo.update_refdata_state.assert_called_once_with(
        sites.Cluster.KBASE,
        "rid",
        update_state.refdata_error("An unexpected error occurred", "refdata error", traceback=tb),
        _T,
    )
    kafka.update_job_state.assert_not_called()
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.ERROR
    assert caplog.records[0].message == "Error test test testing refdata."
    assert caplog.records[0].__dict__[logfields.REFDATA_ID] == "rid"


async def test_handle_exception_bad_args():
    _, _, jfsu = _make_jfsu()

    with pytest.raises(ValueError, match="^e is required$"):
        await jfsu.handle_exception(None, "jid", "testing")
    for bad in [None, "   \t  "]:
        with pytest.raises(ValueError, match="^entity_id is required$"):
            await jfsu.handle_exception(ValueError("x"), bad, "testing")
        with pytest.raises(ValueError, match="^erraction is required$"):
            await jfsu.handle_exception(ValueError("x"), "jid", bad)


async def test_get_parent_job_update_fail_bad_args():
    _, sfsu = _make_sfsu()
    s = models.JobState.COMPLETE

    await _get_parent_job_update_fail(sfsu, None, s, ValueError("job is required"))
    await _get_parent_job_update_fail(sfsu, _JOB, None, ValueError(
        "subjob_transition is required")
    )

    with patch.object(models.JobState, "is_terminal", return_value=False):
        await _get_parent_job_update_fail(sfsu, _JOB, s, ValueError(
            "Seems like someone added a state without updating this method, oops"
        ))


async def _get_parent_job_update_fail(sfsu, job, state, expected):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await sfsu.get_parent_job_update(job, state)


async def test_get_parent_job_update_fail_single_states():
    excluded = models.JobState.canceling_states() | {models.JobState.RECOVERING}
    for s in set(models.JobState) - excluded:
        await _run_get_job_fail(s, {s: (0, None)}, ValueError(
            f"You reported that a subjob transitioned to state {s.value} but no subjobs are "
            + "in that state"
        ))
        await _run_get_job_fail(s, {s: (4, _T2)}, ValueError(
            "More subjobs found (4) than containers (3)"
        ))


async def test_get_parent_job_update_fail_canceling_states():
    for s in models.JobState.canceling_states():
        await _run_get_job_fail(s, {s: (0, None)}, ValueError(
            "Subjobs cannot transition to the canceling states."
        ))


async def test_get_parent_job_update_fail_recovering_state():
    await _run_get_job_fail(models.JobState.RECOVERING, {models.JobState.RECOVERING: (0, None)},
        ValueError("Subjobs cannot transition to the recovering state.")
    )


async def test_get_parent_job_update_fail_multiple_states():
    usg = models.JobState.UPLOAD_SUBMITTING
    esg = models.JobState.ERROR_PROCESSING_SUBMITTING
    usd = models.JobState.UPLOAD_SUBMITTED
    esd = models.JobState.ERROR_PROCESSING_SUBMITTED
    e = models.JobState.ERROR
    c = models.JobState.COMPLETE

    await _run_get_job_fail(usg, {usg: (0, None), esg: (2, _T1)}, ValueError(
        "You reported that a subjob transitioned to state upload_submitting "
        + "but no subjobs are in that state"
    ))
    await _run_get_job_fail(esg, {usg: (2, _T2), esg: (2, _T1)}, ValueError(
        "More subjobs found (4) than containers (3)"
    ))
    await _run_get_job_fail(esd, {usd: (1, _T2), esd: (0, None)}, ValueError(
        "You reported that a subjob transitioned to state error_processing_submitted "
        + "but no subjobs are in that state"
    ))
    await _run_get_job_fail(usd, {usd: (3, _T2), esd: (2, _T1)}, ValueError(
        "More subjobs found (5) than containers (3)"
    ))
    await _run_get_job_fail(c, {c: (0, None), e: (0, None)}, ValueError(
        "You reported that a subjob transitioned to state complete "
        + "but no subjobs are in that state"
    ))
    await _run_get_job_fail(e, {c: (0, None), e: (26, _T1)}, ValueError(
        "More subjobs found (26) than containers (3)"
    ))


async def _run_get_job_fail(
    state: models.JobState,
    ret: dict[models.JobState, tuple[int, datetime.datetime]],
    expected: Exception,
):
    mongo, sfsu = _make_sfsu()
    mongo.have_subjobs_reached_state.return_value = ret

    with pytest.raises(type(expected), match=f"^{re.escape(expected.args[0])}$"):
        await sfsu.get_parent_job_update(_JOB, state)


async def test_get_parent_job_update_basic_states():
    states = [
        models.JobState.CREATED,
        models.JobState.DOWNLOAD_SUBMITTED,
        models.JobState.JOB_SUBMITTING,
        models.JobState.JOB_SUBMITTED,
    ]
    for s in states:
        await _run_get_parent_job_update_basic_states(s, {s: (1, _T2)}, None)
        await _run_get_parent_job_update_basic_states(s, {s: (2, _T1)}, None)
        await _run_get_parent_job_update_basic_states(s, {s: (3, _T1)}, ParentJobUpdate(s, _T1))


async def _run_get_parent_job_update_basic_states(
    state: models.JobState,
    ret: dict[models.JobState, tuple[int, datetime.datetime]],
    expected: ParentJobUpdate | None,
):
    mongo, sfsu = _make_sfsu()
    mongo.have_subjobs_reached_state.return_value = ret

    assert await sfsu.get_parent_job_update(_JOB, state) == expected

    mongo.have_subjobs_reached_state.assert_called_once_with("foo", state)


async def test_get_parent_job_update_paired_states():
    await _get_parent_job_update_paired_states_cases(
        models.JobState.UPLOAD_SUBMITTING, models.JobState.ERROR_PROCESSING_SUBMITTING
    )
    await _get_parent_job_update_paired_states_cases(
        models.JobState.UPLOAD_SUBMITTED, models.JobState.ERROR_PROCESSING_SUBMITTED
    )
    await _get_parent_job_update_paired_states_cases(
        models.JobState.COMPLETE, models.JobState.ERROR
    )


async def _get_parent_job_update_paired_states_cases(state: models.JobState, err: models.JobState):
    states = [state, err]

    ret = {state: (1, _T1), err: (0, None)}
    await _run_get_parent_job_update_paired_states(state, ret, states, None)

    ret = {state: (0, None), err: (1, _T1)}
    await _run_get_parent_job_update_paired_states(err, ret, states, None)

    ret = {state: (1, _T1), err: (1, _T2)}
    await _run_get_parent_job_update_paired_states(state, ret, states, None)
    await _run_get_parent_job_update_paired_states(err, ret, states, None)

    ret = {state: (3, _T1), err: (0, None)}
    await _run_get_parent_job_update_paired_states(state, ret, states, ParentJobUpdate(state, _T1))

    ret = {state: (2, _T1), err: (1, _T2)}
    await _run_get_parent_job_update_paired_states(state, ret, states, ParentJobUpdate(err, _T2))
    await _run_get_parent_job_update_paired_states(err, ret, states, ParentJobUpdate(err, _T2))

    ret = {state: (2, _T2), err: (1, _T1)}
    await _run_get_parent_job_update_paired_states(state, ret, states, ParentJobUpdate(err, _T2))
    await _run_get_parent_job_update_paired_states(err, ret, states, ParentJobUpdate(err, _T2))

    ret = {state: (0, None), err: (3, _T2)}
    await _run_get_parent_job_update_paired_states(err, ret, states, ParentJobUpdate(err, _T2))


async def _run_get_parent_job_update_paired_states(
    state: models.JobState,
    ret: dict[models.JobState, tuple[int, datetime.datetime]],
    states: list[models.JobState],
    expected: ParentJobUpdate | None,
):
    mongo, sfsu = _make_sfsu()
    mongo.have_subjobs_reached_state.return_value = ret

    assert await sfsu.get_parent_job_update(_JOB, state) == expected

    mongo.have_subjobs_reached_state.assert_called_once_with("foo", *states)
