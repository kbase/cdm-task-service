import datetime
import logging
import pytest
from unittest.mock import ANY, create_autospec

from cdmtaskservice import logfields, models, sites, update_state
from cdmtaskservice.jobflows.state_updates import JobFlowStateUpdates
from cdmtaskservice.mongo import MongoDAO
from cdmtaskservice.notifications.kafka_notifications import KafkaNotifier
from cdmtaskservice.timestamp import utcdatetime
from cdmtaskservice.update_state import UpdateField


_T = utcdatetime()
_TRANS_ID = "test-trans-id"
_EXPLICIT_T = datetime.datetime(2025, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)


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

    mongo.update_job_state.assert_called_once_with("jid", upd, _T, _TRANS_ID)
    kafka.update_job_state.assert_called_once_with(
        "jid", upd.new_state, _T, _TRANS_ID, callback=ANY
    )
    await kafka.update_job_state.call_args.kwargs["callback"]
    mongo.job_update_sent.assert_called_once_with("jid", _TRANS_ID)


async def test_update_job_state_explicit_time():
    mongo, kafka, jfsu = _make_jfsu()
    upd = update_state.submitting_job()

    await jfsu.update_job_state("jid", upd, update_time=_EXPLICIT_T)

    mongo.update_job_state.assert_called_once_with("jid", upd, _EXPLICIT_T, _TRANS_ID)
    kafka.update_job_state.assert_called_once_with(
        "jid", upd.new_state, _EXPLICIT_T, _TRANS_ID, callback=ANY
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
