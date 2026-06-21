
from bson.son import SON
import datetime
import re
from pymongo.errors import BulkWriteError, DuplicateKeyError
import pytest
from typing import Coroutine, Callable, Any

from cdmtaskservice import models
from cdmtaskservice import sites
from cdmtaskservice.exceptions import InvalidJobStateError, JobRecoveryError
from cdmtaskservice.mongo import (
    MissingSubJobError,
    JobUpdateConflictError,
    MongoDAO,
    NoSuchJobError,
    NoSuchReferenceDataError,
    NoSuchSubJobError,
    SubJobUpdateConflictError,
)
from cdmtaskservice.update_state import (
    error,
    force_recovering,
    recovering,
    submitted_download,
    submitted_jaws_job,
    submitted_nersc_refdata_download,
    submitting_job,
    submitting_upload,
    submitting_upload_with_exit_code,
)

from conftest import (
    mongo,  # @UnusedImport
    mondb,  # @UnusedImport
    MONGO_TEST_DB,
)

# TODO TEST add more tests

# Mongo only has millisecond precision
_SAFE_TIME = datetime.datetime(2025, 3, 31, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
_BASEJOB = models.AdminJobDetails(
    id="foo",
    job_input=models.JobInput(
        cluster=sites.Cluster.PERLMUTTER_JAWS,
        image="some_image",
        params=models.Parameters(),
        input_files=[models.S3FileWithDataID(file="bucket/file")],
        output_dir="bucket/output",
    ),
    user="user",
    image=models.JobImage(
        name="some_image",
        digest="digest",
        entrypoint=["arg1"],
        registered_by="someuser",
        registered_on=_SAFE_TIME,
    ),
    input_file_count=1,
    state=models.JobState.DOWNLOAD_SUBMITTED,
    transition_times=[
        models.AdminJobStateTransition(
            state=models.JobState.CREATED,
            time=_SAFE_TIME,
            trans_id="trans1",
            notif_sent=False,
        ),
        models.AdminJobStateTransition(
            state=models.JobState.DOWNLOAD_SUBMITTED,
            time=_SAFE_TIME,
            trans_id="trans2",
            notif_sent=False,
        ),
    ]
)

_BASESUBJOB1 = models.SubJob(
    id="bar",
    sub_id=0,
    state=models.JobState.CREATED,
    transition_times=[models.JobStateTransition(state=models.JobState.CREATED,time=_SAFE_TIME)]
)
_BASESUBJOB2 = _BASESUBJOB1.model_copy(deep=True)
_BASESUBJOB2.sub_id = 1
_BASESUBJOB3 = _BASESUBJOB1.model_copy(deep=True)
_BASESUBJOB3.sub_id = 2


async def test_indexes(mongo, mondb):
    mongo.clear_database(MONGO_TEST_DB, drop_indexes=True)
    await MongoDAO.create(mondb)
    cols = mongo.client[MONGO_TEST_DB].list_collection_names()
    assert set(cols) == {"jobs", "refdata", "images", "sites", "subjobs", "exitcodes"}
    siteindex = mongo.client[MONGO_TEST_DB]["sites"].index_information()
    assert siteindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        "site_1": {"v": 2, "key": [("site", 1)], "unique": True}
    }
    jobindex = mongo.client[MONGO_TEST_DB]["jobs"].index_information()
    assert jobindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        "id_1": {"v": 2, "key": [("id", 1)], "unique": True},
        "_update_time_-1": {"key": [("_update_time", -1)], "v": 2},
        "user_1__update_time_-1": {"key": [("user", 1), ("_update_time", -1)], "v": 2},
        "job_input.cluster_1__update_time_-1": {
            "v": 2,
            "key": [("job_input.cluster", 1), ("_update_time", -1)]
        },
        "state_1__update_time_-1": {"v": 2, "key": [("state", 1), ("_update_time", -1)]},
        "user_1_state_1__update_time_-1": {
            "v": 2,
            "key": [("user", 1), ("state", 1), ("_update_time", -1)]
        },
        "user_1_job_input.cluster_1__update_time_-1": {
            "v": 2,
            "key": [("user", 1), ("job_input.cluster", 1), ("_update_time", -1)]
        },
        "transition_times.time_-1": {
            "v": 2,
            "key": [("transition_times.time", -1)],
            "partialFilterExpression": SON([("transition_times.notif_sent", False)])
        },
        "cleaned_1_state_1__update_time_1": {
            "v": 2,
            "key": [("cleaned", 1), ( "state", 1), ( "_update_time", 1)],
            "partialFilterExpression": SON(
                [("cleaned", False), ("state", SON([("$in", ["canceled", "complete", "error"])]))]
            ),
        },
    }
    subjobindex = mongo.client[MONGO_TEST_DB]["subjobs"].index_information()
    assert subjobindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        "id_1_sub_id_1": {"key": [("id", 1), ("sub_id", 1)], "unique": True, "v": 2},
        "id_1_transition_times.state_1_transition_times._retry_1": {
            "key": [("id", 1), ("transition_times.state", 1), ("transition_times._retry", 1)],
            "v": 2
        },
        "state_1_heartbeat_1": {
            "v": 2,
            "key": [("state", 1), ("heartbeat", 1)],
            "partialFilterExpression": SON([
                ("state", SON([("$in", [
                    "created", "download_submitted", "error_processing_submitted",
                    "error_processing_submitting", "job_submitted", "job_submitting",
                    "upload_submitted", "upload_submitting",
                ])]))
            ]),
        },
    }
    ecindex = mongo.client[MONGO_TEST_DB]["exitcodes"].index_information()
    assert ecindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        "id_1": {"key": [("id", 1)], "unique": True, "v": 2},
    }
    refindex = mongo.client[MONGO_TEST_DB]["refdata"].index_information()
    assert refindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        "id_1": {"v": 2, "key": [("id", 1)], "unique": True},
        "file_1": {"v": 2, "key": [("file", 1)]},
        "statuses.cleaned_1_statuses.state_1_statuses._update_time_1": {
            "v": 2,
            "key": [("statuses.cleaned", 1), ("statuses.state", 1), ("statuses._update_time", 1)],
            "partialFilterExpression": SON([
                ("statuses.cleaned", False),
                ("statuses.state", SON([("$in", ["complete", "error"])]))
            ]),
        },
    }
    imgindex = mongo.client[MONGO_TEST_DB]["images"].index_information()
    assert imgindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        "UNIQUE_IMAGE_DIGEST_INDEX": {
            "v": 2,
            "key": [("name", 1), ("digest", 1)],
            "unique": True
        },
        "UNIQUE_IMAGE_TAG_INDEX": {
            "v": 2,
            "key": [("name", 1), ("tag", 1)],
            "unique": True,
            "sparse": True
        }
    }


async def test_job_basic_roundtrip(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)

    got = await mc.get_job("foo", as_admin=True)
    assert got == _BASEJOB


async def test_set_job_clean(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)
    
    got = await mc.get_job("foo", as_admin=True)
    assert got.cleaned is False
    
    await mc.set_job_clean("foo")
    expected = _BASEJOB.model_copy(deep=True)
    expected.cleaned = True
    
    got = await mc.get_job("foo", as_admin=True)
    assert got == expected


async def test_set_job_clean_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)
    
    await _set_job_clean_fail(mc, None, ValueError("job_id is required"))
    await _set_job_clean_fail(mc, "   \t   ", ValueError("job_id is required"))
    await _set_job_clean_fail(mc, "whoop", NoSuchJobError("No job with ID 'whoop' exists"))


async def _set_job_clean_fail(mc, job_id, expected):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.set_job_clean(job_id)


async def test_process_dirty_jobs(mondb):
    mc = await MongoDAO.create(mondb)
    current = datetime.datetime(
        year=2026, month=2, day=10, hour=14, minute=30, second=54, tzinfo=datetime.UTC
    )
    older_than = current - datetime.timedelta(days=30)  # much newer than _SAFE_TIME
    
    # Shouldn't be found due to non-terminal states
    for state in set(models.JobState) - models.JobState.terminal_states():
        running_state = _BASEJOB.model_copy(deep=True)
        running_state.id = state.value
        running_state.state = state  # don't worry about transition_times
        await mc.save_job(running_state)
    
    cleaned = _BASEJOB.model_copy(deep=True)
    cleaned.id = "cleaned"
    cleaned.state = models.JobState.COMPLETE
    cleaned.cleaned = True  # shouldn't be found due to cleaned state
    await mc.save_job(cleaned)
    
    new = _BASEJOB.model_copy(deep=True)
    new.id = "new"
    new.state = models.JobState.ERROR
    new.transition_times.append(models.AdminJobStateTransition(
        state=models.JobState.ERROR,
        time=older_than,  # shouldn't be found due to date = older_than
        trans_id="trans1",
        notif_sent=False,
    ))
    await mc.save_job(new)
    
    # save jobs expected to be found, 1 per terminal state
    found_comp = _BASEJOB.model_copy(deep=True)
    found_comp.id = "found_comp"
    found_comp.state = models.JobState.COMPLETE
    found_comp.transition_times.append(models.AdminJobStateTransition(
        state=models.JobState.COMPLETE,
        time=older_than - datetime.timedelta(seconds=1),
        trans_id="trans1",
        notif_sent=False,
    ))
    await mc.save_job(found_comp)
    
    found_err = _BASEJOB.model_copy(deep=True)
    found_err.id = "found_err"
    found_err.state = models.JobState.ERROR
    await mc.save_job(found_err)
    
    found_cncl = _BASEJOB.model_copy(deep=True)
    found_cncl.id = "found_cncl"
    found_cncl.state = models.JobState.CANCELED
    await mc.save_job(found_cncl)
    
    found = {}
    async def collect(job):
        found[job.id] = job
    await mc.process_dirty_jobs(older_than, collect)
    
    # debugging help
    assert found.keys() == {"found_cncl", "found_err", "found_comp"}
    assert found == {"found_cncl": found_cncl, "found_err": found_err, "found_comp": found_comp}
    
    # test noop
    found.clear()
    await mc.process_dirty_jobs(_SAFE_TIME, collect)
    assert found.keys() == set() 


async def test_process_dirty_jobs_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await _process_dirty_jobs_fail(
        mc, None, lambda: print("foo"), ValueError("older_than is required")
    )
    await _process_dirty_jobs_fail(mc, _SAFE_TIME, None, ValueError("operation is required"))


async def _process_dirty_jobs_fail(mc, older_than, op, expected):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.process_dirty_jobs(older_than, op)


async def test_exit_codes_for_standard_job_roundtrip(mondb):
    mc = await MongoDAO.create(mondb)
    
    await mc.save_exit_codes_for_standard_job("foo", [1, 2, 3, 0])
    await mc.save_exit_codes_for_standard_job("bar", [0])
    
    assert await mc.get_exit_codes_for_standard_job("foo") == [1, 2, 3, 0]
    assert await mc.get_exit_codes_for_standard_job("bar") == [0]
    assert await mc.get_exit_codes_for_standard_job("baz") is None


async def test_exit_codes_for_standard_job_upsert(mondb):
    mc = await MongoDAO.create(mondb)
    
    await mc.save_exit_codes_for_standard_job("foo", [1, 2, 3, 0])
    assert await mc.get_exit_codes_for_standard_job("foo") == [1, 2, 3, 0]
    
    await mc.save_exit_codes_for_standard_job("foo", [0])
    assert await mc.get_exit_codes_for_standard_job("foo") == [0]


async def test_save_exit_codes_for_standard_job_fail(mondb):
    mc = await MongoDAO.create(mondb)
    
    await save_exit_codes_for_standard_job_fail(mc, None, [0], ValueError("job_id is required"))
    await save_exit_codes_for_standard_job_fail(mc, "  \t   ", [0], ValueError(
        "job_id is required"
    ))
    await save_exit_codes_for_standard_job_fail(mc, "f", None, ValueError(
        "exit_codes is required"
    ))
    await save_exit_codes_for_standard_job_fail(mc, "f", [], ValueError("exit_codes is required"))
    

async def save_exit_codes_for_standard_job_fail(mc, job_id, exit_codes, expected):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.save_exit_codes_for_standard_job(job_id, exit_codes)


async def test_update_job(mondb):
    # tests updates that change standard and array fields as well as switching to error
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)

    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    dt2 = dt + datetime.timedelta(minutes=1)
    await mc.update_job_state("foo", submitting_job(), dt, "tid1")  # no fields
    await mc.update_job_state("foo", submitted_jaws_job("123"), dt, "tid2")  # array field
    await mc.update_job_state("foo", submitting_upload(cpu_hours=1.2), dt2, "tid3")  # std field
    await mc.update_job_state("foo", error("adminerr", user_error="usererr"), dt2, "tid4")
    got = await mc.get_job("foo", as_admin=True)
    
    # check expected job structure
    expected = _BASEJOB.model_copy(deep=True)
    expected.state = models.JobState.ERROR
    expected.cpu_hours = 1.2
    expected.jaws_details = models.JAWSDetails(run_id = ["123"])
    expected.error = "usererr"
    expected.admin_error = "adminerr"
    expected.transition_times.extend([
        models.AdminJobStateTransition(
            state=models.JobState.JOB_SUBMITTING,
            time=dt,
            trans_id="tid1",
            notif_sent=False,
        ),
        models.AdminJobStateTransition(
            state=models.JobState.JOB_SUBMITTED,
            time=dt,
            trans_id="tid2",
            notif_sent=False,
        ),
        models.AdminJobStateTransition(
            state=models.JobState.UPLOAD_SUBMITTING,
            time=dt2,
            trans_id="tid3",
            notif_sent=False,
        ),
        models.AdminJobStateTransition(
            state=models.JobState.ERROR,
            time=dt2,
            trans_id="tid4",
            notif_sent=False,
        ),
    ])
    assert got == expected


async def test_update_job_htcondor_stats(mondb):
    """
    HTCondor stats fields are written to htcondor_details subdocument and read back correctly.
    """
    mc = await MongoDAO.create(mondb)
    # HTCondorDetails.cluster_id is required, so initialise it before setting the stats fields
    job = _BASEJOB.model_copy(deep=True)
    job.htcondor_details = models.HTCondorDetails(cluster_id=[42])
    await mc.save_job(job)

    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    await mc.update_job_state(
        "foo",
        error(
            "admin err",
            htcondor_cpu_hours=1.5,
            htcondor_max_memory=536870912,
            htcondor_runtime_seconds=1800.0,
        ),
        dt, "tid1",
    )
    got = await mc.get_job("foo", as_admin=True)
    assert got.htcondor_details == models.HTCondorDetails(
        cluster_id=[42],
        cpu_hours=1.5,
        max_memory=536870912,
        runtime_seconds=1800.0,
    )


async def test_update_job_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)
    initial_doc = await mondb.jobs.find_one({"id": "foo"})
    
    u = submitting_job()
    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    tid = "1"
    await fail_update_job(mc, None, u, dt, tid, ValueError("job_id is required"))
    await fail_update_job(mc, "   \t  ", u, dt, tid, ValueError("job_id is required"))
    await fail_update_job(mc, "foo", None, dt, tid, ValueError("update is required"))
    await fail_update_job(mc, "foo", u, None, tid, ValueError("time is required"))
    await fail_update_job(mc, "foo", u, dt, None, ValueError("trans_id is required"))
    await fail_update_job(mc, "foo", u, dt, "   \t    ", ValueError("trans_id is required"))
    await fail_update_job(mc, "bar", u, dt, tid, NoSuchJobError(
        "No job with ID 'bar' exists"
    ))
    await fail_update_job(mc, "foo", submitting_upload(), dt, tid, InvalidJobStateError(
        "Job 'foo' is in state 'download_submitted', expected 'job_submitted'"
    ), expected_actual_state=models.JobState.DOWNLOAD_SUBMITTED)
    await fail_update_job(mc, "foo", u, _SAFE_TIME + datetime.timedelta(milliseconds=-1), tid,
        ValueError("Job 'foo' last update time is after the provided time")
    )
    assert await mondb.jobs.find_one({"id": "foo"}) == initial_doc


async def test_update_job_and_subjob_fail_update_to_error(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)
    await mc.initialize_subjobs([_BASESUBJOB1])
    u = error("admin error here")
    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    tid = "1"
    
    for state in (
        models.JobState.terminal_states() |
        models.JobState.canceling_states() |
        {models.JobState.RECOVERING}
    ):
        await mondb.jobs.update_one({}, {"$set": {"state": state.value}})
        await mondb.subjobs.update_one({}, {"$set": {"state": state.value}})
        expected_job_doc = await mondb.jobs.find_one({"id": "foo"})
        expected_subjob_doc = await mondb.subjobs.find_one(
            {"id": "bar", "sub_id": 0}
        )
        await fail_update_job(mc, "foo", u, dt, tid, InvalidJobStateError(
            f"Job 'foo' is in disallowed state {state.value!r}"
        ), expected_actual_state=state)
        assert await mondb.jobs.find_one({"id": "foo"}) == expected_job_doc
        await fail_update_subjob(mc, "bar", 0, u, dt, InvalidJobStateError(
            f"Job 'bar' with subjob ID 0 is in disallowed state {state.value!r}"
        ), expected_actual_state=state)
        assert await mondb.subjobs.find_one(
            {"id": "bar", "sub_id": 0}
        ) == expected_subjob_doc


async def fail_update_job(mc, job_id, update, dt, tid, expected, expected_actual_state=None):
    with pytest.raises(type(expected), match=f"^{re.escape(expected.args[0])}$") as exc_info:
        await mc.update_job_state(job_id, update, dt, tid)
    if isinstance(expected, InvalidJobStateError):
        assert exc_info.value.actual_state == expected_actual_state


def check_job_retry_fields(jobdoc: dict[str, Any]):
    assert jobdoc["_retry"] == 0
    check_trans_retry_fields(jobdoc)

def check_trans_retry_fields(jobdoc: dict[str, Any]):
    for tt in jobdoc["transition_times"]:
        assert tt["_retry"] == 0


async def test_job_hidden_fields(mondb):
    # Tests that internal fields are set correctly when performing actions
    # on a job. Does not test other job saving / updating code.
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)

    # check that the internal fields are set correctly
    job = await mondb.jobs.find_one({"id": "foo"})
    assert job["_update_time"] == _SAFE_TIME
    check_job_retry_fields(job)

    # check that updating job state sets internal fields
    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    await mc.update_job_state("foo", submitting_job(), dt, "tid")
    got = await mc.get_job("foo", as_admin=True)
    
    # check expected job structure
    expected = _BASEJOB.model_copy(deep=True)
    expected.state = models.JobState.JOB_SUBMITTING
    expected.transition_times.append(models.AdminJobStateTransition(
        state=models.JobState.JOB_SUBMITTING,
        time=dt,
        trans_id="tid",
        notif_sent=False,
    ))
    assert got == expected
    
    # check that the internal fields are set correctly
    job = await mondb.jobs.find_one({"id": "foo"})
    assert job["_update_time"] == dt
    check_job_retry_fields(job)


async def test_update_job_recovery_cooldown(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)

    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    dt2 = dt + datetime.timedelta(minutes=11)
    dt3 = dt2 + datetime.timedelta(minutes=1)

    def _tt(state, time, trans_id):
        return models.AdminJobStateTransition(
            state=state, time=time, trans_id=trans_id,notif_sent=False
        )
    R = models.JobState.RECOVERING

    # recovery_cooldown=timedelta(0): transitions to RECOVERING, sets _last_recover
    await mc.update_job_state(
        "foo", recovering(), dt, "tid1", recovery_cooldown=datetime.timedelta(0)
    )
    got = await mc.get_job("foo", as_admin=True)
    expected = _BASEJOB.model_copy(deep=True)
    expected.state = R
    expected.transition_times.append(_tt(R, dt, "tid1"))
    assert got == expected
    raw = await mondb.jobs.find_one({"id": "foo"})
    assert raw["_last_recover"] == dt

    # recovery_cooldown with positive timedelta: RECOVERING->RECOVERING, updates _last_recover
    await mc.update_job_state(
        "foo", force_recovering(), dt2, "tid2",
        recovery_cooldown=datetime.timedelta(minutes=10),
    )
    got = await mc.get_job("foo", as_admin=True)
    expected = expected.model_copy(deep=True)
    expected.transition_times.append(_tt(R, dt2, "tid2"))
    assert got == expected
    raw = await mondb.jobs.find_one({"id": "foo"})
    assert raw["_last_recover"] == dt2

    # recovery_cooldown=None (default): RECOVERING->RECOVERING, does not write _last_recover
    await mc.update_job_state("foo", force_recovering(), dt3, "tid3")
    got = await mc.get_job("foo", as_admin=True)
    expected = expected.model_copy(deep=True)
    expected.transition_times.append(_tt(R, dt3, "tid3"))
    assert got == expected
    raw = await mondb.jobs.find_one({"id": "foo"})
    assert raw["_last_recover"] == dt2  # unchanged


async def test_update_job_force_recovery_no_prior_recover(mondb):
    # A positive cooldown on a job with no _last_recover should succeed: exercises the
    # $exists: False branch in the $or cooldown query
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)

    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    await mc.update_job_state(
        "foo", recovering(), dt, "tid1",
        recovery_cooldown=datetime.timedelta(minutes=10),
    )
    got = await mc.get_job("foo", as_admin=True)
    expected = _BASEJOB.model_copy(deep=True)
    expected.state = models.JobState.RECOVERING
    expected.transition_times.append(models.AdminJobStateTransition(
        state=models.JobState.RECOVERING, time=dt, trans_id="tid1", notif_sent=False,
    ))
    assert got == expected
    raw = await mondb.jobs.find_one({"id": "foo"})
    assert raw["_last_recover"] == dt


async def test_update_job_recovery_cooldown_state_mismatch(mondb):
    # A positive cooldown on a job in the wrong state should raise InvalidJobStateError, not
    # JobRecoveryError — the cooldown diagnostic query must not mask a state mismatch
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)  # state = DOWNLOAD_SUBMITTED

    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    initial_doc = await mondb.jobs.find_one({"id": "foo"})
    # force_recovering requires current state = RECOVERING, but job is DOWNLOAD_SUBMITTED
    with pytest.raises(InvalidJobStateError, match=(
        r"^Job 'foo' is in state 'download_submitted', expected 'recovering'$"
    )) as exc_info:
        await mc.update_job_state(
            "foo", force_recovering(), dt, "tid1",
            recovery_cooldown=datetime.timedelta(minutes=10),
        )
    assert exc_info.value.actual_state == models.JobState.DOWNLOAD_SUBMITTED
    assert await mondb.jobs.find_one({"id": "foo"}) == initial_doc


async def test_update_job_recovery_cooldown_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)

    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    # first recovery sets _last_recover
    await mc.update_job_state(
        "foo", recovering(), dt, "tid1", recovery_cooldown=datetime.timedelta(0)
    )

    # force recovery within the cooldown window should fail; remaining = 10min - 5min = 5min
    dt2 = dt + datetime.timedelta(minutes=4)
    expected_doc = await mondb.jobs.find_one({"id": "foo"})
    with pytest.raises(JobRecoveryError, match=(
        r"^Job 'foo' was recovered too recently; "
        r"must wait 0:06:00 more before forcing recovery again$"
    )):
        await mc.update_job_state(
            "foo", force_recovering(), dt2, "tid2",
            recovery_cooldown=datetime.timedelta(minutes=10),
        )
    assert await mondb.jobs.find_one({"id": "foo"}) == expected_doc

    # force recovery after the cooldown window should succeed
    dt3 = dt + datetime.timedelta(minutes=11)
    await mc.update_job_state(
        "foo", force_recovering(), dt3, "tid3",
        recovery_cooldown=datetime.timedelta(minutes=10),
    )
    job = await mondb.jobs.find_one({"id": "foo"})
    assert job["_last_recover"] == dt3


async def test_update_job_last_update_time_conflict(mondb):
    """
    When last_update_time is provided, the write is gated on the job's _update_time matching
    that value (optimistic lock). If another write has occurred since the caller read _update_time,
    JobUpdateConflictError is raised and the job is unchanged. If the value matches, the write
    proceeds normally.

    JobUpdateConflictError is raised even when the concurrent write's _update_time is after the
    provided time — conflict takes priority over the time ordering error.
    """
    mc = await MongoDAO.create(mondb)
    dt1 = _SAFE_TIME + datetime.timedelta(minutes=1)
    dt2 = dt1 + datetime.timedelta(minutes=1)

    await mc.save_job(_BASEJOB)
    # Advance the job to JOB_SUBMITTING; _update_time becomes dt1
    # initial _update_time is _SAFE_TIME — set by save_job from the last transition time
    await mc.update_job_state("foo", submitting_job(), dt1, "tid1")

    conflict_match = re.escape(
        "Job 'foo' update time 2025-03-31 12:01:00.345000+00:00"
        " does not match expected 2025-03-31 12:00:00.345000+00:00"
    )

    # Stale lock (_update_time in DB is dt1, not _SAFE_TIME); provided time dt2 > dt1.
    with pytest.raises(JobUpdateConflictError, match=conflict_match):
        await mc.update_job_state("foo", error("conflict test"), dt2, "tid2",
                                  last_update_time=_SAFE_TIME)

    # Stale lock where the concurrent write's _update_time (dt1) is also after our time
    # (dt0 < dt1). Should raise conflict error, not value error.
    dt0 = _SAFE_TIME - datetime.timedelta(minutes=1)
    with pytest.raises(JobUpdateConflictError, match=conflict_match):
        await mc.update_job_state("foo", error("conflict test"), dt0, "tid3",
                                  last_update_time=_SAFE_TIME)

    # Job is still JOB_SUBMITTING (both lock-gated writes were rejected)
    expected = _BASEJOB.model_copy(deep=True)
    expected.state = models.JobState.JOB_SUBMITTING
    expected.transition_times.append(
        models.AdminJobStateTransition(
            state=models.JobState.JOB_SUBMITTING, time=dt1, trans_id="tid1", notif_sent=False
        )
    )
    assert await mc.get_job("foo", as_admin=True) == expected

    # Writing with the current _update_time (dt1) succeeds
    await mc.update_job_state("foo", error("accepted"), dt2, "tid4", last_update_time=dt1)
    expected.state = models.JobState.ERROR
    expected.admin_error = "accepted"
    expected.transition_times.append(
        models.AdminJobStateTransition(
            state=models.JobState.ERROR, time=dt2, trans_id="tid4", notif_sent=False
        )
    )
    assert await mc.get_job("foo", as_admin=True) == expected


async def test_recover_job(mondb):
    mc = await MongoDAO.create(mondb)

    # save a job with extra transitions and error fields to verify clearing and history capture
    job = _BASEJOB.model_copy(deep=True)
    job.cleaned = True
    job.error = "some error"
    job.admin_error = "some admin error"
    job.traceback = "some traceback"
    dt1 = _SAFE_TIME + datetime.timedelta(minutes=1)
    dt2 = dt1 + datetime.timedelta(minutes=1)
    job.transition_times.extend([
        models.AdminJobStateTransition(
            state=models.JobState.JOB_SUBMITTING, time=dt1, trans_id="t3", notif_sent=True
        ),
        models.AdminJobStateTransition(
            state=models.JobState.RECOVERING, time=dt2, trans_id="t4", notif_sent=False
        ),
    ])
    job.state = models.JobState.RECOVERING
    await mc.save_job(job)

    ds_time = dt2 + datetime.timedelta(seconds=1)
    await mc.recover_job("foo", ds_time, "t5")

    def _tt(state, time, trans_id, notif_sent):
        return models.AdminJobStateTransition(
            state=state, time=time, trans_id=trans_id, notif_sent=notif_sent
        )

    got = await mc.get_job("foo", as_admin=True)
    expected = _BASEJOB.model_copy(deep=True)
    expected.state = models.JobState.DOWNLOAD_SUBMITTED
    expected.cleaned = False
    expected.transition_times = [
        _tt(models.JobState.DOWNLOAD_SUBMITTED, ds_time, "t5", False),
    ]
    expected.trans_history = [
        _tt(models.JobState.CREATED, _SAFE_TIME, "trans1", False),
        _tt(models.JobState.DOWNLOAD_SUBMITTED, _SAFE_TIME, "trans2", False),
        _tt(models.JobState.JOB_SUBMITTING, dt1, "t3", True),
        _tt(models.JobState.RECOVERING, dt2, "t4", False),
    ]
    expected.admin_error_history = ["some admin error"]
    assert got == expected

    raw = await mondb.jobs.find_one({"id": "foo"})
    assert raw["_update_time"] == ds_time
    assert "error" not in raw
    assert "admin_error" not in raw
    assert "traceback" not in raw


async def test_recover_job_second_recovery(mondb):
    # Verify trans_history and admin_error_history accumulate across multiple recoveries,
    # and that RECOVERING transitions are preserved in the history.
    mc = await MongoDAO.create(mondb)

    # Save with a RECOVERING entry already in transition_times to match expected caller behavior.
    rec_time0 = _SAFE_TIME + datetime.timedelta(minutes=1)
    job = _BASEJOB.model_copy(deep=True)
    job.admin_error = "first error"
    job.transition_times.append(
        models.AdminJobStateTransition(
            state=models.JobState.RECOVERING, time=rec_time0, trans_id="r0", notif_sent=False
        )
    )
    job.state = models.JobState.RECOVERING
    await mc.save_job(job)

    ds_time1 = rec_time0 + datetime.timedelta(seconds=1)
    await mc.recover_job("foo", ds_time1, "r1")

    t_err = ds_time1 + datetime.timedelta(minutes=5)
    await mc.update_job_state("foo", error("second error"), t_err, "e1")

    # Transition to RECOVERING before the second recover_job call, as callers are expected to do.
    t_rec2 = t_err + datetime.timedelta(minutes=1)
    await mc.update_job_state("foo", recovering(), t_rec2, "r0_2")

    ds_time2 = t_rec2 + datetime.timedelta(seconds=1)
    await mc.recover_job("foo", ds_time2, "r2")

    def _tt(state, time, trans_id, notif_sent):
        return models.AdminJobStateTransition(
            state=state, time=time, trans_id=trans_id, notif_sent=notif_sent
        )

    got = await mc.get_job("foo", as_admin=True)
    expected = _BASEJOB.model_copy(deep=True)
    expected.state = models.JobState.DOWNLOAD_SUBMITTED
    expected.transition_times = [_tt(models.JobState.DOWNLOAD_SUBMITTED, ds_time2, "r2", False)]
    expected.trans_history = [
        _tt(models.JobState.CREATED, _SAFE_TIME, "trans1", False),
        _tt(models.JobState.DOWNLOAD_SUBMITTED, _SAFE_TIME, "trans2", False),
        _tt(models.JobState.RECOVERING, rec_time0, "r0", False),
        # 1st recover call happens here
        _tt(models.JobState.DOWNLOAD_SUBMITTED, ds_time1, "r1", False),
        _tt(models.JobState.ERROR, t_err, "e1", False),
        _tt(models.JobState.RECOVERING, t_rec2, "r0_2", False),
        # 2nd recovery call happens here
    ]
    expected.admin_error_history = ["first error", "second error"]
    assert got == expected


async def test_recover_job_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)

    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)

    await _fail_recover_job(mc, None, dt, "t1", ValueError("job_id is required"))
    await _fail_recover_job(mc, "   \t  ", dt, "t1", ValueError("job_id is required"))
    await _fail_recover_job(mc, "foo", None, "t1", ValueError(
        "download_submitted_time is required")
    )
    await _fail_recover_job(mc, "foo", dt, None, ValueError("trans_id is required"))
    await _fail_recover_job(mc, "foo", dt, "   \t  ", ValueError("trans_id is required"))
    await _fail_recover_job(mc, "nosuchthing", dt, "t1",
        NoSuchJobError("No job with ID 'nosuchthing' exists"))


async def _fail_recover_job(mc, job_id, ds_time, trans_id, expected):
    with pytest.raises(type(expected), match=f"^{re.escape(expected.args[0])}$"):
        await mc.recover_job(job_id, ds_time, trans_id)


async def test_subjob_basic_roundtrip(mondb):
    mc = await MongoDAO.create(mondb)
    
    sjs = [_BASESUBJOB1, _BASESUBJOB2, _BASESUBJOB3]
    await mc.initialize_subjobs(sjs)
    
    for i in range(3):
        sj = await mc.get_subjob("bar", i)
        assert sj == sjs[i]
    
    sjs_got = await mc.get_subjobs("bar")
    assert sjs_got == sjs 


async def test_initialize_subjobs_fail_bad_args(mondb):
    mc = await MongoDAO.create(mondb)
    
    await initialize_subjobs_fail(mc, None, ValueError("subjobs is required"))
    await initialize_subjobs_fail(
        mc, [_BASESUBJOB1, None, _BASESUBJOB2], ValueError("subjob is required")
    )


async def initialize_subjobs_fail(mc, subjobs, expected):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.initialize_subjobs(subjobs)


async def test_initialize_subjobs_fail_duplicate_ids(mondb):
    # for now just throw a mongo error, this indicates a programming issue
    mc = await MongoDAO.create(mondb)
    bsj2 = _BASESUBJOB2.model_copy(deep=True)
    bsj2.sub_id = 0
    
    sjs = [_BASESUBJOB1, bsj2, _BASESUBJOB3]
    err = (
        "E11000 duplicate key error collection: testing.subjobs index: id_1_sub_id_1 dup key: "
        + "{ id: \"bar\", sub_id: 0"
    )
    with pytest.raises(BulkWriteError, match=err):
        await mc.initialize_subjobs(sjs)


async def test_get_subjob_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.initialize_subjobs([_BASESUBJOB2])
    
    await get_subjob_fail(mc, None, 1, ValueError("job_id is required"))
    await get_subjob_fail(mc, "   \t   ", 1, ValueError("job_id is required"))
    await get_subjob_fail(mc, "bar", None, ValueError("subjob_id is required"))
    await get_subjob_fail(mc, "bar", -1, ValueError("subjob_id must be >= 0"))
    await get_subjob_fail(mc, "foo", 1, NoSuchSubJobError(
        "No sub job with job ID 'foo' and sub job ID 1 exists"
    ))
    await get_subjob_fail(mc, "bar", 0, NoSuchSubJobError(
        "No sub job with job ID 'bar' and sub job ID 0 exists"
    ))
    await get_subjob_fail(mc, "bar", 2, NoSuchSubJobError(
        "No sub job with job ID 'bar' and sub job ID 2 exists"
    ))


async def get_subjob_fail(mc, job_id, subjob_id, expected):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.get_subjob(job_id, subjob_id)


async def test_get_subjobs_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.initialize_subjobs([_BASESUBJOB2])
    
    await get_subjobs_fail(mc, None, ValueError("job_id is required"))
    await get_subjobs_fail(mc, "   \t   ", ValueError("job_id is required"))
    await get_subjobs_fail(mc, "foo", NoSuchSubJobError("No sub jobs found for job ID 'foo'"))


async def get_subjobs_fail(mc, job_id, expected):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.get_subjobs(job_id)


async def test_get_exit_codes_for_subjobs(mondb):
    mc = await MongoDAO.create(mondb)
    sj1 = _BASESUBJOB1.model_copy()
    sj1.exit_code = 3
    sj2 = _BASESUBJOB2.model_copy()
    sj2.exit_code = 0
    await mc.initialize_subjobs([_BASESUBJOB3, sj1, sj2])
    
    assert await mc.get_exit_codes_for_subjobs("bar") == [3, 0, None]


async def test_get_exit_codes_for_subjobs_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.initialize_subjobs([_BASESUBJOB2])
    
    await get_exit_codes_for_subjobs_fail(mc, None, ValueError("job_id is required"))
    await get_exit_codes_for_subjobs_fail(mc, "   \t   ", ValueError("job_id is required"))
    await get_exit_codes_for_subjobs_fail(mc, "foo", NoSuchSubJobError(
        "No sub jobs found for job ID 'foo'"
    ))


async def get_exit_codes_for_subjobs_fail(mc, job_id, expected):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.get_exit_codes_for_subjobs(job_id)


async def test_update_subjob(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.initialize_subjobs([_BASESUBJOB1])

    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    dt2 = dt + datetime.timedelta(minutes=1)
    await mc.update_subjob_state("bar", 0, submitted_download(), dt)
    await mc.update_subjob_state("bar", 0, submitting_job(), dt)
    await mc.update_subjob_state("bar", 0, error("adminerr"), dt2)
    got = await mc.get_subjob("bar", 0)
    
    # check expected job structure
    expected = _BASESUBJOB1.model_copy(deep=True)
    expected.state = models.JobState.ERROR
    expected.admin_error = "adminerr"
    expected.transition_times.extend([
        models.JobStateTransition(state=models.JobState.DOWNLOAD_SUBMITTED, time=dt),
        models.JobStateTransition(state=models.JobState.JOB_SUBMITTING, time=dt),
        models.JobStateTransition(state=models.JobState.ERROR, time=dt2),
    ])
    assert got == expected


async def test_update_subjob_container_stats(mondb):
    """Container stats (exit_code, cpu_hours, max_memory, runtime_seconds) stored in subjob."""
    mc = await MongoDAO.create(mondb)
    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    dt2 = dt + datetime.timedelta(minutes=1)
    sj = models.SubJob(
        id="bar",
        sub_id=0,
        state=models.JobState.JOB_SUBMITTED,
        transition_times=[models.JobStateTransition(state=models.JobState.JOB_SUBMITTED, time=dt)],
    )
    await mc.initialize_subjobs([sj])
    await mc.update_subjob_state(
        "bar", 0,
        submitting_upload_with_exit_code(
            0, cpu_hours=1.5, max_memory=536870912, runtime_seconds=1800.0
        ),
        dt2,
    )

    expected = sj.model_copy(deep=True)
    expected.state = models.JobState.UPLOAD_SUBMITTING
    expected.exit_code = 0
    expected.cpu_hours = 1.5
    expected.max_memory = 536870912
    expected.runtime_seconds = 1800.0
    expected.transition_times.append(
        models.JobStateTransition(state=models.JobState.UPLOAD_SUBMITTING, time=dt2)
    )
    assert await mc.get_subjob("bar", 0) == expected


async def test_update_subjob_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.initialize_subjobs([_BASESUBJOB1])
    initial_doc = await mondb.subjobs.find_one({"id": "bar", "sub_id": 0})
    
    u = submitted_download()
    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    await fail_update_subjob(mc, None, 0, u, dt, ValueError("job_id is required"))
    await fail_update_subjob(mc, "   \t  ", 0, u, dt, ValueError("job_id is required"))
    await fail_update_subjob(mc, "bar", None, u, dt, ValueError("subjob_id is required"))
    await fail_update_subjob(mc, "bar", -1, u, dt, ValueError("subjob_id must be >= 0"))
    await fail_update_subjob(mc, "bar", 0, None, dt, ValueError("update is required"))
    await fail_update_subjob(mc, "bar", 0, u, None, ValueError("time is required"))
    await fail_update_subjob(mc, "foo", 0, u, dt, NoSuchSubJobError(
        "No job with ID 'foo' and subjob ID 0 exists"
    ))
    await fail_update_subjob(mc, "bar", 1, u, dt, NoSuchSubJobError(
        "No job with ID 'bar' and subjob ID 1 exists"
    ))
    await fail_update_subjob(mc, "bar", 0, submitting_upload(), dt, InvalidJobStateError(
        "Job 'bar' with subjob ID 0 is in state 'created', expected 'job_submitted'"
    ), expected_actual_state=models.JobState.CREATED)
    await fail_update_subjob(mc, "bar", 0, u, _SAFE_TIME + datetime.timedelta(milliseconds=-1),
        ValueError("Job 'bar' with subjob ID 0 last update time is after the provided time")
    )
    await fail_update_subjob(mc, "bar", 0, u, dt,
        ValueError("last_update_time must be a timezone aware datetime"),
        last_update_time=datetime.datetime(2025, 4, 2, 12, 0, 0),
    )
    assert await mondb.subjobs.find_one({"id": "bar", "sub_id": 0}) == initial_doc


async def fail_update_subjob(
    mc, job_id, subjob_id, update, dt, expected, expected_actual_state=None, last_update_time=None
):
    with pytest.raises(type(expected), match=f"^{re.escape(expected.args[0])}$") as exc_info:
        await mc.update_subjob_state(
            job_id, subjob_id, update, dt, last_update_time=last_update_time
        )
    if isinstance(expected, InvalidJobStateError):
        assert exc_info.value.actual_state == expected_actual_state


async def test_update_subjob_last_update_time_conflict(mondb):
    """
    When last_update_time is provided, the write is gated on the subjob's _update_time matching
    that value (optimistic lock). If another write has occurred since the caller read _update_time,
    SubJobUpdateConflictError is raised and the subjob is unchanged. If the value matches,
    the write proceeds normally.

    SubJobUpdateConflictError is raised even when the concurrent write's _update_time is after
    the provided time — i.e. the conflict error takes priority over the time ordering error.
    """
    mc = await MongoDAO.create(mondb)
    dt1 = _SAFE_TIME + datetime.timedelta(minutes=1)
    dt2 = dt1 + datetime.timedelta(minutes=1)

    await mc.initialize_subjobs([_BASESUBJOB1])

    # Advance the subjob to DOWNLOAD_SUBMITTED; _update_time becomes dt1
    # initial _update_time is _SAFE_TIME — set by initialize_subjobs from the last transition time
    await mc.update_subjob_state("bar", 0, submitted_download(), dt1)

    conflict_match = re.escape(
        "Job 'bar' subjob 0 update time 2025-03-31 12:01:00.345000+00:00"
        " does not match expected 2025-03-31 12:00:00.345000+00:00"
    )

    # Stale lock (_update_time in DB is dt1, not _SAFE_TIME); provided time dt2 > dt1.
    with pytest.raises(SubJobUpdateConflictError, match=conflict_match):
        await mc.update_subjob_state(
            "bar", 0, error("conflict test"), dt2, last_update_time=_SAFE_TIME
        )

    # Stale lock where the concurrent write's _update_time (dt1) is also after our time
    # (dt0 < dt1). Without the fix this would raise ValueError("last update time is after...")
    # instead of SubJobUpdateConflictError.
    dt0 = _SAFE_TIME - datetime.timedelta(minutes=1)
    with pytest.raises(SubJobUpdateConflictError, match=conflict_match):
        await mc.update_subjob_state(
            "bar", 0, error("conflict test"), dt0, last_update_time=_SAFE_TIME
        )

    # State is still DOWNLOAD_SUBMITTED (both optimistic-lock-gated writes were rejected)
    expected = _BASESUBJOB1.model_copy(deep=True)
    expected.state = models.JobState.DOWNLOAD_SUBMITTED
    expected.transition_times.append(
        models.JobStateTransition(state=models.JobState.DOWNLOAD_SUBMITTED, time=dt1)
    )
    assert await mc.get_subjob("bar", 0) == expected

    # Writing ERROR with the current _update_time (dt1) succeeds
    await mc.update_subjob_state("bar", 0, error("conflict test"), dt2, last_update_time=dt1)
    expected.state = models.JobState.ERROR
    expected.admin_error = "conflict test"
    expected.transition_times.append(
        models.JobStateTransition(state=models.JobState.ERROR, time=dt2)
    )
    assert await mc.get_subjob("bar", 0) == expected


async def test_update_subjob_heartbeat(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.initialize_subjobs([_BASESUBJOB1])

    hb_time = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    await mc.update_subjob_heartbeat("bar", 0, hb_time)

    got = await mc.get_subjob("bar", 0)
    expected = _BASESUBJOB1.model_copy(deep=True)
    expected.heartbeat = hb_time
    assert got == expected

    hb_time2 = hb_time + datetime.timedelta(minutes=5)
    await mc.update_subjob_heartbeat("bar", 0, hb_time2)

    got = await mc.get_subjob("bar", 0)
    expected.heartbeat = hb_time2
    assert got == expected


async def test_update_subjob_heartbeat_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.initialize_subjobs([_BASESUBJOB1])
    hb_time = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)

    await _heartbeat_fail(mc, None, 0, hb_time, ValueError("job_id is required"))
    await _heartbeat_fail(mc, "  \t  ", 0, hb_time, ValueError("job_id is required"))
    await _heartbeat_fail(mc, "bar", None, hb_time, ValueError("subjob_id is required"))
    await _heartbeat_fail(mc, "bar", -1, hb_time, ValueError("subjob_id must be >= 0"))
    await _heartbeat_fail(mc, "bar", 0, None, ValueError("time is required"))


async def _heartbeat_fail(mc, job_id, subjob_id, time, expected):
    with pytest.raises(type(expected), match=f"^{re.escape(expected.args[0])}$"):
        await mc.update_subjob_heartbeat(job_id, subjob_id, time)


async def test_subjob_hidden_fields(mondb):
    # Tests that internal fields are set correctly when performinging actions
    # on a subjob. Does not test other job saving / updating code.
    mc = await MongoDAO.create(mondb)
    await mc.initialize_subjobs([_BASESUBJOB1])

    # check that the internal fields are set correctly
    job = await mondb.subjobs.find_one({"id": "bar", "sub_id": 0})
    check_trans_retry_fields(job)
    assert job["_update_time"] == _SAFE_TIME

    # check that updating subjob state sets internal fields
    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    await mc.update_subjob_state("bar", 0, submitted_download(), dt)
    got = await mc.get_subjob("bar", 0)
    
    # check expected job structure
    expected = _BASESUBJOB1.model_copy(deep=True)
    expected.state = models.JobState.DOWNLOAD_SUBMITTED
    expected.transition_times.append(models.JobStateTransition(
        state=models.JobState.DOWNLOAD_SUBMITTED, time=dt
    ))
    assert got == expected
    
    # check that the internal fields are set correctly
    job = await mondb.subjobs.find_one({"id": "bar", "sub_id": 0})
    check_trans_retry_fields(job)
    assert job["_update_time"] == dt


async def test_recover_subjobs(mondb):
    mc = await MongoDAO.create(mondb)

    dt1 = _SAFE_TIME + datetime.timedelta(minutes=1)
    dt2 = dt1 + datetime.timedelta(minutes=1)

    # subjobs 0 and 2 have error-state fields set to verify clearing;
    # sj0 has extra transitions so trans_history gets a richer set of entries
    sj0 = _BASESUBJOB1.model_copy(deep=True)
    sj0.state = models.JobState.JOB_SUBMITTING
    sj0.transition_times.extend([
        models.JobStateTransition(state=models.JobState.DOWNLOAD_SUBMITTED, time=dt1),
        models.JobStateTransition(state=models.JobState.JOB_SUBMITTING, time=dt2),
    ])
    sj0.exit_code = 5
    sj0.admin_error = "some admin error"
    sj0.traceback = "some traceback"
    sj0.heartbeat = dt2
    sj2 = _BASESUBJOB3.model_copy(deep=True)
    sj2.admin_error = "sj2 error"
    await mc.initialize_subjobs([sj0, _BASESUBJOB2, sj2])

    rec_time = datetime.datetime(2025, 4, 2, 13, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    ds_time = rec_time + datetime.timedelta(seconds=1)

    await mc.recover_subjobs("bar", [0, 2], rec_time, ds_time)

    def _tt(state, time):
        return models.JobStateTransition(state=state, time=time)

    # subjob 0: error fields cleared, trans_history contains all prior transitions,
    #           exit_code and admin_error captured into history arrays, heartbeat cleared
    got0 = await mc.get_subjob("bar", 0)
    assert got0 == models.SubJob(
        id="bar",
        sub_id=0,
        state=models.JobState.DOWNLOAD_SUBMITTED,
        transition_times=[_tt(models.JobState.DOWNLOAD_SUBMITTED, ds_time)],
        trans_history=[
            _tt(models.JobState.CREATED, _SAFE_TIME),
            _tt(models.JobState.DOWNLOAD_SUBMITTED, dt1),
            _tt(models.JobState.JOB_SUBMITTING, dt2),
            _tt(models.JobState.RECOVERING, rec_time),
        ],
        exit_code_history=[5],
        admin_error_history=["some admin error"],
    )
    raw0 = await mondb.subjobs.find_one({"id": "bar", "sub_id": 0})
    assert raw0["_update_time"] == ds_time
    assert "exit_code" not in raw0
    assert "admin_error" not in raw0
    assert "traceback" not in raw0
    assert "heartbeat" not in raw0

    # subjob 2: no exit_code → null recorded in history; admin_error captured; heartbeat cleared
    got2 = await mc.get_subjob("bar", 2)
    assert got2 == models.SubJob(
        id="bar",
        sub_id=2,
        state=models.JobState.DOWNLOAD_SUBMITTED,
        transition_times=[_tt(models.JobState.DOWNLOAD_SUBMITTED, ds_time)],
        trans_history=[
            _tt(models.JobState.CREATED, _SAFE_TIME),
            _tt(models.JobState.RECOVERING, rec_time),
        ],
        exit_code_history=[None],
        admin_error_history=["sj2 error"],
    )
    raw2 = await mondb.subjobs.find_one({"id": "bar", "sub_id": 2})
    assert raw2["_update_time"] == ds_time
    assert "admin_error" not in raw2

    # subjob 1 is untouched
    assert await mc.get_subjob("bar", 1) == _BASESUBJOB2


async def test_recover_subjobs_second_recovery(mondb):
    # When trans_history/exit_code_history/admin_error_history already exist, they should
    # be appended rather than replaced. Exercises the $ifNull branch with existing values.
    mc = await MongoDAO.create(mondb)

    sj0 = _BASESUBJOB1.model_copy(deep=True)
    sj0.exit_code = 5
    sj0.admin_error = "first error"
    await mc.initialize_subjobs([sj0])

    rec_time1 = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    ds_time1 = rec_time1 + datetime.timedelta(seconds=1)

    await mc.recover_subjobs("bar", [0], rec_time1, ds_time1)

    t_err = ds_time1 + datetime.timedelta(minutes=5)
    await mc.update_subjob_state("bar", 0, error("second error"), t_err)

    rec_time2 = t_err + datetime.timedelta(minutes=5)
    ds_time2 = rec_time2 + datetime.timedelta(seconds=1)

    await mc.recover_subjobs("bar", [0], rec_time2, ds_time2)

    def _tt(state, time):
        return models.JobStateTransition(state=state, time=time)

    got = await mc.get_subjob("bar", 0)
    assert got == models.SubJob(
        id="bar",
        sub_id=0,
        state=models.JobState.DOWNLOAD_SUBMITTED,
        transition_times=[_tt(models.JobState.DOWNLOAD_SUBMITTED, ds_time2)],
        trans_history=[
            # from first recovery: original transition_times + RECOVERING1
            _tt(models.JobState.CREATED, _SAFE_TIME),
            _tt(models.JobState.RECOVERING, rec_time1),
            # from second recovery: first recovery's transition_times + RECOVERING2
            _tt(models.JobState.DOWNLOAD_SUBMITTED, ds_time1),
            _tt(models.JobState.ERROR, t_err),
            _tt(models.JobState.RECOVERING, rec_time2),
        ],
        exit_code_history=[5, None],  # no exit code for second failure
        admin_error_history=["first error", "second error"],
    )


async def test_recover_subjobs_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.initialize_subjobs([_BASESUBJOB1, _BASESUBJOB2, _BASESUBJOB3])

    rec_time = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    ds_time = rec_time + datetime.timedelta(seconds=1)

    await fail_recover_subjobs(mc, None, [0], rec_time, ds_time,
        ValueError("job_id is required"))
    await fail_recover_subjobs(mc, "   \t  ", [0], rec_time, ds_time,
        ValueError("job_id is required"))
    await fail_recover_subjobs(mc, "bar", [], rec_time, ds_time,
        ValueError("container_numbers is required and must not be empty"))
    await fail_recover_subjobs(mc, "bar", [-1], rec_time, ds_time,
        ValueError("container_number must be >= 0"))
    await fail_recover_subjobs(mc, "bar", [0], None, ds_time,
        ValueError("recovering_time is required"))
    await fail_recover_subjobs(mc, "bar", [0], rec_time, None,
        ValueError("download_submitted_time is required"))
    await fail_recover_subjobs(mc, "bar", [99], rec_time, ds_time,
        MissingSubJobError("Expected to reset 1 subjobs for job 'bar' but only matched 0"))
    await fail_recover_subjobs(mc, "nobar", [0], rec_time, ds_time,
        MissingSubJobError("Expected to reset 1 subjobs for job 'nobar' but only matched 0"))


async def fail_recover_subjobs(mc, job_id, container_numbers, rec_time, ds_time, expected):
    with pytest.raises(type(expected), match=f"^{re.escape(expected.args[0])}$"):
        await mc.recover_subjobs(job_id, container_numbers, rec_time, ds_time)


async def test_have_subjobs_reached_state(mondb):
    mc = await MongoDAO.create(mondb)
    
    sjs = [_BASESUBJOB1, _BASESUBJOB2, _BASESUBJOB3]
    await mc.initialize_subjobs(sjs)
    
    c = models.JobState.CREATED
    ds = models.JobState.DOWNLOAD_SUBMITTED
    js = models.JobState.JOB_SUBMITTING
    
    # can't tell the difference between no subjobs and no subjobs in state
    assert await mc.have_subjobs_reached_state("nobar", c) == {c: (0, None)}
    assert await mc.have_subjobs_reached_state("bar", c) == {c: (3, _SAFE_TIME)}
    assert await mc.have_subjobs_reached_state("bar", ds) == {ds: (0, None)}
    assert await mc.have_subjobs_reached_state("bar", c, ds) == {c: (3, _SAFE_TIME), ds: (0, None)}
    
    dt1 = _SAFE_TIME + datetime.timedelta(minutes=1)
    await mc.update_subjob_state("bar", 0, submitted_download(), dt1)
    assert await mc.have_subjobs_reached_state("bar", c) == {c: (3, _SAFE_TIME)}
    assert await mc.have_subjobs_reached_state("bar", ds) == {ds: (1, dt1)}
    assert await mc.have_subjobs_reached_state("bar", c, ds) == {c: (3, _SAFE_TIME), ds: (1, dt1)}
    
    dt2 = dt1 + datetime.timedelta(minutes=1)
    await mc.update_subjob_state("bar", 1, submitted_download(), dt2)
    assert await mc.have_subjobs_reached_state("bar", ds) == {ds: (2, dt2)}
    assert await mc.have_subjobs_reached_state("bar", c, ds) == {c: (3, _SAFE_TIME), ds: (2, dt2)}
    
    dt3 = dt1 + datetime.timedelta(seconds=1)
    await mc.update_subjob_state("bar", 2, submitted_download(), dt3)
    assert await mc.have_subjobs_reached_state("bar", ds) == {ds: (3, dt2)}
    assert await mc.have_subjobs_reached_state("bar", c, ds) == {c: (3, _SAFE_TIME), ds: (3, dt2)}
    
    dt4 = dt1 + datetime.timedelta(hours=1)
    await mc.update_subjob_state("bar", 2, submitting_job(), dt4)
    assert await mc.have_subjobs_reached_state("bar", ds) == {ds: (3, dt2)}
    assert await mc.have_subjobs_reached_state("bar", js) == {js: (1, dt4)}
    assert await mc.have_subjobs_reached_state("bar", *list(models.JobState)) == {
        c: (3, _SAFE_TIME),
        ds: (3, dt2),
        js: (1, dt4),
        models.JobState.JOB_SUBMITTED: (0, None),
        models.JobState.UPLOAD_SUBMITTING: (0, None),
        models.JobState.UPLOAD_SUBMITTED: (0, None),
        models.JobState.COMPLETE: (0, None),
        models.JobState.CANCELING: (0, None),
        models.JobState.CANCELED: (0, None),
        models.JobState.RECOVERING: (0, None),
        models.JobState.ERROR_PROCESSING_SUBMITTING: (0, None),
        models.JobState.ERROR_PROCESSING_SUBMITTED: (0, None),
        models.JobState.ERROR: (0, None),
    }


async def test_have_subjobs_reached_state_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.initialize_subjobs([_BASESUBJOB1])
    
    s = models.JobState.CREATED
    await fail_have_subjobs_reached_state(mc, None, ValueError("job_id is required"), s)
    await fail_have_subjobs_reached_state(mc, "   \t  ", ValueError("job_id is required"), s)
    await fail_have_subjobs_reached_state(mc, "bar", ValueError("state is required"), None)
    await fail_have_subjobs_reached_state(mc, "bar", ValueError("state is required"), s, None, s)


async def fail_have_subjobs_reached_state(mc, job_id, expected, *states):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.have_subjobs_reached_state(job_id, *states)


async def test_refdata_redundant_update_time(mondb):
    # Tests that an internal update time field is set correctly when performing actions
    # on refdata. Does not test other refdata saving / updating code.
    mc = await MongoDAO.create(mondb)
    rd = models.AdminReferenceData(
        registered_by="yermum",
        registered_on=_SAFE_TIME,
        id="foo",
        file="bucket/key",
        unpack=False,
        statuses=[models.AdminReferenceDataStatus(
            cluster=sites.Cluster.PERLMUTTER_JAWS,
            state=models.ReferenceDataState.CREATED,
            transition_times=[models.RefDataStateTransition(
                state=models.ReferenceDataState.CREATED,
                time=_SAFE_TIME
            )],
        )]
    )
    await mc.save_refdata(rd)

    # check refdata roundtripping works
    got = await mc.get_refdata_by_id("foo", as_admin=True)
    assert got == rd
    
    # check that the update time is set correctly
    refd = await mondb.refdata.find_one({"id": "foo"})
    assert refd["statuses"][0]["_update_time"] == _SAFE_TIME
    
    # check that updating refdata state sets an internal update time
    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    await mc.update_refdata_state(
        sites.Cluster.PERLMUTTER_JAWS,
        "foo",
        submitted_nersc_refdata_download("ntid"),
        dt,
    )
    got = await mc.get_refdata_by_id("foo", as_admin=True)
    
    # check expected refdata structure
    rd.statuses[0].state = models.ReferenceDataState.DOWNLOAD_SUBMITTED
    rd.statuses[0].transition_times.append(models.RefDataStateTransition(
        state=models.ReferenceDataState.DOWNLOAD_SUBMITTED,
        time=dt,
    ))
    rd.statuses[0].nersc_download_task_id = ["ntid"]
    assert got == rd
    
    # check that the update time is set correctly
    refd = await mondb.refdata.find_one({"id": "foo"})
    assert refd["statuses"][0]["_update_time"] == dt
    
    # check that adding a new site to refdata adds the update time correctly
    rds = models.AdminReferenceDataStatus(
        cluster=sites.Cluster.KBASE,
        state=models.ReferenceDataState.CREATED,
        transition_times=[models.RefDataStateTransition(
            state=models.ReferenceDataState.CREATED,
            time=_SAFE_TIME
        )],
    )
    await mc.add_refdata_site("foo", rds)
    
    # Check roundtrip works
    rd.statuses.append(rds)
    got = await mc.get_refdata_by_id("foo", as_admin=True)
    assert got == rd
    
    # check that the update time is set correctly fir both sites
    refd = await mondb.refdata.find_one({"id": "foo"})
    assert refd["statuses"][0]["_update_time"] == dt
    assert refd["statuses"][1]["_update_time"] == _SAFE_TIME


async def test_job_update_sent(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)
    
    job2 = _BASEJOB.model_copy(deep=True)
    job2.id = "bar"
    await mc.save_job(job2)
    
    await mc.job_update_sent("foo", "trans2")
    await mc.job_update_sent("bar", "trans1")
    
    got1 = await mc.get_job("foo", as_admin=True)
    got2 = await mc.get_job("bar", as_admin=True)
    
    expected1 = _BASEJOB.model_copy(deep=True)
    expected1.transition_times[1].notif_sent = True
    
    job2.transition_times[0].notif_sent = True
    
    assert got1 == expected1
    assert got2 == job2


async def test_job_update_sent_fail_bad_args(mondb):
    mc = await MongoDAO.create(mondb)
    await _fail_job_update_sent(mc, None, "foo", ValueError("job_id is required"))
    await _fail_job_update_sent(mc, "   \t    ", "foo", ValueError("job_id is required"))
    await _fail_job_update_sent(mc, "foo", None, ValueError("trans_id is required"))
    await _fail_job_update_sent(mc, "foo", "    \t    ", ValueError("trans_id is required"))


async def test_job_update_sent_fail_no_such_job(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)
    await _fail_job_update_sent(mc, "fooo", "trans1", NoSuchJobError(
        "No job with ID 'fooo' and state transition ID 'trans1' exists")
    )
    await _fail_job_update_sent(mc, "foo", "trans3", NoSuchJobError(
        "No job with ID 'foo' and state transition ID 'trans3' exists")
    )
    # check the job wasn't updated
    gotjob = await mc.get_job("foo", as_admin=True)
    assert gotjob == _BASEJOB


async def _fail_job_update_sent(mc: MongoDAO, job_id: str, trans_id: str, expected: Exception):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.job_update_sent(job_id, trans_id)


async def test_process_jobs_with_unsent_updates_noop(mondb):
    mc = await MongoDAO.create(mondb)
    job = _BASEJOB.model_copy(deep=True)
    for trans in job.transition_times:
        trans.notif_sent = True
    await mc.save_job(job)
    
    jobs = []
    dt = datetime.datetime(1985, 3, 31, 12, 0, 0, tzinfo=datetime.timezone.utc)
    async def collector(j: models.AdminJobDetails):
        jobs.append(j)
    count = await mc.process_jobs_with_unsent_updates(collector, dt)
    assert jobs == []
    assert count == 0


async def set_up_jobs(mc: MongoDAO) -> tuple[models.AdminJobDetails]:
    old_dt = datetime.datetime(2000, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc)
    newer_dt = datetime.datetime(2020, 1, 1, 1, 1, 2, tzinfo=datetime.timezone.utc)
    
    # job should never show up, dates are very new
    await mc.save_job(_BASEJOB)
    
    # job should never show up even with old dates since messages are sent
    job1 = _BASEJOB.model_copy(deep=True)
    job1.id = "job1"
    for trans in job1.transition_times:
        trans.notif_sent = True
        trans.time = old_dt
    await mc.save_job(job1)
    
    # jobs with a very new unsent transition and old sent transition sholdn't show up
    job2 = _BASEJOB.model_copy(deep=True)
    job2.id = "job2"
    job2.transition_times[0].notif_sent = True
    job2.transition_times[0].time = old_dt
    await mc.save_job(job2)
    
    # should hit 2nd position and miss the first position because the notification is sent
    job3 = _BASEJOB.model_copy(deep=True)
    job3.id = "job3"
    job3.transition_times[0].notif_sent = True
    job3.transition_times[0].time = old_dt
    job3.transition_times[1].time = newer_dt
    await mc.save_job(job3)
    
    # should hit 1st position and miss the 2nd position because the time is new
    job4 = _BASEJOB.model_copy(deep=True)
    job4.id = "job4"
    job4.transition_times[0].time = newer_dt
    await mc.save_job(job4)
    
    # should hit both positions
    job5 = _BASEJOB.model_copy(deep=True)
    job5.id = "job5"
    job5.transition_times[0].time = newer_dt
    job5.transition_times[1].time = newer_dt
    await mc.save_job(job5)
    
    return job3, job4, job5


async def test_process_jobs_with_unsent_updates(mondb):
    mc = await MongoDAO.create(mondb)
    jobs = {}
    async def collector(j: models.AdminJobDetails):
        jobs[j.id] = j
        
    job3, job4, job5 = await set_up_jobs(mc)
    
    # check no jobs are found
    dt = datetime.datetime(2020, 1, 1, 1, 1, 2, tzinfo=datetime.timezone.utc)
    count = await mc.process_jobs_with_unsent_updates(collector, dt)
    assert jobs == {}
    assert count == 0
    
    # check expected jobs are found
    dt = datetime.datetime(2020, 1, 1, 1, 1, 3, tzinfo=datetime.timezone.utc)
    count = await mc.process_jobs_with_unsent_updates(collector, dt)
    assert jobs.keys() == {"job3", "job4", "job5"}
    assert count == 3
    assert jobs["job3"] == job3
    assert jobs["job4"] == job4
    assert jobs["job5"] == job5


async def test_process_jobs_with_unsent_updates_using_index(mondb):
    # This tests that the mongo query for the respective function uses the correct index.
    mc = await MongoDAO.create(mondb)
    await set_up_jobs(mc)
    
    dt = datetime.datetime(2020, 1, 1, 1, 1, 3, tzinfo=datetime.timezone.utc)
    # This query is copied from the function and needs to be kept in sync
    sf = f"{models.FLD_COMMON_TRANS_TIMES}.{models.FLD_JOB_STATE_TRANSITION_NOTIFICATION_SENT}"
    query = {
        sf: False,
        models.FLD_COMMON_TRANS_TIMES:{
            "$elemMatch": {
                models.FLD_JOB_STATE_TRANSITION_NOTIFICATION_SENT: False,
                models.FLD_COMMON_STATE_TRANSITION_TIME: {"$lt": dt}
            }
        }
    }
    plan = await mondb.jobs.find(query).explain()
    index = plan["queryPlanner"]["winningPlan"]["inputStage"]
    assert index == {
        "stage": "IXSCAN",
        "keyPattern": {
            "transition_times.time": -1
        },
        "indexName": "transition_times.time_-1",
        "isMultiKey": True,
        "multiKeyPaths": {
            "transition_times.time": [
                "transition_times"
            ]
        },
        "isUnique": False,
        "isSparse": False,
        "isPartial": True,
        "indexVersion": 2,
        "direction": "forward",
        "indexBounds": {
            "transition_times.time": [
                "(new Date(1577840463000), new Date(-9223372036854775808)]"
            ]
        }
    }


async def test_process_jobs_with_unsent_updates_fail_bad_args(mondb):
    mc = await MongoDAO.create(mondb)
    async def foo(j: models.AdminJobDetails):
        pass

    await _fail_process_jobs_with_unsent_updates(mc, None, _SAFE_TIME, ValueError(
        "processor is required")
    )
    await _fail_process_jobs_with_unsent_updates(mc, foo, None, ValueError(
        "older_than is required")
    )
    await _fail_process_jobs_with_unsent_updates(
        mc,
        foo,
        datetime.datetime.now(),
        ValueError("older_than must be a timezone aware datetime")
    )


async def _fail_process_jobs_with_unsent_updates(
    mc: MongoDAO,
    processor: Callable[[models.AdminJobDetails], Coroutine[None, None, None]],
    older_than: datetime.datetime,
    expected: Exception,
):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.process_jobs_with_unsent_updates(processor, older_than)


async def test_set_refdata_clean(mondb):
    mc = await MongoDAO.create(mondb)
    rd = models.AdminReferenceData(
        registered_by="yermum",
        registered_on=_SAFE_TIME,
        id="foo",
        file="bucket/key",
        unpack=False,
        statuses=[
            models.AdminReferenceDataStatus(
                cluster=sites.Cluster.PERLMUTTER_JAWS,
                state=models.ReferenceDataState.CREATED,
                transition_times=[
                    models.RefDataStateTransition(
                        state=models.ReferenceDataState.CREATED,
                        time=_SAFE_TIME
                    ),
                ],
            ),
            models.AdminReferenceDataStatus(
                cluster=sites.Cluster.KBASE,
                state=models.ReferenceDataState.ERROR,
                transition_times=[
                    models.RefDataStateTransition(
                        state=models.ReferenceDataState.CREATED,
                        time=_SAFE_TIME
                    ),
                    models.RefDataStateTransition(
                        state=models.ReferenceDataState.ERROR,
                        time=_SAFE_TIME
                    ),
                ],
            ),
        ],
    )
    await mc.save_refdata(rd)
    
    got = await mc.get_refdata_by_id("foo", as_admin=True)
    assert [r.cleaned for r in got.statuses] == [False, False]
    
    await mc.set_refdata_clean(sites.Cluster.KBASE, "foo")
    rd.statuses[1].cleaned = True
    got = await mc.get_refdata_by_id("foo", as_admin=True)
    assert got == rd
    
    await mc.set_refdata_clean(sites.Cluster.PERLMUTTER_JAWS, "foo")
    rd.statuses[0].cleaned = True
    got = await mc.get_refdata_by_id("foo", as_admin=True)
    assert got == rd


async def test_set_refdata_clean_fail(mondb):
    mc = await MongoDAO.create(mondb)
    rd = models.AdminReferenceData(
        registered_by="yermum",
        registered_on=_SAFE_TIME,
        id="foo",
        file="bucket/key",
        unpack=False,
        statuses=[
            models.AdminReferenceDataStatus(
                cluster=sites.Cluster.PERLMUTTER_JAWS,
                state=models.ReferenceDataState.CREATED,
                transition_times=[
                    models.RefDataStateTransition(
                        state=models.ReferenceDataState.CREATED,
                        time=_SAFE_TIME
                    ),
                ],
            ),
        ],
    )
    await mc.save_refdata(rd)
    
    k = sites.Cluster.KBASE
    p = sites.Cluster.PERLMUTTER_JAWS
    await _fail_set_refdata_clean(mc, None, p, ValueError("refdata_id is required"))
    await _fail_set_refdata_clean(mc, "  \t   ", p, ValueError("refdata_id is required"))
    await _fail_set_refdata_clean(mc, "foo", None, ValueError("cluster is required"))
    await _fail_set_refdata_clean(mc, "bar", p, NoSuchReferenceDataError(
        "No reference data with ID 'bar' for cluster perlmutter-jaws exists"
    ))
    await _fail_set_refdata_clean(mc, "foo", k, NoSuchReferenceDataError(
        "No reference data with ID 'foo' for cluster kbase exists"
    ))


async def _fail_set_refdata_clean(
    mc: MongoDAO, refdata_id: str, cluster: sites.Cluster, expected: Exception,
):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.set_refdata_clean(cluster, refdata_id)


async def test_process_dirty_refdata(mondb):
    mc = await MongoDAO.create(mondb)
    current = datetime.datetime(
        year=2026, month=2, day=10, hour=14, minute=30, second=54, tzinfo=datetime.UTC
    )
    older_than = current - datetime.timedelta(days=30)  # much newer than _SAFE_TIME
    orig = models.AdminReferenceData(
        registered_by="yermum",
        registered_on=_SAFE_TIME,
        id="foo",
        file="bucket/key",
        unpack=False,
        statuses=[
            models.AdminReferenceDataStatus(
                cluster=sites.Cluster.PERLMUTTER_JAWS,
                state=models.ReferenceDataState.ERROR,
                transition_times=[
                    models.RefDataStateTransition(
                        state=models.ReferenceDataState.CREATED,
                        time=older_than
                    ),
                ],
            ),
            models.AdminReferenceDataStatus(
                cluster=sites.Cluster.KBASE,
                state=models.ReferenceDataState.COMPLETE,
                transition_times=[
                    models.RefDataStateTransition(
                        state=models.ReferenceDataState.DOWNLOAD_SUBMITTED,
                        time=older_than
                    ),
                ],
            ),
        ],
    )
    
    rd = orig.model_copy(deep=True, update={"id": "toonew"})
    await mc.save_refdata(rd)  # shouldn't be found due to date = older_than
    
    # Shouldn't be found due to non-terminal states
    for state in set(models.ReferenceDataState) - models.ReferenceDataState.terminal_states():
        rd = rd.model_copy(deep=True, update={"id": state.value, "file": "bucket/" + state.value})
        rd.statuses[0].state=state
        rd.statuses[0].transition_times[0].time = _SAFE_TIME
        await mc.save_refdata(rd)
    
    rd = rd.model_copy(deep=True, update={"id": "cleaned", "file": "bucket/cleaned"})
    rd.statuses[1].transition_times[0].time = _SAFE_TIME
    rd.statuses[1].cleaned = True
    await mc.save_refdata(rd)  # shouldn't be found due to cleaned state
    
    # save refdata expected to be found, 1 per terminal state
    comp = orig.model_copy(deep=True, update={"id": "found_comp", "file": "bucket/comp"})
    comp.statuses[1].transition_times[0].time = older_than - datetime.timedelta(seconds=1)
    await mc.save_refdata(comp)
    
    err= orig.model_copy(deep=True, update={"id": "found_err", "file": "bucket/err"})
    err.statuses[0].transition_times[0].time = older_than - datetime.timedelta(seconds=1)
    await mc.save_refdata(err)
    
    found = {}
    async def collect(refdata):
        found[refdata.id] = refdata
    await mc.process_dirty_refdata(older_than, collect)
    
    # debugging help
    assert found.keys() == {"found_err", "found_comp"}
    assert found == {"found_comp": comp, "found_err": err}
    
    # test noop
    found.clear()
    await mc.process_dirty_refdata(_SAFE_TIME, collect)
    assert found.keys() == set() 


async def test_process_dirty_refdata_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await _process_dirty_refdata_fail(
        mc, None, lambda: print("foo"), ValueError("older_than is required")
    )
    await _process_dirty_refdata_fail(mc, _SAFE_TIME, None, ValueError("operation is required"))


async def _process_dirty_refdata_fail(mc, older_than, op, expected):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.process_dirty_refdata(older_than, op)


def _make_sj(job_id, sub_id, state=models.JobState.CREATED, *, time=_SAFE_TIME, heartbeat=None):
    return models.SubJob(
        id=job_id,
        sub_id=sub_id,
        state=state,
        heartbeat=heartbeat,
        transition_times=[models.JobStateTransition(state=state, time=time)],
    )


_HB_THRESHOLD  = datetime.datetime(2025, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
_HB_OLD        = _HB_THRESHOLD - datetime.timedelta(seconds=1)
_HB_NEW        = _HB_THRESHOLD + datetime.timedelta(seconds=1)
_HB_T0         = datetime.datetime(2025, 6, 1, 1, tzinfo=datetime.timezone.utc)  # j1/0 absent hb
_HB_T1         = datetime.datetime(2025, 6, 1, 2, tzinfo=datetime.timezone.utc)  # j1/1 null hb
_HB_T2         = datetime.datetime(2025, 6, 1, 3, tzinfo=datetime.timezone.utc)  # j2/0 null hb
_HB_T3         = datetime.datetime(2025, 6, 1, 4, tzinfo=datetime.timezone.utc)  # j3/0 stale hb
_HB_T4         = datetime.datetime(2025, 6, 1, 5, tzinfo=datetime.timezone.utc)  # j3/1 stale hb
_HB_T5         = datetime.datetime(2025, 6, 1, 6, tzinfo=datetime.timezone.utc)  # j4/0 stale hb


async def _setup_heartbeat_subjobs(mc, mondb):
    await mc.initialize_subjobs([
        # heartbeat absent after $unset → missing:in, stale:out
        _make_sj("j1", 0, time=_HB_T0),
        # heartbeat null → missing:in, stale:out
        _make_sj("j1", 1, time=_HB_T1),
        # heartbeat null, different active state → missing:in, stale:out
        _make_sj("j2", 0, models.JobState.DOWNLOAD_SUBMITTED, time=_HB_T2),
        # recent hb on same job → missing:out, stale:out
        _make_sj("j2", 1, models.JobState.JOB_SUBMITTED, heartbeat=_HB_NEW),
        # stale hb → missing:out, stale:in
        _make_sj("j3", 0, models.JobState.JOB_SUBMITTED, heartbeat=_HB_OLD, time=_HB_T3),
        # stale hb, different active state and time → missing:out, stale:in
        _make_sj("j3", 1, models.JobState.DOWNLOAD_SUBMITTED, heartbeat=_HB_OLD, time=_HB_T4),
        # recent hb on same job → missing:out, stale:out
        _make_sj("j3", 2, heartbeat=_HB_NEW),
        # stale hb, second job → missing:out, stale:in
        _make_sj("j4", 0, heartbeat=_HB_OLD, time=_HB_T5),
        # terminal → neither (one with stale hb, one with no hb)
        _make_sj("j5", 0, models.JobState.ERROR, heartbeat=_HB_OLD),
        _make_sj("j5", 1, models.JobState.ERROR),
        _make_sj("j5", 2, models.JobState.COMPLETE, heartbeat=_HB_OLD),
        _make_sj("j5", 3, models.JobState.COMPLETE),
        # canceling → neither (one with stale hb, one with no hb)
        _make_sj("j6", 0, models.JobState.CANCELING, heartbeat=_HB_OLD),
        _make_sj("j6", 1, models.JobState.CANCELING),
        _make_sj("j6", 2, models.JobState.CANCELED, heartbeat=_HB_OLD),
        _make_sj("j6", 3, models.JobState.CANCELED),
        # recovering → neither (one with stale hb, one with no hb)
        _make_sj("j7", 0, models.JobState.RECOVERING, heartbeat=_HB_OLD),
        _make_sj("j7", 1, models.JobState.RECOVERING),
    ])
    await mondb.subjobs.update_one({"id": "j1", "sub_id": 0}, {"$unset": {"heartbeat": ""}})


async def test_get_subjobs_with_missing_heartbeat(mondb):
    mc = await MongoDAO.create(mondb)
    await _setup_heartbeat_subjobs(mc, mondb)

    assert await mc.get_subjobs_with_missing_heartbeat() == {
        "j1": {0: _HB_T0, 1: _HB_T1},
        "j2": {0: _HB_T2},
    }


async def test_get_subjobs_with_stale_heartbeat(mondb):
    mc = await MongoDAO.create(mondb)
    await _setup_heartbeat_subjobs(mc, mondb)

    assert await mc.get_subjobs_with_stale_heartbeat(_HB_THRESHOLD) == {
        "j3": {0: _HB_T3, 1: _HB_T4},
        "j4": {0: _HB_T5},
    }


async def test_get_subjobs_with_stale_heartbeat_fail(mondb):
    mc = await MongoDAO.create(mondb)
    with pytest.raises(ValueError, match="^older_than is required$"):
        await mc.get_subjobs_with_stale_heartbeat(None)
    with pytest.raises(ValueError, match="^older_than must be a timezone aware datetime$"):
        await mc.get_subjobs_with_stale_heartbeat(datetime.datetime(2025, 1, 1))
