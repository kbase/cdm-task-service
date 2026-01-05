
from bson.son import SON
import datetime
import re
from pymongo.errors import BulkWriteError, DuplicateKeyError
import pytest
from typing import Coroutine, Callable, Any

from cdmtaskservice import models
from cdmtaskservice import sites
from cdmtaskservice.mongo import MongoDAO, NoSuchJobError, NoSuchSubJobError
from cdmtaskservice.update_state import (
    error,
    submitted_download,
    submitted_jaws_job,
    submitted_nersc_refdata_download,
    submitting_job,
    submitting_upload,
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


@pytest.mark.asyncio
async def test_indexes(mongo, mondb):
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
        }
    }
    subjobindex = mongo.client[MONGO_TEST_DB]["subjobs"].index_information()
    assert subjobindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        'id_1_sub_id_1': {'key': [('id', 1), ('sub_id', 1)], 'unique': True, 'v': 2},
        'id_1_transition_times.state_1_transition_times._retry_1': {
            'key': [('id', 1), ('transition_times.state', 1), ('transition_times._retry', 1)],
            'v': 2
        }
    }
    ecindex = mongo.client[MONGO_TEST_DB]["exitcodes"].index_information()
    assert ecindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        'id_1': {'key': [('id', 1)], 'unique': True, 'v': 2},
    }
    refindex = mongo.client[MONGO_TEST_DB]["refdata"].index_information()
    assert refindex == {
        "_id_": {"v": 2, "key": [("_id", 1)]},
        "id_1": {"v": 2, "key": [("id", 1)], "unique": True},
        "file_1": {"v": 2, "key": [("file", 1)]}
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


@pytest.mark.asyncio
async def test_job_basic_roundtrip(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)

    got = await mc.get_job("foo", as_admin=True)
    assert got == _BASEJOB


@pytest.mark.asyncio
async def test_exit_codes_for_standard_job_roundtrip(mondb):
    mc = await MongoDAO.create(mondb)
    
    await mc.save_exit_codes_for_standard_job("foo", [1, 2, 3, 0])
    await mc.save_exit_codes_for_standard_job("bar", [0])
    
    assert await mc.get_exit_codes_for_standard_job("foo") == [1, 2, 3, 0]
    assert await mc.get_exit_codes_for_standard_job("bar") == [0]
    assert await mc.get_exit_codes_for_standard_job("baz") is None


@pytest.mark.asyncio
async def test_exit_codes_for_standard_job_upsert(mondb):
    mc = await MongoDAO.create(mondb)
    
    await mc.save_exit_codes_for_standard_job("foo", [1, 2, 3, 0])
    assert await mc.get_exit_codes_for_standard_job("foo") == [1, 2, 3, 0]
    
    await mc.save_exit_codes_for_standard_job("foo", [0])
    assert await mc.get_exit_codes_for_standard_job("foo") == [0]


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_update_job(mondb):
    # tests updates that change standard and array fields as well as switching to error
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)

    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    dt2 = dt + datetime.timedelta(minutes=1)
    await mc.update_job_state("foo", submitting_job(), dt, "tid1")  # no fields
    await mc.update_job_state("foo", submitted_jaws_job("123"), dt, "tid2")  # array field
    await mc.update_job_state("foo", submitting_upload(1.2), dt2, "tid3")  # std field
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


@pytest.mark.asyncio
async def test_update_job_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)
    
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
        "No job with ID 'bar' in state download_submitted exists"
    ))
    await fail_update_job(mc, "foo", submitting_upload(), dt, tid, NoSuchJobError(
        "No job with ID 'foo' in state job_submitted exists"
    ))
    await fail_update_job(mc, "foo", u, _SAFE_TIME + datetime.timedelta(milliseconds=-1), tid,
        NoSuchJobError("No job with ID 'foo' in state download_submitted exists")
    )


@pytest.mark.asyncio
async def test_update_job_and_subjob_fail_update_to_error(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.save_job(_BASEJOB)
    await mc.initialize_subjobs([_BASESUBJOB1])
    u = error("admin error here")
    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    tid = "1"
    
    for state in models.JobState.terminal_states():
        mondb.jobs.update_one({}, {"$set": {"state": state.value}})
        mondb.subjobs.update_one({}, {"$set": {"state": state.value}})
        await fail_update_job(mc, "foo", u, dt, tid, NoSuchJobError(
            "No job with ID 'foo' not in states ['complete', 'error'] exists"
        ))
        await fail_update_subjob(mc, "bar", 0, u, dt, NoSuchSubJobError(
            "No job with ID 'bar' and subjob ID 0 not in states ['complete', 'error'] exists")
        )


async def fail_update_job(mc, job_id, update, dt, tid, expected):
    with pytest.raises(type(expected), match=f"^{re.escape(expected.args[0])}$"):
        await mc.update_job_state(job_id, update, dt, tid)


def check_job_retry_fields(jobdoc: dict[str, Any]):
    assert jobdoc["_retry"] == 0
    check_trans_retry_fields(jobdoc)

def check_trans_retry_fields(jobdoc: dict[str, Any]):
    for tt in jobdoc["transition_times"]:
        assert tt["_retry"] == 0


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_subjob_basic_roundtrip(mondb):
    mc = await MongoDAO.create(mondb)
    
    sjs = [_BASESUBJOB1, _BASESUBJOB2, _BASESUBJOB3]
    await mc.initialize_subjobs(sjs)
    
    for i in range(3):
        sj = await mc.get_subjob("bar", i)
        assert sj == sjs[i]
    
    sjs_got = await mc.get_subjobs("bar")
    assert sjs_got == sjs 


@pytest.mark.asyncio
async def test_initialize_subjobs_fail_bad_args(mondb):
    mc = await MongoDAO.create(mondb)
    
    await initialize_subjobs_fail(mc, None, ValueError("subjobs is required"))
    await initialize_subjobs_fail(
        mc, [_BASESUBJOB1, None, _BASESUBJOB2], ValueError("subjob is required")
    )


async def initialize_subjobs_fail(mc, subjobs, expected):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.initialize_subjobs(subjobs)


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_get_subjobs_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.initialize_subjobs([_BASESUBJOB2])
    
    await get_subjobs_fail(mc, None, ValueError("job_id is required"))
    await get_subjobs_fail(mc, "   \t   ", ValueError("job_id is required"))
    await get_subjobs_fail(mc, "foo", NoSuchSubJobError("No sub jobs found for job ID 'foo'"))


async def get_subjobs_fail(mc, job_id, expected):
    with pytest.raises(type(expected), match=f"^{expected.args[0]}$"):
        await mc.get_subjobs(job_id)


@pytest.mark.asyncio
async def test_get_exit_codes_for_subjobs(mondb):
    mc = await MongoDAO.create(mondb)
    sj1 = _BASESUBJOB1.model_copy()
    sj1.exit_code = 3
    sj2 = _BASESUBJOB2.model_copy()
    sj2.exit_code = 0
    await mc.initialize_subjobs([_BASESUBJOB3, sj1, sj2])
    
    assert await mc.get_exit_codes_for_subjobs("bar") == [3, 0, None]


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_update_subjob_fail(mondb):
    mc = await MongoDAO.create(mondb)
    await mc.initialize_subjobs([_BASESUBJOB1])
    
    u = submitted_download()
    dt = datetime.datetime(2025, 4, 2, 12, 0, 0, 345000, tzinfo=datetime.timezone.utc)
    await fail_update_subjob(mc, None, 0, u, dt, ValueError("job_id is required"))
    await fail_update_subjob(mc, "   \t  ", 0, u, dt, ValueError("job_id is required"))
    await fail_update_subjob(mc, "bar", None, u, dt, ValueError("subjob_id is required"))
    await fail_update_subjob(mc, "bar", -1, u, dt, ValueError("subjob_id must be >= 0"))
    await fail_update_subjob(mc, "bar", 0, None, dt, ValueError("update is required"))
    await fail_update_subjob(mc, "bar", 0, u, None, ValueError("time is required"))
    await fail_update_subjob(mc, "foo", 0, u, dt, NoSuchSubJobError(
        "No job with ID 'foo' and subjob ID 0 in state created exists"
    ))
    await fail_update_subjob(mc, "bar", 1, u, dt, NoSuchSubJobError(
        "No job with ID 'bar' and subjob ID 1 in state created exists"
    ))
    await fail_update_subjob(mc, "bar", 0, submitting_upload(), dt, NoSuchSubJobError(
        "No job with ID 'bar' and subjob ID 0 in state job_submitted exists"
    ))
    await fail_update_subjob(mc, "bar", 0, u, _SAFE_TIME + datetime.timedelta(milliseconds=-1),
         NoSuchSubJobError("No job with ID 'bar' and subjob ID 0 in state created exists")
    )


async def fail_update_subjob(mc, job_id, subjob_id, update, dt, expected):
    with pytest.raises(type(expected), match=f"^{re.escape(expected.args[0])}$"):
        await mc.update_subjob_state(job_id, subjob_id, update, dt)


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
        models.JobState.ERROR_PROCESSING_SUBMITTING: (0, None),
        models.JobState.ERROR_PROCESSING_SUBMITTED: (0, None),
        models.JobState.ERROR: (0, None),
    }


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_refdata_redundant_update_time(mondb):
    # Tests that an internal update time field is set correctly when performing actions
    # on refdata. Does not test other refdata saving / updating code.
    mc = await MongoDAO.create(mondb)
    rd = models.ReferenceData(
        registered_by="yermum",
        registered_on=_SAFE_TIME,
        id="foo",
        file="bucket/key",
        unpack=False,
        statuses=[models.ReferenceDataStatus(
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
    got = await mc.get_refdata_by_id("foo")
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
    got = await mc.get_refdata_by_id("foo")
    
    # check expected refdata structure
    rd.statuses[0].state = models.ReferenceDataState.DOWNLOAD_SUBMITTED
    rd.statuses[0].transition_times.append(models.RefDataStateTransition(
        state=models.ReferenceDataState.DOWNLOAD_SUBMITTED,
        time=dt,
    ))
    assert got == rd
    
    # check that the update time is set correctly
    refd = await mondb.refdata.find_one({"id": "foo"})
    assert refd["statuses"][0]["_update_time"] == dt


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_job_update_sent_fail_bad_args(mondb):
    mc = await MongoDAO.create(mondb)
    await _fail_job_update_sent(mc, None, "foo", ValueError("job_id is required"))
    await _fail_job_update_sent(mc, "   \t    ", "foo", ValueError("job_id is required"))
    await _fail_job_update_sent(mc, "foo", None, ValueError("trans_id is required"))
    await _fail_job_update_sent(mc, "foo", "    \t    ", ValueError("trans_id is required"))


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
